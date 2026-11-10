//! Running a kernel op: its shader, loaded on first use, and one pass over a caller's buffers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use super::{Kernel, KernelError};
use crate::WgpuBackend;
use crate::buffer::Storage;
use crate::context::Context;
use crate::fence::{Ticket, TicketState};
use crate::record::RecordCx;
use crate::shader::{BindingKind, BindingSpec, Bound, Dispatch, Shader, ShaderSpec};

const SOURCE: &str = concat!(include_str!("common.wgsl"), include_str!("kernels.wgsl"));
const WORKGROUP: u32 = 64;

const fn storage(index: u32, name: &'static str, read_only: bool) -> BindingSpec {
    BindingSpec {
        index,
        name,
        kind: BindingKind::Storage {
            read_only,
            stride: 4,
        },
    }
}

const PARAMS: BindingSpec = BindingSpec {
    index: 2,
    name: "params",
    kind: BindingKind::Uniform { size: 16 },
};

/// What most ops bind: the input, the output and their counts.
const PLAIN: &[BindingSpec] = &[storage(0, "src", true), storage(1, "dst", false), PARAMS];
const DECODE: &[BindingSpec] = &[
    storage(0, "src", true),
    storage(1, "dst", false),
    PARAMS,
    storage(3, "status", false),
];
const CRC: &[BindingSpec] = &[
    storage(0, "src", true),
    storage(1, "dst", false),
    PARAMS,
    storage(4, "table", true),
];

const fn spec(
    name: &'static str,
    entry: &'static str,
    bindings: &'static [BindingSpec],
) -> ShaderSpec {
    ShaderSpec {
        name,
        entry,
        source: SOURCE,
        workgroup: [WORKGROUP, 1, 1],
        bindings,
        // Every run gives its own counts.
        dispatch: Dispatch::Counts([1, 1, 1]),
        inputs: &[],
        outputs: &[],
    }
}

/// In the order of [`Kernel`].
const SPECS: [ShaderSpec; 8] = [
    spec("kernels.f32_from_le", "f32_from_le", PLAIN),
    spec("kernels.f16_to_f32", "f16_to_f32", PLAIN),
    spec("kernels.f32_to_unorm8", "f32_to_unorm8", PLAIN),
    spec("kernels.copy", "copy_words", PLAIN),
    spec("kernels.hex_lower", "hex_lower", PLAIN),
    spec("kernels.base64_encode", "base64_encode", PLAIN),
    spec("kernels.base64_decode", "base64_decode", DECODE),
    spec("kernels.crc32_chunks", "crc32_chunks", CRC),
];

/// Distinct counts whose parameter buffers are kept.
const PARAMS_KEPT: usize = 64;

/// The kernel ops on one device. A shader is built the first time its op is used, or found if
/// this device has built it before. After a lost device is replaced, make another: this one
/// keeps answering with the device's error.
pub struct Kernels {
    pub(super) context: Arc<Context>,
    shaders: [OnceLock<Shader>; 8],
    /// The counts of a run, on the device, by what they are. They are copied into the shader's
    /// uniform by the same encoder that records the pass, so two runs of an op in one encoder
    /// each see their own.
    params: Mutex<HashMap<[u32; 3], Arc<Storage>>>,
    pub(super) table: OnceLock<Storage>,
}

impl Kernels {
    pub fn new(backend: &WgpuBackend) -> Result<Kernels, KernelError> {
        Ok(Kernels::on(backend.context().map_err(KernelError::Open)?))
    }

    /// The kernels on the device a recording body is recording for. A body keeps them, and makes
    /// them again when [`Kernels::device_key`] is no longer its buffers'.
    pub fn in_record(cx: &RecordCx<'_>) -> Kernels {
        Kernels::on(Arc::clone(cx.context))
    }

    fn on(context: Arc<Context>) -> Kernels {
        Kernels {
            context,
            shaders: [const { OnceLock::new() }; 8],
            params: Mutex::new(HashMap::new()),
            table: OnceLock::new(),
        }
    }

    /// Which device this is on. A buffer with the same key is on it.
    pub fn device_key(&self) -> u64 {
        self.context.key
    }

    fn shader(&self, kernel: Kernel) -> Result<&Shader, KernelError> {
        let slot = &self.shaders[kernel.index()];
        if let Some(shader) = slot.get() {
            return Ok(shader);
        }
        let loaded = Shader::load_on(Arc::clone(&self.context), &SPECS[kernel.index()])
            .map_err(KernelError::Shader)?;
        Ok(slot.get_or_init(|| loaded))
    }

    fn params(&self, values: [u32; 3]) -> Result<Arc<Storage>, KernelError> {
        let mut kept = self.params.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(found) = kept.get(&values) {
            return Ok(Arc::clone(found));
        }
        let buffer =
            Storage::zeroed(Arc::clone(&self.context), 16).map_err(KernelError::Storage)?;
        let mut bytes = [0; 16];
        for (word, value) in bytes.as_chunks_mut::<4>().0.iter_mut().zip(values) {
            *word = value.to_le_bytes();
        }
        buffer.write(0, &bytes).map_err(KernelError::Storage)?;
        if kept.len() >= PARAMS_KEPT {
            kept.clear();
        }
        let buffer = Arc::new(buffer);
        kept.insert(values, Arc::clone(&buffer));
        Ok(buffer)
    }

    /// Records `kernel` over `n` elements of `src` into `dst`, as one compute pass in `encoder`.
    /// Nothing is submitted. Base64 decode is [`Kernels::base64_decode`], which also reads what
    /// the op reports.
    pub fn encode(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        kernel: Kernel,
        src: &Storage,
        dst: &Storage,
        n: u64,
    ) -> Result<(), KernelError> {
        if kernel == Kernel::Base64Decode {
            return Err(KernelError::Size(
                "base64 decode reports a length: run it with `base64_decode`".into(),
            ));
        }
        if kernel == Kernel::Crc32Chunks {
            return self.encode_crc32_chunks(encoder, src, dst, n);
        }
        self.encode_with(encoder, kernel, src, dst, n, None)
    }

    pub(super) fn encode_with(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        kernel: Kernel,
        src: &Storage,
        dst: &Storage,
        n: u64,
        extra: Option<&Storage>,
    ) -> Result<(), KernelError> {
        if n == 0 {
            return Ok(());
        }
        let (need_in, need_out) = (
            kernel.input_len(n).next_multiple_of(4),
            kernel.output_len(n).next_multiple_of(4),
        );
        if src.size() < need_in || dst.size() < need_out {
            return Err(KernelError::Size(format!(
                "{kernel:?} over {n} needs {need_in} input and {need_out} output bytes, and was given {} and {}",
                src.size(),
                dst.size()
            )));
        }
        let threads = kernel.threads(n);
        let groups = threads.div_ceil(u64::from(WORKGROUP));
        let max = u64::from(self.context.record.compute.max_workgroups_per_dimension);
        let (x, y) = match groups <= max {
            true => (groups, 1),
            false => (max, groups.div_ceil(max)),
        };
        // Offsets inside the shader are 32-bit, in bytes and in output bytes.
        let fits = |len: u64| u32::try_from(len).is_ok();
        if y > max || !fits(need_in) || !fits(need_out) {
            return Err(KernelError::Size(format!(
                "{kernel:?} over {n} elements doesn't fit one pass on this device"
            )));
        }
        let params = self.params([n as u32, threads as u32, kernel.output_len(n) as u32])?;
        let shader = self.shader(kernel)?;
        let plain = [
            Bound::Storage(src),
            Bound::Storage(dst),
            Bound::UniformFrom(&params),
        ];
        let with = extra.map(|extra| [plain[0], plain[1], plain[2], Bound::Storage(extra)]);
        let bound: &[Bound<'_>] = match &with {
            Some(with) => with,
            None => &plain,
        };
        shader
            .encode_sized(encoder, bound, [x as u32, y as u32, 1])
            .map_err(KernelError::Shader)
    }

    /// An encoder on this device, to record ops into.
    pub fn encoder(&self) -> wgpu::CommandEncoder {
        self.context
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor::default())
    }

    /// Submits `encoder` and gives the ticket for it. Never waits. For ops run by hand: in a
    /// graph, the region submits.
    pub fn submit(&self, encoder: wgpu::CommandEncoder) -> Ticket {
        let context = &self.context;
        let state = TicketState::new();
        let finished = Arc::clone(&state);
        {
            let _held = context.submit.lock();
            context.queue.submit([encoder.finish()]);
            context.count_submit();
            context
                .queue
                .on_submitted_work_done(move || finished.finish());
        }
        Ticket::new(state, Arc::clone(context), None)
    }

    /// Records `kernel` and submits it alone.
    pub fn run(
        &self,
        kernel: Kernel,
        src: &Storage,
        dst: &Storage,
        n: u64,
    ) -> Result<Ticket, KernelError> {
        let mut encoder = self.encoder();
        self.encode(&mut encoder, kernel, src, dst, n)?;
        Ok(self.submit(encoder))
    }

    /// Records a copy of `n` bytes from the start of `src` to the start of `dst` with the
    /// device's copy command: what [`Kernel::Copy`] does, without a shader. `n` is a multiple
    /// of 4.
    pub fn copy_with_engine(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        src: &Storage,
        dst: &Storage,
        n: u64,
    ) -> Result<(), KernelError> {
        if !n.is_multiple_of(4) || src.size() < n || dst.size() < n {
            return Err(KernelError::Size(format!(
                "a copy of {n} bytes, which has to be a multiple of 4, between buffers of {} and {}",
                src.size(),
                dst.size()
            )));
        }
        let key = self.context.key;
        if src.device_key() != key || dst.device_key() != key {
            return Err(KernelError::Size("a buffer is on another device".into()));
        }
        if n > 0 {
            encoder.copy_buffer_to_buffer(&src.buffer, 0, &dst.buffer, 0, n);
        }
        Ok(())
    }
}
