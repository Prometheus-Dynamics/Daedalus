//! Running a kernel op: its shader, loaded on first use, and one pass over a caller's buffers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use super::{Kernel, KernelError};
use crate::WgpuBackend;
use crate::buffer::Storage;
use crate::context::Context;
use crate::fence::{Ticket, TicketState};
use crate::record::RecordCx;
use crate::shader::{BindingKind, BindingSpec, Bound, Dispatch, OpenPass, Shader, ShaderSpec};

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
    kind: BindingKind::Storage {
        read_only: true,
        stride: 0,
    },
};

const COPY_SOURCE: &str = include_str!("copy.wgsl");

/// The vector entry points of the other ops, with the common text they call.
const WIDE_SOURCE: &str = concat!(include_str!("common.wgsl"), include_str!("wide.wgsl"));

/// The copy's bindings: the input and output as vectors of four words, so one invocation's
/// loads and stores are vectors. A binding is at least one element, so a copy this shape is
/// used for is at least a vector on both sides.
const COPY: &[BindingSpec] = &[
    BindingSpec {
        index: 0,
        name: "src",
        kind: BindingKind::Storage {
            read_only: true,
            stride: 16,
        },
    },
    BindingSpec {
        index: 1,
        name: "dst",
        kind: BindingKind::Storage {
            read_only: false,
            stride: 16,
        },
    },
    PARAMS,
];

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

/// The indexes in [`SPECS`] of the vector entry points: each op with a whole-vector shape, for
/// counts that are whole vectors. The others use the word text.
pub(super) const WIDE_COPY: usize = 8;
pub(super) const WIDE_F16: usize = 9;
pub(super) const WIDE_UNORM8: usize = 10;
pub(super) const WIDE_HEX: usize = 11;

/// In the order of [`Kernel`], then the vector entry points above.
pub(super) const SPECS: [ShaderSpec; 12] = [
    spec("kernels.f32_from_le", "f32_from_le", PLAIN),
    spec("kernels.f16_to_f32", "f16_to_f32", PLAIN),
    spec("kernels.f32_to_unorm8", "f32_to_unorm8", PLAIN),
    spec("kernels.copy", "copy_words", PLAIN),
    spec("kernels.hex_lower", "hex_lower", PLAIN),
    spec("kernels.base64_encode", "base64_encode", PLAIN),
    spec("kernels.base64_decode", "base64_decode", DECODE),
    spec("kernels.crc32_chunks", "crc32_chunks", CRC),
    ShaderSpec {
        name: "kernels.copy.wide",
        entry: "copy_quads",
        source: COPY_SOURCE,
        workgroup: [WORKGROUP, 1, 1],
        bindings: COPY,
        dispatch: Dispatch::Counts([1, 1, 1]),
        inputs: &[],
        outputs: &[],
    },
    ShaderSpec {
        name: "kernels.f16_to_f32.wide",
        entry: "f16_to_f32_wide",
        source: WIDE_SOURCE,
        workgroup: [WORKGROUP, 1, 1],
        bindings: COPY,
        dispatch: Dispatch::Counts([1, 1, 1]),
        inputs: &[],
        outputs: &[],
    },
    ShaderSpec {
        name: "kernels.f32_to_unorm8.wide",
        entry: "f32_to_unorm8_wide",
        source: WIDE_SOURCE,
        workgroup: [WORKGROUP, 1, 1],
        bindings: COPY,
        dispatch: Dispatch::Counts([1, 1, 1]),
        inputs: &[],
        outputs: &[],
    },
    ShaderSpec {
        name: "kernels.hex_lower.wide",
        entry: "hex_lower_wide",
        source: WIDE_SOURCE,
        workgroup: [WORKGROUP, 1, 1],
        bindings: COPY,
        dispatch: Dispatch::Counts([1, 1, 1]),
        inputs: &[],
        outputs: &[],
    },
];

/// One op, checked: its counts on the device, and how many workgroups it dispatches.
/// The params buffer, the counts, and the index in [`SPECS`] of the shader that runs.
pub(super) type Planned = (Arc<Storage>, [u32; 3], usize);

/// Distinct counts whose parameter buffers are kept.
const PARAMS_KEPT: usize = 64;

/// The kernel ops on one device. A shader is built the first time its op is used, or found if
/// this device has built it before. After a lost device is replaced, make another: this one
/// keeps answering with the device's error.
pub struct Kernels {
    pub(super) context: Arc<Context>,
    shaders: [OnceLock<Shader>; 12],
    /// The counts of a run, on the device, by what they are: one small buffer for each
    /// distinct set, bound as it is. Two runs of an op in one encoder each bind their own, and
    /// nothing is copied before a pass.
    params: Mutex<HashMap<[u32; 3], Arc<Storage>>>,
    pub(super) table: OnceLock<Storage>,
    /// Ops run with Vulkan itself, where the device is on Vulkan.
    #[cfg(target_os = "linux")]
    pub(super) chains: super::chained::Chains,
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
            shaders: [const { OnceLock::new() }; 12],
            params: Mutex::new(HashMap::new()),
            table: OnceLock::new(),
            #[cfg(target_os = "linux")]
            chains: super::chained::Chains::default(),
        }
    }

    /// Which device this is on. A buffer with the same key is on it.
    pub fn device_key(&self) -> u64 {
        self.context.key
    }

    /// The shader of spec `at` in [`SPECS`], loaded on first use.
    pub(super) fn shader(&self, at: usize) -> Result<&Shader, KernelError> {
        let slot = &self.shaders[at];
        if let Some(shader) = slot.get() {
            return Ok(shader);
        }
        let loaded =
            Shader::load_on(Arc::clone(&self.context), &SPECS[at]).map_err(KernelError::Shader)?;
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
        self.encode_with(
            (&mut OpenPass::default(), encoder),
            kernel,
            src,
            dst,
            n,
            None,
        )
    }

    /// Checks one op against its buffers, and gives its counts on the device and how many
    /// workgroups it dispatches. `None` when there is nothing to run.
    pub(super) fn plan(
        &self,
        kernel: Kernel,
        src: &Storage,
        dst: &Storage,
        n: u64,
    ) -> Result<Option<Planned>, KernelError> {
        if n == 0 {
            return Ok(None);
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
        // Each op with a vector shape takes it for a count that is a whole number of its
        // vectors, on both sides, so no vector is partly past the output: a device clamps a
        // storage index past a buffer's end onto its last element, which would overwrite a whole
        // one. The other counts take the word text. F32FromLe is a copy of its bits.
        let (spec, threads) = match kernel {
            Kernel::Copy | Kernel::F32FromLe
                if kernel.output_len(n) >= 16 && kernel.output_len(n).is_multiple_of(16) =>
            {
                (WIDE_COPY, kernel.output_len(n).div_ceil(16))
            }
            Kernel::F16ToF32 if n >= 8 && n.is_multiple_of(8) => (WIDE_F16, n / 8),
            Kernel::F32ToUnorm8 if n >= 16 && n.is_multiple_of(16) => (WIDE_UNORM8, n / 16),
            Kernel::HexLower if n >= 16 && n.is_multiple_of(16) => (WIDE_HEX, n / 16),
            _ => (kernel.index(), kernel.threads(n)),
        };
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
        Ok(Some((params, [x as u32, y as u32, 1], spec)))
    }

    /// Records one op into `into`: the pass kept open, and the encoder it is in.
    pub(super) fn encode_with(
        &self,
        (open, encoder): (&mut OpenPass, &mut wgpu::CommandEncoder),
        kernel: Kernel,
        src: &Storage,
        dst: &Storage,
        n: u64,
        extra: Option<&Storage>,
    ) -> Result<(), KernelError> {
        let Some((params, counts, spec)) = self.plan(kernel, src, dst, n)? else {
            return Ok(());
        };
        let shader = self.shader(spec)?;
        let plain = [
            Bound::Storage(src),
            Bound::Storage(dst),
            Bound::Storage(&params),
        ];
        let with = extra.map(|extra| [plain[0], plain[1], plain[2], Bound::Storage(extra)]);
        let bound: &[Bound<'_>] = match &with {
            Some(with) => with,
            None => &plain,
        };
        shader
            .encode_sized_in(open, encoder, bound, counts)
            .map_err(KernelError::Shader)
    }

    /// Several ops recorded together: one pass for all of them, and one submit. This is the
    /// cheapest way to run more than one op, on every device.
    pub fn batch(&self) -> Batch<'_> {
        #[cfg(target_os = "linux")]
        if let Some(ops) = self.chains.begin(self) {
            return Batch {
                encoded: None,
                chained: Some(ops),
                kernels: self,
            };
        }
        Batch {
            encoded: Some((OpenPass::default(), self.encoder())),
            #[cfg(target_os = "linux")]
            chained: None,
            kernels: self,
        }
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
        let mut batch = self.batch();
        batch.add(kernel, src, dst, n)?;
        Ok(batch.submit())
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
            encoder.copy_buffer_to_buffer(src.buffer(), 0, dst.buffer(), 0, n);
        }
        Ok(())
    }
}

/// Ops being recorded together. They share one compute pass, and [`Batch::submit`] submits
/// them all at once.
pub struct Batch<'k> {
    /// Through wgpu's encoder: the pass kept open, and the encoder it is in. The pass is first,
    /// so it ends before the encoder is finished or dropped.
    encoded: Option<(OpenPass, wgpu::CommandEncoder)>,
    /// With Vulkan itself: the ops are listed, and recorded when the batch is submitted.
    #[cfg(target_os = "linux")]
    chained: Option<Vec<super::chained::Op>>,
    kernels: &'k Kernels,
}

impl Batch<'_> {
    /// Records `kernel` over `n` elements of `src` into `dst`. Base64 decode isn't batched: it
    /// reports a length, which [`Kernels::base64_decode`] reads.
    pub fn add(
        &mut self,
        kernel: Kernel,
        src: &Storage,
        dst: &Storage,
        n: u64,
    ) -> Result<(), KernelError> {
        let extra = match kernel {
            Kernel::Base64Decode => {
                return Err(KernelError::Size(
                    "base64 decode reports a length: run it with `base64_decode`".into(),
                ));
            }
            Kernel::Crc32Chunks => Some(self.kernels.crc_table()?),
            _ => None,
        };
        #[cfg(target_os = "linux")]
        if let Some(ops) = &mut self.chained {
            return super::chained::add(self.kernels, ops, kernel, [src, dst], n, extra);
        }
        match &mut self.encoded {
            Some((open, encoder)) => {
                self.kernels
                    .encode_with((open, encoder), kernel, src, dst, n, extra)
            }
            None => Ok(()),
        }
    }

    /// Submits everything recorded and gives the ticket for it. Never waits.
    pub fn submit(self) -> Ticket {
        #[cfg(target_os = "linux")]
        if let Some(ops) = self.chained {
            return self.kernels.chains.submit(self.kernels, ops);
        }
        match self.encoded {
            Some((mut open, encoder)) => {
                open.close();
                self.kernels.submit(encoder)
            }
            None => Ticket::done(Arc::clone(&self.kernels.context)),
        }
    }
}
