//! The Daedalus data-parallel ops on GPU-resident data (`kernels.wgsl`): the GPU column of
//! `docs/backend-matrix.md`. Use them when the data is already on the device (convert before a
//! readback, after a decode). Whether they beat the CPU kernels depends on the device: the
//! matrix records the measured times, and the caller chooses.

use crate::shader::submit_lock::LockedQueue;
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use wgpu::util::DeviceExt as _;

use super::{GpuStorage, GpuTicket};
use crate::{GpuContextHandle, GpuError};

const COMMON: &str = include_str!("kernels_common.wgsl");
const COMPUTE: &str = include_str!("kernels.wgsl");
/// Width of the fragment form's render target, in output words.
const FRAGMENT_WIDTH: u32 = 4096;
const WORKGROUP: u64 = 64;

/// An op [`GpuKernels`] runs; each matches a `daedalus-kernels` CPU kernel bit for bit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GpuKernel {
    /// `n` RGB8 pixels to RGBA8.
    RgbToRgba,
    /// `n` RGBA8 pixels to RGB8.
    RgbaToRgb,
    /// `n` gray pixels to RGBA8.
    GrayToRgba,
    /// `n` RGBA8 pixels to their red channel.
    RgbaRed,
    /// `n` little-endian f32 (a typed view of bytes).
    F32FromLe,
    /// `n` binary16 values to f32.
    F16ToF32,
    /// `n` f32 to unorm8 (clamp, scale, round half away from zero; NaN to 0).
    F32ToUnorm8,
    /// `n` bytes copied by a shader (the copy engine is `copy_buffer_to_buffer`).
    Copy,
    /// `n` bytes to lowercase hex.
    HexLower,
    /// `n` bytes to standard base64 with padding.
    Base64Encode,
    /// `n` standard base64 symbols (a multiple of 4) to bytes; see [`GpuKernels::base64_decode`].
    Base64Decode,
    /// The CRC-32 of each 4 KiB chunk of `n` bytes; [`GpuKernels::crc32`] combines them.
    Crc32Chunks,
}

impl GpuKernel {
    fn entry(self) -> &'static str {
        match self {
            GpuKernel::RgbToRgba => "rgb_to_rgba",
            GpuKernel::RgbaToRgb => "rgba_to_rgb",
            GpuKernel::GrayToRgba => "gray_to_rgba",
            GpuKernel::RgbaRed => "rgba_red",
            GpuKernel::F32FromLe => "f32_from_le",
            GpuKernel::F16ToF32 => "f16_to_f32",
            GpuKernel::F32ToUnorm8 => "f32_to_unorm8",
            GpuKernel::Copy => "copy_words",
            GpuKernel::HexLower => "hex_lower",
            GpuKernel::Base64Encode => "base64_encode",
            GpuKernel::Base64Decode => "base64_decode",
            GpuKernel::Crc32Chunks => "crc32_chunks",
        }
    }

    /// Input bytes for `n` elements.
    pub fn input_len(self, n: u64) -> u64 {
        match self {
            GpuKernel::RgbToRgba => 3 * n,
            GpuKernel::RgbaToRgb | GpuKernel::RgbaRed => 4 * n,
            GpuKernel::F32FromLe | GpuKernel::F32ToUnorm8 => 4 * n,
            GpuKernel::F16ToF32 => 2 * n,
            GpuKernel::GrayToRgba
            | GpuKernel::Copy
            | GpuKernel::HexLower
            | GpuKernel::Base64Encode
            | GpuKernel::Base64Decode
            | GpuKernel::Crc32Chunks => n,
        }
    }

    /// Output bytes for `n` elements (base64 decode: the most, before padding).
    pub fn output_len(self, n: u64) -> u64 {
        match self {
            GpuKernel::RgbToRgba | GpuKernel::GrayToRgba => 4 * n,
            GpuKernel::RgbaToRgb => 3 * n,
            GpuKernel::RgbaRed | GpuKernel::F32ToUnorm8 | GpuKernel::Copy => n,
            GpuKernel::F32FromLe | GpuKernel::F16ToF32 => 4 * n,
            GpuKernel::HexLower => 2 * n,
            GpuKernel::Base64Encode => n.div_ceil(3) * 4,
            GpuKernel::Base64Decode => n / 4 * 3,
            GpuKernel::Crc32Chunks => n.div_ceil(4096) * 4,
        }
    }

    /// Writing threads for `n` elements.
    fn threads(self, n: u64) -> u64 {
        match self {
            GpuKernel::RgbToRgba
            | GpuKernel::GrayToRgba
            | GpuKernel::F32FromLe
            | GpuKernel::F16ToF32 => n,
            GpuKernel::Base64Encode => n.div_ceil(3),
            _ => self.output_len(n).div_ceil(4),
        }
    }
}

/// The CRC-32 byte table, little-endian words.
fn crc_table() -> Vec<u8> {
    (0u32..256)
        .flat_map(|b| {
            let mut crc = b;
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0xEDB8_8320 & (crc & 1).wrapping_neg());
            }
            crc.to_le_bytes()
        })
        .collect()
}

/// Compute pipelines for the [`GpuKernel`] ops on one device, built on first use (with the
/// installed pipeline cache).
pub struct GpuKernels {
    device: wgpu::Device,
    queue: LockedQueue,
    module: wgpu::ShaderModule,
    layout: wgpu::BindGroupLayout,
    pipeline_layout: wgpu::PipelineLayout,
    pipelines: Mutex<HashMap<GpuKernel, Arc<wgpu::ComputePipeline>>>,
    max_groups: u32,
    /// The CRC-32 table (`aux` for `crc32_chunks`).
    crc_table: wgpu::Buffer,
    gpu: GpuContextHandle,
    /// Fragment forms, and their render targets by size.
    fragments: Mutex<HashMap<GpuKernel, Arc<super::FragmentPass>>>,
    targets: Mutex<HashMap<(u32, u32), Arc<super::GpuTexture>>>,
    /// The status buffer of every kernel but base64 decode (which needs its own per call).
    shared_status: wgpu::Buffer,
    /// Bind groups by dispatch shape, so a repeated dispatch allocates nothing in Daedalus.
    compute_bindings: Mutex<HashMap<ComputeKey, Arc<wgpu::BindGroup>>>,
    fragment_bindings: Mutex<HashMap<FragmentKey, Arc<(super::FragmentBinding, GpuStorage)>>>,
}

type ComputeKey = (GpuKernel, wgpu::Buffer, wgpu::Buffer, u64);
type FragmentKey = (GpuKernel, wgpu::Buffer, u64, u32);

/// Cached bindings per `GpuKernels` (cleared when full: steady state reuses a few shapes).
const BINDING_CACHE: usize = 64;

impl std::fmt::Debug for GpuKernels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuKernels").finish_non_exhaustive()
    }
}

fn entry(binding: u32, read_only: bool, min: u64) -> wgpu::BindGroupLayoutEntry {
    wgpu::BindGroupLayoutEntry {
        binding,
        visibility: wgpu::ShaderStages::COMPUTE,
        ty: wgpu::BindingType::Buffer {
            ty: wgpu::BufferBindingType::Storage { read_only },
            has_dynamic_offset: false,
            // The shader's minimum, stated up front: with `None`, wgpu re-checks the bound sizes
            // on every dispatch (and allocates to track them).
            min_binding_size: std::num::NonZeroU64::new(min),
        },
        count: None,
    }
}

impl GpuKernels {
    /// The kernels on `gpu`'s device. `Unsupported` on a non-wgpu backend.
    pub fn new(gpu: &GpuContextHandle) -> Result<Self, GpuError> {
        let (device, queue) = gpu.wgpu_locked().ok_or(GpuError::Unsupported)?;
        let scope = device.push_error_scope(wgpu::ErrorFilter::Validation);
        let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("daedalus-kernels"),
            source: wgpu::ShaderSource::Wgsl(format!("{COMMON}\n{COMPUTE}").into()),
        });
        let layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("daedalus-kernels"),
            entries: &[
                // src, dst, status and aux are `array<u32>` (4 bytes at least), params a
                // 16-byte struct.
                entry(0, true, 4),
                entry(1, false, 4),
                entry(2, true, 16),
                entry(3, false, 8),
                entry(4, true, 4),
            ],
        });
        let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("daedalus-kernels"),
            bind_group_layouts: &[Some(&layout)],
            immediate_size: 0,
        });
        if let Some(err) = pollster::block_on(scope.pop()) {
            return Err(GpuError::Internal(format!("kernels.wgsl rejected: {err}")));
        }
        Ok(Self {
            device: device.clone(),
            queue: queue.clone(),
            module,
            layout,
            pipeline_layout,
            pipelines: Mutex::new(HashMap::new()),
            max_groups: device.limits().max_compute_workgroups_per_dimension,
            crc_table: device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("daedalus-kernels-crc-table"),
                contents: &crc_table(),
                usage: wgpu::BufferUsages::STORAGE,
            }),
            gpu: gpu.clone(),
            fragments: Mutex::new(HashMap::new()),
            targets: Mutex::new(HashMap::new()),
            shared_status: status_buffer(device),
            compute_bindings: Mutex::new(HashMap::new()),
            fragment_bindings: Mutex::new(HashMap::new()),
        })
    }

    fn pipeline(&self, kernel: GpuKernel) -> Arc<wgpu::ComputePipeline> {
        let mut pipelines = self.pipelines.lock();
        pipelines
            .entry(kernel)
            .or_insert_with(|| {
                let cache = super::pipeline_cache_for(&self.device);
                Arc::new(
                    self.device
                        .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                            label: Some(kernel.entry()),
                            layout: Some(&self.pipeline_layout),
                            module: &self.module,
                            entry_point: Some(kernel.entry()),
                            compilation_options: Default::default(),
                            cache: cache.as_ref(),
                        }),
                )
            })
            .clone()
    }

    /// Encode `kernel` over `n` elements from `src` into `dst` (both from offset 0).
    /// `InvalidInput` when a buffer is too small, `n` does not fit a dispatch, or (base64
    /// decode) `n` is not a multiple of 4. Pass `timestamps` to time the pass.
    pub fn encode(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        kernel: GpuKernel,
        src: &GpuStorage,
        dst: &GpuStorage,
        n: u64,
        timestamps: Option<wgpu::ComputePassTimestampWrites<'_>>,
    ) -> Result<(), GpuError> {
        self.encode_with_status(encoder, kernel, src, dst, n, None, timestamps)
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_with_status(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        kernel: GpuKernel,
        src: &GpuStorage,
        dst: &GpuStorage,
        n: u64,
        // A per-call status buffer (base64 decode); `None` uses the shared one and the binding
        // cache.
        status: Option<&wgpu::Buffer>,
        timestamps: Option<wgpu::ComputePassTimestampWrites<'_>>,
    ) -> Result<(), GpuError> {
        let invalid = |what: String| Err(GpuError::InvalidInput(what));
        if n == 0 {
            return Ok(());
        }
        if kernel == GpuKernel::Base64Decode && !n.is_multiple_of(4) {
            return invalid(format!(
                "base64 input of {n} symbols is not a multiple of 4"
            ));
        }
        let (need_in, need_out) = (
            kernel.input_len(n),
            kernel.output_len(n).next_multiple_of(4),
        );
        if src.size() < need_in.next_multiple_of(4) || dst.size() < need_out {
            return invalid(format!(
                "{kernel:?} over {n} needs {need_in} input and {need_out} output bytes; got {} and {}",
                src.size(),
                dst.size()
            ));
        }
        let threads = kernel.threads(n);
        let groups = threads.div_ceil(WORKGROUP);
        let max = u64::from(self.max_groups);
        let (x, y) = if groups <= max {
            (groups, 1)
        } else {
            (max, groups.div_ceil(max))
        };
        if y > max || n > u64::from(u32::MAX) / 4 {
            return invalid(format!(
                "{kernel:?} over {n} elements does not fit one dispatch"
            ));
        }
        let key = (kernel, src.buffer().clone(), dst.buffer().clone(), n);
        let cached = status
            .is_none()
            .then(|| self.compute_bindings.lock().get(&key).cloned())
            .flatten();
        let bind = match cached {
            Some(bind) => bind,
            None => {
                let bind = Arc::new(self.compute_binding(kernel, src, dst, n, threads, status));
                if status.is_none() {
                    let mut cache = self.compute_bindings.lock();
                    if cache.len() >= BINDING_CACHE {
                        cache.clear();
                    }
                    cache.insert(key, bind.clone());
                }
                bind
            }
        };
        let pipeline = self.pipeline(kernel);
        // No pass label: wgpu copies it into a `String` on every pass.
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: None,
            timestamp_writes: timestamps,
        });
        pass.set_pipeline(&pipeline);
        pass.set_bind_group(0, &*bind, &[]);
        pass.dispatch_workgroups(x as u32, y as u32, 1);
        Ok(())
    }

    /// The bind group (and its parameter buffer) for one dispatch shape.
    fn compute_binding(
        &self,
        kernel: GpuKernel,
        src: &GpuStorage,
        dst: &GpuStorage,
        n: u64,
        threads: u64,
        status: Option<&wgpu::Buffer>,
    ) -> wgpu::BindGroup {
        let status = status.unwrap_or(&self.shared_status);
        let params = [n as u32, threads as u32, kernel.output_len(n) as u32, 0];
        let params = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("daedalus-kernels-params"),
                contents: &params
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
                usage: wgpu::BufferUsages::STORAGE,
            });
        self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some(kernel.entry()),
            layout: &self.layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: src.buffer().as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: dst.buffer().as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: params.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: status.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 4,
                    resource: self.crc_table.as_entire_binding(),
                },
            ],
        })
    }

    /// Drop the cached bindings (they hold the source and destination buffers alive).
    pub fn clear_cache(&self) {
        self.compute_bindings.lock().clear();
        self.fragment_bindings.lock().clear();
    }

    fn status_buffer(&self) -> wgpu::Buffer {
        status_buffer(&self.device)
    }

    /// Copy `n` bytes from `src` to `dst` on the copy engine (`copy_buffer_to_buffer`): the
    /// fixed-function form of [`GpuKernel::Copy`]. `n` must be a multiple of 4.
    pub fn copy_with_engine(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        src: &GpuStorage,
        dst: &GpuStorage,
        n: u64,
    ) -> Result<(), GpuError> {
        if !n.is_multiple_of(4) || src.size() < n || dst.size() < n {
            return Err(GpuError::InvalidInput(format!(
                "copy of {n} bytes (a multiple of 4) between {} and {} byte buffers",
                src.size(),
                dst.size()
            )));
        }
        encoder.copy_buffer_to_buffer(src.buffer(), 0, dst.buffer(), 0, n);
        Ok(())
    }

    /// Whether `kernel` has a fragment form ([`GpuKernels::encode_fragment`]): all but base64
    /// decode, whose error report needs atomics.
    pub fn has_fragment(kernel: GpuKernel) -> bool {
        !matches!(kernel, GpuKernel::Base64Decode | GpuKernel::Crc32Chunks)
    }

    /// The CRC-32 (IEEE) of `n` bytes of `src`: chunk CRCs on the GPU, combined on the host
    /// (`daedalus_kernels::crc32_combine`). Waits for the result.
    pub fn crc32(&self, src: &GpuStorage, n: u64) -> Result<u32, GpuError> {
        if n == 0 {
            return Ok(0);
        }
        let chunks = n.div_ceil(4096);
        let dst = GpuStorage::new(&self.gpu, chunks * 4, Some("daedalus-kernels-crc-chunks"))?;
        self.run(GpuKernel::Crc32Chunks, src, &dst, n)?.wait();
        let mut words = vec![0u8; (chunks * 4) as usize];
        dst.read_into(0, &mut words)?;
        let mut crc = 0u32;
        for (i, w) in words.as_chunks::<4>().0.iter().enumerate() {
            let len = (n - i as u64 * 4096).min(4096);
            crc = daedalus_kernels::crc32_combine(crc, u32::from_le_bytes(*w), len);
        }
        Ok(crc)
    }

    fn fragment(&self, kernel: GpuKernel) -> Result<Arc<super::FragmentPass>, GpuError> {
        if let Some(pass) = self.fragments.lock().get(&kernel) {
            return Ok(pass.clone());
        }
        let word = kernel.entry();
        let wgsl = format!(
            "@group(0) @binding(0) var<storage, read> src: array<u32>;
             @group(0) @binding(1) var<storage, read> params: Params;
             {COMMON}
             @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<u32> {{
                 let t = u32(pos.y) * params.unused + u32(pos.x);
                 if (t >= params.threads) {{ return vec4<u32>(0u); }}
                 return vec4<u32>(word_{word}(t), 0u, 0u, 0u);
             }}"
        );
        let pass = Arc::new(super::FragmentPass::with_device(
            &self.device,
            &self.queue,
            &wgsl,
            &[wgpu::TextureFormat::R32Uint],
        )?);
        self.fragments.lock().insert(kernel, pass.clone());
        Ok(pass)
    }

    /// Encode `kernel` as a fragment pass: each fragment of an `R32Uint` target computes one
    /// output word, then the target is copied into `dst`. The fast form on tile-based GPUs (V3D,
    /// Mali), where fragment passes beat compute; the results equal [`GpuKernels::encode`]'s.
    pub fn encode_fragment(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        kernel: GpuKernel,
        src: &GpuStorage,
        dst: &GpuStorage,
        n: u64,
        timestamps: Option<wgpu::RenderPassTimestampWrites<'_>>,
    ) -> Result<(), GpuError> {
        if !Self::has_fragment(kernel) {
            return Err(GpuError::Unsupported);
        }
        if n == 0 {
            return Ok(());
        }
        let (need_in, need_out) = (
            kernel.input_len(n),
            kernel.output_len(n).next_multiple_of(4),
        );
        if src.size() < need_in.next_multiple_of(4) || dst.size() < need_out {
            return Err(GpuError::InvalidInput(format!(
                "{kernel:?} over {n} needs {need_in} input and {need_out} output bytes; got {} and {}",
                src.size(),
                dst.size()
            )));
        }
        let threads = kernel.threads(n);
        let max = self.device.limits().max_texture_dimension_2d;
        let width = FRAGMENT_WIDTH.min(max);
        let height = threads.div_ceil(u64::from(width));
        if height > u64::from(max) {
            return Err(GpuError::InvalidInput(format!(
                "{kernel:?} over {n} elements does not fit one {width}x{max} target"
            )));
        }
        let (w, h) = ((threads.min(u64::from(width))) as u32, height as u32);
        let target = {
            let mut targets = self.targets.lock();
            match targets.get(&(w, h)) {
                Some(t) => t.clone(),
                None => {
                    let t = Arc::new(super::GpuTexture::with_device(
                        &self.device,
                        &self.queue,
                        (w, h),
                        wgpu::TextureFormat::R32Uint,
                        Some("daedalus-kernels-fragment"),
                    )?);
                    targets.insert((w, h), t.clone());
                    t
                }
            }
        };
        let pass = self.fragment(kernel)?;
        let key = (kernel, src.buffer().clone(), n, w);
        let cached = self.fragment_bindings.lock().get(&key).cloned();
        let binding = match cached {
            Some(binding) => binding,
            None => {
                let params = [n as u32, threads as u32, kernel.output_len(n) as u32, w];
                let params = GpuStorage::with_data(
                    &self.gpu,
                    &params
                        .iter()
                        .flat_map(|v| v.to_le_bytes())
                        .collect::<Vec<_>>(),
                    Some("daedalus-kernels-params"),
                )?;
                let binding = Arc::new((pass.bind(&[], &[src, &params])?, params));
                let mut cache = self.fragment_bindings.lock();
                if cache.len() >= BINDING_CACHE {
                    cache.clear();
                }
                cache.insert(key, binding.clone());
                binding
            }
        };
        pass.encode_bound_timed(encoder, &binding.0, &[&target], timestamps)?;
        // Full rows, then the last (partial) row.
        let full_rows = (threads / u64::from(w)) as u32;
        let copy = |encoder: &mut wgpu::CommandEncoder, y: u32, rows: u32, words: u32| {
            encoder.copy_texture_to_buffer(
                wgpu::TexelCopyTextureInfo {
                    texture: target.texture(),
                    mip_level: 0,
                    origin: wgpu::Origin3d { x: 0, y, z: 0 },
                    aspect: wgpu::TextureAspect::All,
                },
                wgpu::TexelCopyBufferInfo {
                    buffer: dst.buffer(),
                    layout: wgpu::TexelCopyBufferLayout {
                        offset: u64::from(y) * u64::from(w) * 4,
                        bytes_per_row: (rows > 1).then_some(w * 4),
                        rows_per_image: None,
                    },
                },
                wgpu::Extent3d {
                    width: words,
                    height: rows,
                    depth_or_array_layers: 1,
                },
            );
        };
        if full_rows > 0 {
            copy(encoder, 0, full_rows, w);
        }
        let rest = (threads - u64::from(full_rows) * u64::from(w)) as u32;
        if rest > 0 {
            copy(encoder, full_rows, 1, rest);
        }
        Ok(())
    }

    /// Encode and submit `kernel`'s fragment form (never waits).
    pub fn run_fragment(
        &self,
        kernel: GpuKernel,
        src: &GpuStorage,
        dst: &GpuStorage,
        n: u64,
    ) -> Result<GpuTicket, GpuError> {
        let mut encoder = self.device.create_command_encoder(&Default::default());
        self.encode_fragment(&mut encoder, kernel, src, dst, n, None)?;
        Ok(GpuTicket::submit_on(
            &self.device,
            &self.queue,
            [encoder.finish()],
        ))
    }

    /// Encode and submit `kernel` (never waits).
    pub fn run(
        &self,
        kernel: GpuKernel,
        src: &GpuStorage,
        dst: &GpuStorage,
        n: u64,
    ) -> Result<GpuTicket, GpuError> {
        let mut encoder = self.device.create_command_encoder(&Default::default());
        self.encode(&mut encoder, kernel, src, dst, n, None)?;
        Ok(GpuTicket::submit_on(
            &self.device,
            &self.queue,
            [encoder.finish()],
        ))
    }

    /// Decode `n` base64 symbols in `src` into `dst` and wait for the result: the decoded
    /// length, or `InvalidInput` naming the first invalid symbol (or stray bits in the last
    /// one), as the CPU decoder does.
    pub fn base64_decode(
        &self,
        src: &GpuStorage,
        dst: &GpuStorage,
        n: u64,
    ) -> Result<u64, GpuError> {
        if n == 0 {
            return Ok(0);
        }
        let status = self.status_buffer();
        let mut encoder = self.device.create_command_encoder(&Default::default());
        self.encode_with_status(
            &mut encoder,
            GpuKernel::Base64Decode,
            src,
            dst,
            n,
            Some(&status),
            None,
        )?;
        let readback = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("daedalus-kernels-status-read"),
            size: 8,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        encoder.copy_buffer_to_buffer(&status, 0, &readback, 0, 8);
        GpuTicket::submit_on(&self.device, &self.queue, [encoder.finish()]).wait();
        let (tx, rx) = std::sync::mpsc::channel();
        self.queue
            .map_async(readback.slice(..), wgpu::MapMode::Read, move |r| {
                drop(tx.send(r))
            });
        let _ = self.device.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });
        rx.recv()
            .map_err(|e| GpuError::Internal(format!("status map canceled: {e}")))?
            .map_err(|e| GpuError::Internal(format!("status map failed: {e:?}")))?;
        let words = {
            let view = readback
                .slice(..)
                .get_mapped_range()
                .map_err(|e| GpuError::Internal(format!("status map failed: {e:?}")))?;
            let (w, _) = view.as_chunks::<4>();
            [u32::from_le_bytes(w[0]), u32::from_le_bytes(w[1])]
        };
        readback.unmap();
        if words[0] != u32::MAX {
            return Err(GpuError::InvalidInput(format!(
                "invalid base64 symbol at offset {}",
                words[0]
            )));
        }
        Ok(u64::from(words[1]))
    }
}

/// A base64-decode status buffer: [no invalid symbol, decoded length 0].
fn status_buffer(device: &wgpu::Device) -> wgpu::Buffer {
    device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("daedalus-kernels-status"),
        contents: &[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0],
        usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
    })
}
