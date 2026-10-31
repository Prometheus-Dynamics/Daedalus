//! The Daedalus data-parallel ops on GPU-resident data (`kernels.wgsl`): the GPU column of
//! `docs/backend-matrix.md`. Use them when the data is already on the device (convert before a
//! readback, after a decode). Whether they beat the CPU kernels depends on the device: the
//! matrix records the measured times, and the caller chooses.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use wgpu::util::DeviceExt as _;

use super::{GpuStorage, GpuTicket};
use crate::{GpuContextHandle, GpuError};

const WGSL: &str = include_str!("kernels.wgsl");
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
            | GpuKernel::Base64Decode => n,
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

/// Compute pipelines for the [`GpuKernel`] ops on one device, built on first use (with the
/// installed pipeline cache).
pub struct GpuKernels {
    device: wgpu::Device,
    queue: wgpu::Queue,
    module: wgpu::ShaderModule,
    layout: wgpu::BindGroupLayout,
    pipeline_layout: wgpu::PipelineLayout,
    pipelines: Mutex<HashMap<GpuKernel, Arc<wgpu::ComputePipeline>>>,
    max_groups: u32,
}

impl std::fmt::Debug for GpuKernels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuKernels").finish_non_exhaustive()
    }
}

fn entry(binding: u32, read_only: bool) -> wgpu::BindGroupLayoutEntry {
    wgpu::BindGroupLayoutEntry {
        binding,
        visibility: wgpu::ShaderStages::COMPUTE,
        ty: wgpu::BindingType::Buffer {
            ty: wgpu::BufferBindingType::Storage { read_only },
            has_dynamic_offset: false,
            min_binding_size: None,
        },
        count: None,
    }
}

impl GpuKernels {
    /// The kernels on `gpu`'s device. `Unsupported` on a non-wgpu backend.
    pub fn new(gpu: &GpuContextHandle) -> Result<Self, GpuError> {
        let (device, queue) = gpu.wgpu().ok_or(GpuError::Unsupported)?;
        let scope = device.push_error_scope(wgpu::ErrorFilter::Validation);
        let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("daedalus-kernels"),
            source: wgpu::ShaderSource::Wgsl(WGSL.into()),
        });
        let layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("daedalus-kernels"),
            entries: &[
                entry(0, true),
                entry(1, false),
                entry(2, true),
                entry(3, false),
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
        let status = self.status_buffer();
        self.encode_with_status(encoder, kernel, src, dst, n, &status, timestamps)
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_with_status(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        kernel: GpuKernel,
        src: &GpuStorage,
        dst: &GpuStorage,
        n: u64,
        status: &wgpu::Buffer,
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
        let bind = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
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
            ],
        });
        let pipeline = self.pipeline(kernel);
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some(kernel.entry()),
            timestamp_writes: timestamps,
        });
        pass.set_pipeline(&pipeline);
        pass.set_bind_group(0, &bind, &[]);
        pass.dispatch_workgroups(x as u32, y as u32, 1);
        Ok(())
    }

    fn status_buffer(&self) -> wgpu::Buffer {
        self.device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("daedalus-kernels-status"),
                contents: &[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0],
                usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
            })
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
            &status,
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
        readback
            .slice(..)
            .map_async(wgpu::MapMode::Read, move |r| drop(tx.send(r)));
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
