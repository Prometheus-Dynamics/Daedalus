//! The image ops on GPU-resident frames (`image.wgsl`): the GPU form of the `daedalus.image.*`
//! ops in `docs/backend-matrix.md`, bit-exact with their scalar references in
//! `daedalus_kernels::reference`. Line is not here: one line is a serial walk, so it has no
//! data-parallel form (the registry says so).

use daedalus_kernels::reference::Blend;
use wgpu::util::DeviceExt as _;

use super::{GpuStorage, GpuTicket};
use crate::shader::submit_lock::LockedQueue;
use crate::{GpuContextHandle, GpuError};

const SHADER: &str = include_str!("image.wgsl");
const ENTRY: &str = "image_op";
const WG: u32 = 64;
/// The `Params` struct of `image.wgsl`: 16 u32s.
const PARAMS_BYTES: u64 = 64;
/// Byte indices stay in u32 arithmetic in the shader: a frame is at most this many bytes.
const MAX_BYTES: u64 = 1 << 30;
/// Bilinear taps are exact in u32 only below this many output samples per axis.
const MAX_TAP_SAMPLES: u64 = 1 << 20;

/// The shape of a tightly packed frame: `width` by `height` pixels of `channels` bytes each.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageShape {
    pub width: u32,
    pub height: u32,
    pub channels: u32,
}

impl ImageShape {
    pub const fn new(width: u32, height: u32, channels: u32) -> Self {
        Self {
            width,
            height,
            channels,
        }
    }

    /// Bytes in the frame.
    pub fn bytes(self) -> u64 {
        u64::from(self.width) * u64::from(self.height) * u64::from(self.channels)
    }
}

/// The mask operations of [`ImageOp::MaskRop`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaskRop {
    And,
    Or,
    Xor,
}

/// An image op and its parameters. The [`ImageCall::aux`] input is the palette (256 RGBA8
/// entries) for [`ImageOp::Palette`], the fill colour (`channels` bytes) for [`ImageOp::Fill`],
/// the destination for [`ImageOp::Blend`] (the call's `src` is the foreground) and the second
/// mask for [`ImageOp::MaskRop`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ImageOp {
    /// Block means by an integer factor (`reference::resize_area`).
    ResizeArea { factor: u32 },
    /// Bilinear resize to `out_width` by `out_height` (`reference::resize_bilinear`).
    ResizeBilinear { out_width: u32, out_height: u32 },
    /// The `width` by `height` rectangle at (`left`, `top`) (`reference::crop`).
    Crop {
        left: u32,
        top: u32,
        width: u32,
        height: u32,
    },
    /// Mirror left to right and/or top to bottom (`reference::flip`).
    Flip { horizontal: bool, vertical: bool },
    /// Quarter turns clockwise (`reference::rotate`).
    Rotate { quarters: u32 },
    /// The rectangle set to the fill colour in `aux` (`reference::fill`).
    Fill {
        left: u32,
        top: u32,
        width: u32,
        height: u32,
    },
    /// Porter-Duff blend of RGBA8 pixels (`reference::blend`).
    Blend { mode: Blend, premultiplied: bool },
    /// Index bytes to RGBA8 through the palette in `aux` (`reference::palette`).
    Palette,
    /// 255 where R, G and B lie in the ranges, else 0 (`reference::color_key_mask`).
    ColorKeyMask { min: [u8; 3], max: [u8; 3] },
    /// AND, OR or XOR of `src` and `aux` (`reference::mask_rop`).
    MaskRop(MaskRop),
}

/// One dispatch: `op` over `src` (of `shape`), with `aux` where the op takes one, into `dst`.
#[derive(Clone, Copy, Debug)]
pub struct ImageCall<'a> {
    pub op: ImageOp,
    /// The shape of `src`.
    pub shape: ImageShape,
    pub src: &'a GpuStorage,
    pub aux: Option<&'a GpuStorage>,
    pub dst: &'a GpuStorage,
}

/// What one op reads and writes, and the parameters the shader takes.
struct Plan {
    out: ImageShape,
    src_bytes: u64,
    aux_bytes: u64,
    params: [u32; 16],
}

fn invalid<T>(what: String) -> Result<T, GpuError> {
    Err(GpuError::InvalidInput(what))
}

fn plan(op: ImageOp, s: ImageShape) -> Result<Plan, GpuError> {
    let ImageShape {
        width: w,
        height: h,
        channels: ch,
    } = s;
    if w == 0 || h == 0 || ch == 0 {
        return invalid(format!("image {s:?} has an empty dimension"));
    }
    if s.bytes() > MAX_BYTES {
        return invalid(format!(
            "image {s:?} is larger than the shader's byte indices"
        ));
    }
    let (w64, h64) = (u64::from(w), u64::from(h));
    let inside = |left: u32, top: u32, rw: u32, rh: u32| -> Result<(), GpuError> {
        if rw == 0 || rh == 0 {
            return invalid(format!("rectangle {rw}x{rh} is empty"));
        }
        if u64::from(left) + u64::from(rw) > w64 || u64::from(top) + u64::from(rh) > h64 {
            return invalid(format!(
                "rectangle at ({left}, {top}) {rw}x{rh} leaves {s:?}"
            ));
        }
        Ok(())
    };
    let same = s.bytes();
    // Set by each arm: the output shape, the aux bytes, the op code and f0..f7.
    let (out, aux_bytes, code, f): (ImageShape, u64, u32, [u32; 8]) = match op {
        ImageOp::ResizeArea { factor } => {
            if factor == 0 {
                return invalid("an area factor of 0".into());
            }
            let out = ImageShape::new((w / factor).max(1), (h / factor).max(1), ch);
            (out, 0, 0, [factor, 0, 0, 0, 0, 0, 0, 0])
        }
        ImageOp::ResizeBilinear {
            out_width,
            out_height,
        } => {
            if out_width == 0 || out_height == 0 {
                return invalid("a bilinear resize to an empty image".into());
            }
            // The taps: (2 d + 1) * src and 2 * dst * 4096 must fit u32.
            let (ow, oh) = (u64::from(out_width), u64::from(out_height));
            let fits_x = 2 * (ow + 1) * w64 < 1 << 32 && ow < MAX_TAP_SAMPLES;
            let fits_y = 2 * (oh + 1) * h64 < 1 << 32 && oh < MAX_TAP_SAMPLES;
            if !fits_x || !fits_y {
                return invalid(format!(
                    "bilinear taps of {s:?} to {out_width}x{out_height} overflow u32"
                ));
            }
            (ImageShape::new(out_width, out_height, ch), 0, 1, [0; 8])
        }
        ImageOp::Crop {
            left,
            top,
            width,
            height,
        } => {
            inside(left, top, width, height)?;
            (
                ImageShape::new(width, height, ch),
                0,
                2,
                [left, top, 0, 0, 0, 0, 0, 0],
            )
        }
        ImageOp::Flip {
            horizontal,
            vertical,
        } => (
            s,
            0,
            3,
            [u32::from(horizontal), u32::from(vertical), 0, 0, 0, 0, 0, 0],
        ),
        ImageOp::Rotate { quarters } => {
            let q = quarters % 4;
            let out = if q % 2 == 1 {
                ImageShape::new(h, w, ch)
            } else {
                s
            };
            (out, 0, 4, [q, 0, 0, 0, 0, 0, 0, 0])
        }
        ImageOp::Fill {
            left,
            top,
            width,
            height,
        } => {
            inside(left, top, width, height)?;
            (s, u64::from(ch), 5, [left, top, width, height, 0, 0, 0, 0])
        }
        ImageOp::Blend {
            mode,
            premultiplied,
        } => {
            if ch != 4 {
                return invalid(format!("a blend takes RGBA8 pixels, not {ch} channels"));
            }
            let mode = match mode {
                Blend::SrcOver => 0,
                Blend::Src => 1,
                Blend::DstOver => 2,
            };
            (
                s,
                same,
                6,
                [mode, u32::from(premultiplied), 0, 0, 0, 0, 0, 0],
            )
        }
        ImageOp::Palette => {
            if ch != 1 {
                return invalid(format!("a palette takes index bytes, not {ch} channels"));
            }
            (ImageShape::new(w, h, 4), 4 * 256, 7, [0; 8])
        }
        ImageOp::ColorKeyMask { min, max } => {
            if ch < 3 {
                return invalid(format!("a colour key reads R, G and B, not {ch} channels"));
            }
            (
                ImageShape::new(w, h, 1),
                0,
                8,
                [
                    u32::from(min[0]),
                    u32::from(min[1]),
                    u32::from(min[2]),
                    u32::from(max[0]),
                    u32::from(max[1]),
                    u32::from(max[2]),
                    0,
                    0,
                ],
            )
        }
        ImageOp::MaskRop(op) => {
            let f0 = match op {
                MaskRop::And => 0,
                MaskRop::Or => 1,
                MaskRop::Xor => 2,
            };
            (s, same, 9, [f0, 0, 0, 0, 0, 0, 0, 0])
        }
    };
    let src_bytes = s.bytes();
    let out_len = out.bytes();
    if out_len > MAX_BYTES {
        return invalid(format!(
            "{op:?} over {s:?} writes {out_len} bytes, more than the shader indexes"
        ));
    }
    let mut params = [0u32; 16];
    params[0] = code;
    params[1] = w;
    params[2] = h;
    params[3] = ch;
    params[4] = out.width;
    params[5] = out.height;
    params[6] = out_len as u32;
    params[7] = out_len.div_ceil(4) as u32;
    params[8..16].copy_from_slice(&f);
    Ok(Plan {
        out,
        src_bytes,
        aux_bytes,
        params,
    })
}

/// The compute pipeline of the image ops on one device (one entry point, the op in the
/// parameters).
pub struct GpuImages {
    device: wgpu::Device,
    queue: LockedQueue,
    pipeline: wgpu::ComputePipeline,
    layout: wgpu::BindGroupLayout,
    max_groups: u32,
    /// Stands in for `aux` where an op takes none.
    unused_aux: GpuStorage,
}

impl std::fmt::Debug for GpuImages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuImages").finish_non_exhaustive()
    }
}

fn entry(binding: u32, read_only: bool, min: u64) -> wgpu::BindGroupLayoutEntry {
    wgpu::BindGroupLayoutEntry {
        binding,
        visibility: wgpu::ShaderStages::COMPUTE,
        ty: wgpu::BindingType::Buffer {
            ty: wgpu::BufferBindingType::Storage { read_only },
            has_dynamic_offset: false,
            min_binding_size: std::num::NonZeroU64::new(min),
        },
        count: None,
    }
}

impl GpuImages {
    /// The image pipeline on `gpu`'s device. `Unsupported` on a non-wgpu backend.
    pub fn new(gpu: &GpuContextHandle) -> Result<Self, GpuError> {
        let (device, queue) = gpu.wgpu_locked().ok_or(GpuError::Unsupported)?;
        let scope = device.push_error_scope(wgpu::ErrorFilter::Validation);
        let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("daedalus-image"),
            source: wgpu::ShaderSource::Wgsl(SHADER.into()),
        });
        let layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("daedalus-image"),
            entries: &[
                entry(0, true, 4),
                entry(1, true, 4),
                entry(2, false, 4),
                entry(3, true, PARAMS_BYTES),
            ],
        });
        let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("daedalus-image"),
            bind_group_layouts: &[Some(&layout)],
            immediate_size: 0,
        });
        let cache = super::pipeline_cache_for(device);
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some(ENTRY),
            layout: Some(&pipeline_layout),
            module: &module,
            entry_point: Some(ENTRY),
            compilation_options: Default::default(),
            cache: cache.as_ref(),
        });
        if let Some(err) = pollster::block_on(scope.pop()) {
            return Err(GpuError::Internal(format!("image.wgsl rejected: {err}")));
        }
        Ok(Self {
            device: device.clone(),
            queue: queue.clone(),
            pipeline,
            layout,
            max_groups: device.limits().max_compute_workgroups_per_dimension,
            unused_aux: GpuStorage::new(gpu, 4, Some("daedalus-image-aux"))?,
        })
    }

    /// The shape `op` writes for a frame of `shape`, or `InvalidInput`.
    pub fn output(op: ImageOp, shape: ImageShape) -> Result<ImageShape, GpuError> {
        plan(op, shape).map(|p| p.out)
    }

    /// Encode `call` into `encoder`, returning the output shape. `InvalidInput` when the shape
    /// does not fit the op, a buffer is too small, or the dispatch is too large.
    pub fn encode(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        call: ImageCall<'_>,
        timestamps: Option<wgpu::ComputePassTimestampWrites<'_>>,
    ) -> Result<ImageShape, GpuError> {
        let plan = plan(call.op, call.shape)?;
        if call.src.size() < plan.src_bytes {
            return invalid(format!(
                "{:?} over {:?} needs {} source bytes; got {}",
                call.op,
                call.shape,
                plan.src_bytes,
                call.src.size()
            ));
        }
        let aux = match (plan.aux_bytes, call.aux) {
            (0, _) => &self.unused_aux,
            (need, Some(aux)) if aux.size() >= need => aux,
            (need, _) => {
                return invalid(format!("{:?} needs {need} aux bytes", call.op));
            }
        };
        let out_len = plan.out.bytes();
        if call.dst.size() < out_len.next_multiple_of(4) {
            return invalid(format!(
                "{:?} over {:?} writes {out_len} bytes; the destination holds {}",
                call.op,
                call.shape,
                call.dst.size()
            ));
        }
        let threads = u64::from(plan.params[7]);
        let groups = threads.div_ceil(u64::from(WG));
        let max = u64::from(self.max_groups);
        let (x, y) = if groups <= max {
            (groups, 1)
        } else {
            (max, groups.div_ceil(max))
        };
        if y > max {
            return invalid(format!(
                "{:?} over {:?} does not fit one dispatch",
                call.op, call.shape
            ));
        }
        let params = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("daedalus-image-params"),
                contents: &plan
                    .params
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
                usage: wgpu::BufferUsages::STORAGE,
            });
        let bind = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some(ENTRY),
            layout: &self.layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: call.src.buffer().as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: aux.buffer().as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: call.dst.buffer().as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: params.as_entire_binding(),
                },
            ],
        });
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: None,
            timestamp_writes: timestamps,
        });
        pass.set_pipeline(&self.pipeline);
        pass.set_bind_group(0, &bind, &[]);
        pass.dispatch_workgroups(x as u32, y as u32, 1);
        Ok(plan.out)
    }

    /// Encode and submit `call` (never waits).
    pub fn run(&self, call: ImageCall<'_>) -> Result<GpuTicket, GpuError> {
        let mut encoder = self.device.create_command_encoder(&Default::default());
        self.encode(&mut encoder, call, None)?;
        Ok(GpuTicket::submit_on(
            &self.device,
            &self.queue,
            [encoder.finish()],
        ))
    }
}
