//! The copy from a buffer of image rows into a texture.

use std::fmt;
use std::sync::Arc;

use super::fold::{Plan, plan};
use crate::WgpuBackend;
use crate::buffer::Storage;
use crate::context::{Context, OpenError};
use crate::fence::{Ticket, TicketState};
use crate::fragment::{FragmentBinding, FragmentError, FragmentPass};
use crate::texture::{Format, Texture, TextureDesc, TextureError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImageError {
    Open(OpenError),
    /// The stride, the size or the buffer doesn't fit the image. The text says which.
    Invalid(String),
    Texture(TextureError),
    Fragment(FragmentError),
}

impl fmt::Display for ImageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ImageError::Open(error) => error.fmt(f),
            ImageError::Invalid(text) => f.write_str(text),
            ImageError::Texture(error) => error.fmt(f),
            ImageError::Fragment(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ImageError {}

/// What a folded copy needs beside the texture: the wide texture the copy lands in, and the
/// pass that unfolds it.
struct Unfold {
    wide: Texture,
    pass: FragmentPass,
    binding: FragmentBinding,
}

/// The repack that turns rows at any stride into rows at a stride the device copies: one compute
/// pass for the whole image, where a copy a row at a time would be a command for each row.
struct Repack {
    pipeline: wgpu::ComputePipeline,
    /// The packed rows, `pitch` bytes apart, which the texture copy reads.
    out: Storage,
    /// Bytes a row of the image takes.
    row: u64,
    /// Bytes from the start of one packed row to the next: a multiple of the copy's alignment.
    pitch: u64,
    height: u32,
}

/// The repack's shader. Each invocation packs one word of the output: four bytes, each read from
/// the source at the image's stride, and zero past the image's row.
const REPACK: &str = "
struct Params {
    offset: u32,
    stride: u32,
    row: u32,
    pitch: u32,
    height: u32,
    size: u32,
    pad0: u32,
    pad1: u32,
};
@group(0) @binding(0) var<uniform> p: Params;
@group(0) @binding(1) var<storage, read> src: array<u32>;
@group(0) @binding(2) var<storage, read_write> dst: array<u32>;

fn byte_at(addr: u32) -> u32 {
    if (addr >= p.size) {
        return 0u;
    }
    return (src[addr / 4u] >> ((addr % 4u) * 8u)) & 0xffu;
}

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
    let word = id.x + id.y * 65535u * 64u;
    if (word >= p.pitch * p.height / 4u) {
        return;
    }
    var packed = 0u;
    for (var k = 0u; k < 4u; k = k + 1u) {
        let at = word * 4u + k;
        let y = at / p.pitch;
        let x = at % p.pitch;
        var v = 0u;
        if (x < p.row) {
            v = byte_at(p.offset + y * p.stride + x);
        }
        packed = packed | (v << (k * 8u));
    }
    dst[word] = packed;
}
";

impl Repack {
    fn new(backend: &WgpuBackend, desc: TextureDesc) -> Result<Repack, ImageError> {
        let context = backend.context().map_err(ImageError::Open)?;
        let row = u64::from(desc.width) * u64::from(desc.format.bytes_per_pixel());
        let pitch = row.next_multiple_of(256);
        let out = Storage::new(backend, pitch * u64::from(desc.height))
            .map_err(|error| ImageError::Invalid(error.to_string()))?;
        let module = context
            .device
            .create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some("image repack"),
                source: wgpu::ShaderSource::Wgsl(REPACK.into()),
            });
        let pipeline = context
            .device
            .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                label: Some("image repack"),
                layout: None,
                module: &module,
                entry_point: Some("main"),
                compilation_options: Default::default(),
                cache: None,
            });
        Ok(Repack {
            pipeline,
            out,
            row,
            pitch,
            height: desc.height,
        })
    }

    /// Records the pack of the image at `offset` of `buffer`, rows `stride` bytes apart.
    fn encode(
        &self,
        context: &Context,
        encoder: &mut wgpu::CommandEncoder,
        buffer: &Storage,
        offset: u64,
        stride: u64,
    ) -> Result<(), ImageError> {
        // Each call has its own parameters and bind group, so two images encoded before one
        // submit cannot see each other's.
        let mut params = [0u32; 8];
        params[0] = offset as u32;
        params[1] = stride as u32;
        params[2] = self.row as u32;
        params[3] = self.pitch as u32;
        params[4] = self.height;
        params[5] = buffer.size() as u32;
        let uniform = context.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("image repack parameters"),
            size: 32,
            usage: wgpu::BufferUsages::UNIFORM,
            mapped_at_creation: true,
        });
        {
            let mut view = uniform
                .slice(..)
                .get_mapped_range_mut()
                .map_err(|error| ImageError::Invalid(error.to_string()))?;
            let bytes: Vec<u8> = params
                .iter()
                .flat_map(|value| value.to_le_bytes())
                .collect();
            view.copy_from_slice(&bytes);
        }
        uniform.unmap();
        let layout = self.pipeline.get_bind_group_layout(0);
        let bind = context
            .device
            .create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("image repack"),
                layout: &layout,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: uniform.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: buffer.wgpu().as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: self.out.wgpu().as_entire_binding(),
                    },
                ],
            });
        let words = self.pitch * u64::from(self.height) / 4;
        let groups = words.div_ceil(64);
        let (x, y) = (groups.min(65535), groups.div_ceil(65535));
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("image repack"),
            timestamp_writes: None,
        });
        pass.set_pipeline(&self.pipeline);
        pass.set_bind_group(0, &bind, &[]);
        pass.dispatch_workgroups(x as u32, y as u32, 1);
        Ok(())
    }
}

/// A copy of images of one size, format and stride into one texture, set up once and run for
/// each image. Made at setup.
pub struct ImageCopy {
    texture: Texture,
    stride: u64,
    plan: Plan,
    unfold: Option<Unfold>,
    repack: Option<Repack>,
    context: Arc<Context>,
}

/// The WGSL scalar a format's texels are.
fn scalar(format: Format) -> &'static str {
    match format {
        Format::R8Uint | Format::Rgba16Uint | Format::Rg32Uint => "u32",
        _ => "f32",
    }
}

impl ImageCopy {
    /// A copy of `desc`-sized images whose rows are `stride` bytes apart in their buffer.
    pub fn new(backend: &WgpuBackend, desc: TextureDesc, stride: u64) -> Result<Self, ImageError> {
        let context = backend.context().map_err(ImageError::Open)?;
        let pixel = desc.format.bytes_per_pixel();
        if stride < u64::from(desc.width) * u64::from(pixel) {
            return Err(ImageError::Invalid(format!(
                "a stride of {stride} is shorter than a row of {} pixels of {pixel} bytes",
                desc.width
            )));
        }
        let texture = Texture::new(backend, desc).map_err(ImageError::Texture)?;
        let max_side = context.limits.max_texture_dimension_2d;
        let plan = plan(stride, desc.width, desc.height, pixel, max_side);
        let unfold = match plan {
            Plan::Folded { rows } => {
                let across = (stride / u64::from(pixel)) as u32;
                let wide = TextureDesc {
                    format: desc.format,
                    width: across * rows,
                    height: desc.height / rows,
                };
                let wide = Texture::new(backend, wide).map_err(ImageError::Texture)?;
                let ty = scalar(desc.format);
                let wgsl = format!(
                    "@group(0) @binding(0) var folded: texture_2d<{ty}>;
                     @fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<{ty}> {{
                         let at = vec2<u32>(pos.xy);
                         let folded_at = vec2<u32>(at.x + (at.y % {rows}u) * {across}u, at.y / {rows}u);
                         return textureLoad(folded, vec2<i32>(folded_at), 0);
                     }}"
                );
                let pass = FragmentPass::new(backend, &wgsl, &[desc.format])
                    .map_err(ImageError::Fragment)?;
                let binding = pass.bind(&[&wide], &[]).map_err(ImageError::Fragment)?;
                Some(Unfold {
                    wide,
                    pass,
                    binding,
                })
            }
            Plan::Direct | Plan::Rows => None,
        };
        // Rows at a stride nothing folds are repacked in one pass. A folded copy whose buffer ends
        // early falls back to the same repack.
        let repack = match plan {
            Plan::Rows | Plan::Folded { .. } => Some(Repack::new(backend, desc)?),
            Plan::Direct => None,
        };
        Ok(ImageCopy {
            texture,
            stride,
            plan,
            unfold,
            repack,
            context,
        })
    }

    /// The texture every copy lands in.
    pub fn texture(&self) -> &Texture {
        &self.texture
    }

    /// How the copy is made, which depends only on the stride and the size.
    pub fn plan(&self) -> Plan {
        self.plan
    }

    /// Records the copy of the image that starts `offset` bytes into `buffer`.
    pub fn encode(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        buffer: &Storage,
        offset: u64,
    ) -> Result<(), ImageError> {
        let desc = self.texture.desc();
        let pixel = u64::from(desc.format.bytes_per_pixel());
        let row = u64::from(desc.width) * pixel;
        let needed = self.stride * u64::from(desc.height - 1) + row;
        let fits = offset
            .checked_add(needed)
            .is_some_and(|end| end <= buffer.size);
        if !fits || !offset.is_multiple_of(pixel) {
            return Err(ImageError::Invalid(format!(
                "an image of {needed} bytes at {offset} doesn't fit a buffer of {}, or starts inside a pixel",
                buffer.size
            )));
        }
        if buffer.context.key != self.context.key {
            return Err(ImageError::Invalid(
                "the buffer is on another device".into(),
            ));
        }
        let source = |offset, bytes_per_row: Option<u64>, rows: u32| wgpu::TexelCopyBufferInfo {
            buffer: buffer.buffer(),
            layout: wgpu::TexelCopyBufferLayout {
                offset,
                bytes_per_row: bytes_per_row.map(|bytes| bytes as u32),
                rows_per_image: Some(rows),
            },
        };
        let whole = |texture: &Texture| wgpu::Extent3d {
            width: texture.desc().width,
            height: texture.desc().height,
            depth_or_array_layers: 1,
        };
        match (self.plan, &self.unfold) {
            (Plan::Direct, _) => encoder.copy_buffer_to_texture(
                source(offset, Some(self.stride), desc.height),
                self.texture.texture.as_image_copy(),
                whole(&self.texture),
            ),
            (Plan::Folded { rows }, Some(unfold)) => {
                // The last folded row would run past the image's last pixel by its padding.
                // The buffer has to hold that too, or the rows are copied one at a time.
                let folded = self.stride * u64::from(desc.height);
                if offset + folded > buffer.size {
                    return self.encode_rows(encoder, buffer, offset);
                }
                let wide_rows = desc.height / rows;
                encoder.copy_buffer_to_texture(
                    source(offset, Some(self.stride * u64::from(rows)), wide_rows),
                    unfold.wide.texture.as_image_copy(),
                    whole(&unfold.wide),
                );
                unfold
                    .pass
                    .encode_bound(encoder, &unfold.binding, &[&self.texture])
                    .map_err(ImageError::Fragment)?;
            }
            _ => return self.encode_rows(encoder, buffer, offset),
        }
        Ok(())
    }

    /// Packs the rows into the repack's buffer in one pass, and copies that buffer to the
    /// texture in one copy: right for any stride.
    fn encode_rows(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        buffer: &Storage,
        offset: u64,
    ) -> Result<(), ImageError> {
        let Some(repack) = &self.repack else {
            return Err(ImageError::Invalid(
                "the copy was set up without its repack".into(),
            ));
        };
        let desc = self.texture.desc();
        repack.encode(&self.context, encoder, buffer, offset, self.stride)?;
        encoder.copy_buffer_to_texture(
            wgpu::TexelCopyBufferInfo {
                buffer: repack.out.wgpu(),
                layout: wgpu::TexelCopyBufferLayout {
                    offset: 0,
                    bytes_per_row: Some(repack.pitch as u32),
                    rows_per_image: Some(desc.height),
                },
            },
            self.texture.texture.as_image_copy(),
            wgpu::Extent3d {
                width: desc.width,
                height: desc.height,
                depth_or_array_layers: 1,
            },
        );
        Ok(())
    }

    /// Copies one image and submits. For a copy run by hand: in a graph, the region submits.
    pub fn run(&self, buffer: &Storage, offset: u64) -> Result<Ticket, ImageError> {
        let context = &self.context;
        let mut encoder = context
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor::default());
        self.encode(&mut encoder, buffer, offset)?;
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
        Ok(Ticket::new(state, Arc::clone(context), None))
    }
}
