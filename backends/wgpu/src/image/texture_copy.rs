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

/// A copy of images of one size, format and stride into one texture, set up once and run for
/// each image. Made at setup.
pub struct ImageCopy {
    texture: Texture,
    stride: u64,
    plan: Plan,
    unfold: Option<Unfold>,
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
        Ok(ImageCopy {
            texture,
            stride,
            plan,
            unfold,
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
            buffer: &buffer.buffer,
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

    /// One copy for each row: right for any stride, and slow.
    fn encode_rows(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        buffer: &Storage,
        offset: u64,
    ) -> Result<(), ImageError> {
        let desc = self.texture.desc();
        for row in 0..desc.height {
            encoder.copy_buffer_to_texture(
                wgpu::TexelCopyBufferInfo {
                    buffer: &buffer.buffer,
                    layout: wgpu::TexelCopyBufferLayout {
                        offset: offset + u64::from(row) * self.stride,
                        bytes_per_row: None,
                        rows_per_image: None,
                    },
                },
                wgpu::TexelCopyTextureInfo {
                    texture: &self.texture.texture,
                    mip_level: 0,
                    origin: wgpu::Origin3d { x: 0, y: row, z: 0 },
                    aspect: wgpu::TextureAspect::All,
                },
                wgpu::Extent3d {
                    width: desc.width,
                    height: 1,
                    depth_or_array_layers: 1,
                },
            );
        }
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
