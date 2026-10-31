//! Node-owned persistent 2D textures: fragment-pass inputs and render targets.

use std::sync::Arc;

use crate::{GpuContextHandle, GpuError};

use super::gpu_storage::{GpuReadback, StagingRing};

/// A 2D texture a node keeps on the engine's device across ticks: a fragment pass samples it
/// (`textureLoad`) or renders into it. Upload with [`GpuTexture::write`]; read back with
/// [`GpuTexture::start_read`] (polled) or [`GpuTexture::read_into`] (blocking).
///
/// Formats a [`super::FragmentPass`] renders into on every backend: `R8Unorm`, `Rgba8Unorm`,
/// `R32Float`, `Rg32Float`.
pub struct GpuTexture {
    texture: wgpu::Texture,
    view: wgpu::TextureView,
    device: wgpu::Device,
    queue: wgpu::Queue,
    staging: Arc<StagingRing>,
}

impl std::fmt::Debug for GpuTexture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuTexture")
            .field("size", &self.size())
            .field("format", &self.format())
            .finish_non_exhaustive()
    }
}

impl GpuTexture {
    /// A zeroed `width` x `height` texture of `format` that can be sampled, rendered into,
    /// written and read back. `Unsupported` on a non-wgpu backend; `InvalidInput` for an empty
    /// or oversized extent, or a format without a fixed texel size (compressed, depth).
    pub fn new(
        gpu: &GpuContextHandle,
        (width, height): (u32, u32),
        format: wgpu::TextureFormat,
        label: Option<&str>,
    ) -> Result<Self, GpuError> {
        let (device, queue) = gpu.wgpu().ok_or(GpuError::Unsupported)?;
        let max = device.limits().max_texture_dimension_2d;
        if width == 0 || height == 0 || width > max || height > max {
            return Err(GpuError::InvalidInput(format!(
                "GpuTexture extent {width}x{height} must be within 1..={max}"
            )));
        }
        if texel_bytes(format).is_none() {
            return Err(GpuError::InvalidInput(format!(
                "GpuTexture format {format:?} has no fixed texel size"
            )));
        }
        let texture = device.create_texture(&wgpu::TextureDescriptor {
            label,
            size: wgpu::Extent3d {
                width,
                height,
                depth_or_array_layers: 1,
            },
            mip_level_count: 1,
            sample_count: 1,
            dimension: wgpu::TextureDimension::D2,
            format,
            usage: wgpu::TextureUsages::TEXTURE_BINDING
                | wgpu::TextureUsages::RENDER_ATTACHMENT
                | wgpu::TextureUsages::COPY_SRC
                | wgpu::TextureUsages::COPY_DST,
            view_formats: &[],
        });
        let view = texture.create_view(&Default::default());
        Ok(Self {
            texture,
            view,
            device: device.clone(),
            queue: queue.clone(),
            staging: Arc::default(),
        })
    }

    pub fn size(&self) -> (u32, u32) {
        (self.texture.width(), self.texture.height())
    }

    pub fn format(&self) -> wgpu::TextureFormat {
        self.texture.format()
    }

    /// Bytes per texel.
    pub fn texel_bytes(&self) -> usize {
        texel_bytes(self.format()).expect("checked at creation")
    }

    /// Bytes of the whole texture, tightly packed (no row padding).
    pub fn len_bytes(&self) -> usize {
        let (w, h) = self.size();
        w as usize * h as usize * self.texel_bytes()
    }

    pub fn texture(&self) -> &wgpu::Texture {
        &self.texture
    }

    /// The default view, for the node's own bind groups.
    pub fn view(&self) -> &wgpu::TextureView {
        &self.view
    }

    /// Queue an upload of the whole texture from tightly packed rows
    /// ([`GpuTexture::len_bytes`] bytes); it lands before the next submission.
    pub fn write(&self, bytes: &[u8]) -> Result<(), GpuError> {
        if bytes.len() != self.len_bytes() {
            return Err(GpuError::InvalidInput(format!(
                "GpuTexture write of {} bytes, the texture holds {}",
                bytes.len(),
                self.len_bytes()
            )));
        }
        let (width, height) = self.size();
        self.queue.write_texture(
            self.texture.as_image_copy(),
            bytes,
            wgpu::TexelCopyBufferLayout {
                offset: 0,
                bytes_per_row: Some(width * self.texel_bytes() as u32),
                rows_per_image: Some(height),
            },
            self.texture.size(),
        );
        Ok(())
    }

    /// Start copying the whole texture into a reusable staging buffer, without waiting. Work
    /// queued before (a pass rendering into it) lands first. The [`GpuReadback`] delivers
    /// tightly packed rows ([`GpuTexture::len_bytes`]).
    pub fn start_read(&self) -> Result<GpuReadback, GpuError> {
        let (width, height) = self.size();
        let unpadded = width as usize * self.texel_bytes();
        let padded = unpadded.next_multiple_of(wgpu::COPY_BYTES_PER_ROW_ALIGNMENT as usize);
        let copy = (padded * height as usize) as u64;
        let staging = self.staging.take(&self.device, copy);
        let mut encoder = self.device.create_command_encoder(&Default::default());
        encoder.copy_texture_to_buffer(
            self.texture.as_image_copy(),
            wgpu::TexelCopyBufferInfo {
                buffer: &staging,
                layout: wgpu::TexelCopyBufferLayout {
                    offset: 0,
                    bytes_per_row: Some(padded as u32),
                    rows_per_image: Some(height),
                },
            },
            self.texture.size(),
        );
        self.queue.submit([encoder.finish()]);
        Ok(GpuReadback::begin(
            &self.device,
            &self.staging,
            staging,
            unpadded * height as usize,
            copy,
            Some((padded, unpadded)),
        ))
    }

    /// Read the whole texture into `out` (tightly packed), waiting for the GPU. For setup and
    /// tests: a tick polls [`GpuTexture::start_read`].
    pub fn read_into(&self, out: &mut [u8]) -> Result<(), GpuError> {
        let mut readback = self.start_read()?;
        readback.wait();
        readback
            .try_read_into(out)
            .unwrap_or_else(|| Err(GpuError::Internal("readback did not complete".into())))
    }

    /// Staging buffers this texture has created for readbacks.
    pub fn staging_buffers_created(&self) -> usize {
        self.staging.created()
    }
}

fn texel_bytes(format: wgpu::TextureFormat) -> Option<usize> {
    let (bw, bh) = format.block_dimensions();
    if (bw, bh) != (1, 1) {
        return None;
    }
    format.block_copy_size(None).map(|n| n as usize)
}
