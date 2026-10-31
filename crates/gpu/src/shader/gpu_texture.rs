//! Node-owned persistent 2D textures: fragment-pass inputs and render targets.

use std::sync::Arc;

use crate::{GpuContextHandle, GpuError};

use super::GpuStorage;
use super::GpuTicket;
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
    /// Padded rows for [`GpuTexture::copy_from_storage`] when a frame's stride is not a multiple
    /// of 256 (made on first need, reused).
    repitch: parking_lot::Mutex<Option<wgpu::Buffer>>,
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
            repitch: parking_lot::Mutex::new(None),
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

    /// Record a copy of the whole texture from `src` (rows `stride` bytes apart, from `offset`):
    /// typically a camera frame imported with [`GpuStorage::import_dmabuf`], to sample it in a
    /// fragment pass. It is `vkCmdCopyBufferToImage`; v3dv runs a whole-slice copy like this on
    /// its texture formatting unit (TFU), with no shader, when `offset` is 0.
    ///
    /// A `stride` that is not a multiple of 256 (wgpu's row alignment for buffer-to-texture
    /// copies, for example the 640-byte rows of a 640-wide GREY level) is re-pitched first: each
    /// row is copied into a padded intermediate buffer the texture keeps, then that buffer is
    /// copied in. That costs one small buffer copy per row on top. Allocating frames with
    /// 256-aligned strides keeps the direct path.
    ///
    /// `InvalidInput` when `stride` is smaller than a row, a re-pitched row or `offset` is not
    /// 4-byte aligned, `offset` is not texel aligned, or the rows run past `src`.
    pub fn copy_from_storage(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        src: &GpuStorage,
        offset: u64,
        stride: u32,
    ) -> Result<(), GpuError> {
        let (width, height) = self.size();
        let row = width as u64 * self.texel_bytes() as u64;
        let invalid = |what: String| Err(GpuError::InvalidInput(what));
        if u64::from(stride) < row {
            return invalid(format!("stride {stride} is shorter than a {row}-byte row"));
        }
        if !offset.is_multiple_of(self.texel_bytes() as u64) {
            return invalid(format!("offset {offset} is not texel aligned"));
        }
        let end = offset + u64::from(stride) * u64::from(height - 1) + row;
        if end > src.size() {
            return invalid(format!(
                "a {width}x{height} copy with stride {stride} from offset {offset} needs {end} \
                 bytes, the buffer has {}",
                src.size()
            ));
        }
        if stride.is_multiple_of(wgpu::COPY_BYTES_PER_ROW_ALIGNMENT) {
            self.copy_rows(encoder, src.buffer(), offset, stride);
            return Ok(());
        }
        let align = wgpu::COPY_BUFFER_ALIGNMENT;
        if !row.is_multiple_of(align)
            || !offset.is_multiple_of(align)
            || !u64::from(stride).is_multiple_of(align)
        {
            return invalid(format!(
                "re-pitching stride {stride} needs 4-byte aligned rows ({row} bytes), stride and \
                 offset ({offset})"
            ));
        }
        let padded = (row as u32).next_multiple_of(wgpu::COPY_BYTES_PER_ROW_ALIGNMENT);
        let needed = u64::from(padded) * u64::from(height);
        let mut repitch = self.repitch.lock();
        if repitch.as_ref().is_none_or(|b| b.size() < needed) {
            *repitch = Some(self.device.create_buffer(&wgpu::BufferDescriptor {
                label: Some("gpu-texture-repitch"),
                size: needed,
                usage: wgpu::BufferUsages::COPY_SRC | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            }));
        }
        let staging = repitch.as_ref().expect("made above");
        for y in 0..u64::from(height) {
            encoder.copy_buffer_to_buffer(
                src.buffer(),
                offset + y * u64::from(stride),
                staging,
                y * u64::from(padded),
                row,
            );
        }
        self.copy_rows(encoder, staging, 0, padded);
        Ok(())
    }

    fn copy_rows(
        &self,
        encoder: &mut wgpu::CommandEncoder,
        src: &wgpu::Buffer,
        offset: u64,
        stride: u32,
    ) {
        encoder.copy_buffer_to_texture(
            wgpu::TexelCopyBufferInfo {
                buffer: src,
                layout: wgpu::TexelCopyBufferLayout {
                    offset,
                    bytes_per_row: Some(stride),
                    rows_per_image: Some(self.texture.height()),
                },
            },
            self.texture.as_image_copy(),
            self.texture.size(),
        );
    }

    /// [`GpuTexture::copy_from_storage`] submitted on its own; poll the ticket, or encode the
    /// passes reading the texture after it on the same queue.
    pub fn upload_from_storage(
        &self,
        src: &GpuStorage,
        offset: u64,
        stride: u32,
    ) -> Result<GpuTicket, GpuError> {
        let mut encoder = self.device.create_command_encoder(&Default::default());
        self.copy_from_storage(&mut encoder, src, offset, stride)?;
        Ok(GpuTicket::submit_on(
            &self.device,
            &self.queue,
            [encoder.finish()],
        ))
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
