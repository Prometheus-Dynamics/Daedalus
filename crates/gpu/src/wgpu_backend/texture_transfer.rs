//! Texture readback helpers shared by the sync and async `WgpuBackend` paths.

use std::sync::Arc;

use super::WgpuBackend;
use crate::shader::staging_size;
use crate::{GpuError, GpuImageHandle, format_bytes_per_pixel};

impl WgpuBackend {
    pub(super) fn texture_for_readback(
        &self,
        handle: &GpuImageHandle,
    ) -> Result<Arc<wgpu::Texture>, GpuError> {
        self.resources
            .textures
            .lock()
            .get(&handle.id)
            .cloned()
            .ok_or(GpuError::Unsupported)
    }

    /// Tight bytes per row and the padded row pitch the GPU reads and writes for `handle`.
    pub(super) fn texture_readback_layout(
        &self,
        handle: &GpuImageHandle,
    ) -> Result<(usize, usize), GpuError> {
        let bpp = format_bytes_per_pixel(handle.format).ok_or(GpuError::Unsupported)? as usize;
        let bytes_per_row = handle.width as usize * bpp;
        let align = self.caps.bytes_per_row_alignment.max(1) as usize;
        Ok((bytes_per_row, bytes_per_row.div_ceil(align) * align))
    }

    /// A pooled MAP_READ staging buffer holding at least `size` bytes, or a new one.
    pub(super) fn take_staging_buffer(&self, size: u64, label: &str) -> wgpu::Buffer {
        let alloc = staging_size(size);
        let pooled = self.staging_pool.lock().take(alloc);
        if let Some(buffer) = pooled {
            return buffer;
        }
        self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some(label),
            size: alloc,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        })
    }

    pub(super) fn return_staging_buffer(&self, buffer: wgpu::Buffer) {
        self.staging_pool.lock().put(buffer);
    }

    /// Copy the texture into `staging` with padded rows and submit the copy.
    pub(super) fn submit_texture_readback(
        &self,
        texture: &wgpu::Texture,
        staging: &wgpu::Buffer,
        handle: &GpuImageHandle,
        padded_bpr: usize,
    ) {
        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("tex-readback-encoder"),
            });
        encoder.copy_texture_to_buffer(
            wgpu::TexelCopyTextureInfo {
                texture,
                mip_level: 0,
                origin: wgpu::Origin3d::ZERO,
                aspect: wgpu::TextureAspect::All,
            },
            wgpu::TexelCopyBufferInfo {
                buffer: staging,
                layout: wgpu::TexelCopyBufferLayout {
                    offset: 0,
                    bytes_per_row: Some(padded_bpr as u32),
                    rows_per_image: Some(handle.height),
                },
            },
            wgpu::Extent3d {
                width: handle.width,
                height: handle.height,
                depth_or_array_layers: 1,
            },
        );
        self.queue.submit(Some(encoder.finish()));
    }
}

/// Copy `height` rows of `bytes_per_row` bytes out of `src`, whose rows start `padded_bpr` apart.
///
/// Rows are copied straight from the mapped staging slice into the result, so the mapped data is
/// read once.
pub(super) fn unpad_rows(
    src: &[u8],
    bytes_per_row: usize,
    padded_bpr: usize,
    height: usize,
) -> Vec<u8> {
    if bytes_per_row == padded_bpr {
        return src[..bytes_per_row * height].to_vec();
    }
    let mut out = Vec::with_capacity(bytes_per_row * height);
    for row in 0..height {
        let start = row * padded_bpr;
        out.extend_from_slice(&src[start..start + bytes_per_row]);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::unpad_rows;

    #[test]
    fn unpad_rows_drops_row_padding() {
        // Two rows of 3 bytes padded to a stride of 5.
        let src = [1, 2, 3, 0, 0, 4, 5, 6, 0, 0];
        assert_eq!(unpad_rows(&src, 3, 5, 2), vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn unpad_rows_without_padding_copies_prefix() {
        let src = [9, 8, 7, 6];
        assert_eq!(unpad_rows(&src, 2, 2, 2), vec![9, 8, 7, 6]);
    }
}
