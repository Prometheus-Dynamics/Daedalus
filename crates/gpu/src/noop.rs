use std::sync::Mutex;

use crate::handles::{GpuBufferHandle, GpuImageHandle};
use crate::traits::GpuBackend;
use crate::{
    GpuAdapterInfo, GpuBackendKind, GpuCapabilities, GpuError, GpuFormat, GpuFormatFeatures,
    GpuImageRequest, GpuMemoryLocation, GpuOptions, GpuRequest, buffer::TransferStats,
};

/// Noop backend: deterministic, does nothing.
#[derive(Default)]
pub struct NoopBackend {
    stats: Mutex<TransferStats>,
}

impl GpuBackend for NoopBackend {
    fn kind(&self) -> GpuBackendKind {
        GpuBackendKind::Noop
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn adapter_info(&self) -> GpuAdapterInfo {
        GpuAdapterInfo {
            name: "noop".into(),
            backend: GpuBackendKind::Noop,
            device_id: None,
            vendor_id: None,
        }
    }

    fn capabilities(&self) -> GpuCapabilities {
        GpuCapabilities {
            supported_formats: vec![GpuFormat::R8Unorm, GpuFormat::Rgba8Unorm],
            format_features: vec![
                GpuFormatFeatures {
                    format: GpuFormat::R8Unorm,
                    sampleable: true,
                    renderable: true,
                    storage: false,
                    max_samples: 1,
                },
                GpuFormatFeatures {
                    format: GpuFormat::Rgba8Unorm,
                    sampleable: true,
                    renderable: true,
                    storage: false,
                    max_samples: 1,
                },
            ],
            format_blocks: vec![
                crate::GpuBlockInfo {
                    format: GpuFormat::R8Unorm,
                    block_width: 1,
                    block_height: 1,
                    bytes_per_block: 1,
                },
                crate::GpuBlockInfo {
                    format: GpuFormat::Rgba8Unorm,
                    block_width: 1,
                    block_height: 1,
                    bytes_per_block: 4,
                },
            ],
            max_buffer_size: 0,
            max_texture_dimension: 0,
            max_texture_samples: 1,
            staging_alignment: 1,
            max_inflight_copies: 1,
            queue_count: 1,
            min_buffer_copy_offset_alignment: 1,
            bytes_per_row_alignment: 1,
            rows_per_image_alignment: 1,
            has_transfer_queue: false,
        }
    }

    fn select_adapter(&self, _opts: &GpuOptions) -> Result<GpuAdapterInfo, GpuError> {
        Ok(self.adapter_info())
    }

    fn create_buffer(&self, req: &GpuRequest) -> Result<GpuBufferHandle, GpuError> {
        if req.usage.is_empty() {
            return Err(GpuError::Unsupported);
        }
        let mut stats = self
            .stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        stats.record_upload(req.size_bytes);
        Ok(GpuBufferHandle::new(
            req.size_bytes,
            GpuMemoryLocation::Cpu,
            req.usage,
        ))
    }

    fn create_image(&self, req: &GpuImageRequest) -> Result<GpuImageHandle, GpuError> {
        if req.usage.is_empty() {
            return Err(GpuError::Unsupported);
        }
        let mut stats = self
            .stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let bpp = crate::format_bytes_per_pixel(req.format).unwrap_or(4) as u64;
        let bytes = (req.width as u64) * (req.height as u64) * bpp;
        stats.record_upload(bytes);
        Ok(GpuImageHandle::new(
            req.format,
            req.width,
            req.height,
            GpuMemoryLocation::Cpu,
            req.usage,
        ))
    }

    fn stats(&self) -> TransferStats {
        *self
            .stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn take_stats(&self) -> TransferStats {
        self.stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }

    fn record_download(&self, bytes: u64) {
        let mut stats = self
            .stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        stats.record_download(bytes);
    }

    fn upload_texture(
        &self,
        req: &GpuImageRequest,
        data: &[u8],
    ) -> Result<GpuImageHandle, GpuError> {
        let handle = self.create_image(req)?;
        let mut stats = self
            .stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        stats.record_upload(data.len() as u64);
        Ok(handle)
    }

    fn read_texture(&self, handle: &GpuImageHandle) -> Result<Vec<u8>, GpuError> {
        let bpp = crate::format_bytes_per_pixel(handle.format).unwrap_or(4) as usize;
        let bytes = (handle.width as usize) * (handle.height as usize) * bpp;
        self.record_download(bytes as u64);
        Ok(vec![0; bytes])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GpuUsage;

    #[test]
    fn noop_creates_handles() {
        let backend = NoopBackend::default();
        let buf = backend
            .create_buffer(&GpuRequest {
                usage: GpuUsage::UPLOAD,
                format: None,
                size_bytes: 128,
            })
            .unwrap();
        assert_eq!(buf.size_bytes, 128);
        let img = backend
            .create_image(&GpuImageRequest {
                format: GpuFormat::Rgba8Unorm,
                width: 1,
                height: 1,
                samples: 1,
                usage: GpuUsage::RENDER_TARGET,
            })
            .unwrap();
        assert_eq!(img.format, GpuFormat::Rgba8Unorm);
    }
}
