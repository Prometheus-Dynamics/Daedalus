use std::sync::Mutex;

use crate::handles::{GpuBufferHandle, GpuImageHandle};
use crate::traits::GpuBackend;
use crate::{
    GpuAdapterInfo, GpuBackendKind, GpuCapabilities, GpuError, GpuFormat, GpuFormatFeatures,
    GpuImageRequest, GpuMemoryLocation, GpuOptions, GpuRequest, GpuUsage,
    buffer::{BufferPool, SimpleBufferPool, TransferStats},
};

/// Deterministic mock backend for tests/CI.
pub struct MockBackend {
    adapter: GpuAdapterInfo,
    caps: GpuCapabilities,
    pool: SimpleBufferPool,
    stats: Mutex<TransferStats>,
}

impl Default for MockBackend {
    fn default() -> Self {
        Self {
            adapter: GpuAdapterInfo {
                name: "mock-adapter".into(),
                backend: GpuBackendKind::Mock,
                device_id: Some("mock-device".into()),
                vendor_id: Some("mock-vendor".into()),
            },
            caps: GpuCapabilities {
                supported_formats: vec![
                    GpuFormat::R8Unorm,
                    GpuFormat::Rgba8Unorm,
                    GpuFormat::Rgba16Float,
                ],
                format_features: vec![
                    GpuFormatFeatures {
                        format: GpuFormat::R8Unorm,
                        sampleable: true,
                        renderable: true,
                        storage: true,
                        max_samples: 4,
                    },
                    GpuFormatFeatures {
                        format: GpuFormat::Rgba8Unorm,
                        sampleable: true,
                        renderable: true,
                        storage: true,
                        max_samples: 4,
                    },
                    GpuFormatFeatures {
                        format: GpuFormat::Rgba16Float,
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
                    crate::GpuBlockInfo {
                        format: GpuFormat::Rgba16Float,
                        block_width: 1,
                        block_height: 1,
                        bytes_per_block: 8,
                    },
                ],
                max_buffer_size: 64 << 20,
                max_texture_dimension: 4096,
                max_texture_samples: 4,
                staging_alignment: 256,
                max_inflight_copies: 4,
                queue_count: 1,
                min_buffer_copy_offset_alignment: 256,
                bytes_per_row_alignment: 256,
                rows_per_image_alignment: 1,
                has_transfer_queue: false,
            },
            pool: SimpleBufferPool::new(),
            stats: Mutex::new(TransferStats::default()),
        }
    }
}

impl GpuBackend for MockBackend {
    fn kind(&self) -> GpuBackendKind {
        GpuBackendKind::Mock
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn adapter_info(&self) -> GpuAdapterInfo {
        self.adapter.clone()
    }

    fn capabilities(&self) -> GpuCapabilities {
        self.caps.clone()
    }

    fn select_adapter(&self, opts: &GpuOptions) -> Result<GpuAdapterInfo, GpuError> {
        if let Some(pref) = opts.preferred_backend
            && pref != GpuBackendKind::Mock
        {
            return Err(GpuError::AdapterUnavailable);
        }
        if let Some(label) = &opts.adapter_label
            && label != &self.adapter.name
        {
            return Err(GpuError::AdapterUnavailable);
        }
        Ok(self.adapter.clone())
    }

    fn create_buffer(&self, req: &GpuRequest) -> Result<GpuBufferHandle, GpuError> {
        if req.size_bytes > self.caps.max_buffer_size {
            return Err(GpuError::AllocationFailed);
        }
        if req.usage.is_empty() {
            return Err(GpuError::Unsupported);
        }
        if self.caps.staging_alignment > 0
            && !req.size_bytes.is_multiple_of(self.caps.staging_alignment)
        {
            return Err(GpuError::AllocationFailed);
        }
        let handle = self
            .pool
            .alloc(req.size_bytes, req.usage, GpuMemoryLocation::Gpu)?;
        let mut stats = self.stats.lock().expect("mock stats lock");
        stats.record_upload(req.size_bytes);
        Ok(handle)
    }

    fn create_image(&self, req: &GpuImageRequest) -> Result<GpuImageHandle, GpuError> {
        if req.width > self.caps.max_texture_dimension
            || req.height > self.caps.max_texture_dimension
        {
            return Err(GpuError::AllocationFailed);
        }
        let features = self
            .caps
            .format_features
            .iter()
            .find(|f| f.format == req.format)
            .ok_or(GpuError::Unsupported)?;
        let bpp = crate::format_bytes_per_pixel(req.format).ok_or(GpuError::Unsupported)?;
        let bytes_per_row = (req.width * bpp) as u32;
        if !bytes_per_row.is_multiple_of(self.caps.bytes_per_row_alignment) {
            return Err(GpuError::AllocationFailed);
        }
        if req.usage.contains(GpuUsage::RENDER_TARGET) && !features.renderable {
            return Err(GpuError::Unsupported);
        }
        if req.usage.contains(GpuUsage::STORAGE) && !features.storage {
            return Err(GpuError::Unsupported);
        }
        if features.max_samples < req.samples {
            return Err(GpuError::Unsupported);
        }
        if req.usage.is_empty() {
            return Err(GpuError::Unsupported);
        }
        let mut stats = self.stats.lock().expect("mock stats lock");
        let bytes = (req.width as u64) * (req.height as u64) * 4;
        stats.record_upload(bytes);
        Ok(GpuImageHandle::new(
            req.format,
            req.width,
            req.height,
            GpuMemoryLocation::Gpu,
            req.usage,
        ))
    }

    fn stats(&self) -> TransferStats {
        *self.stats.lock().expect("mock stats lock")
    }

    fn take_stats(&self) -> TransferStats {
        self.stats.lock().expect("mock stats lock").take()
    }

    fn record_download(&self, bytes: u64) {
        let mut stats = self.stats.lock().expect("mock stats lock");
        stats.record_download(bytes);
    }

    fn upload_texture(
        &self,
        req: &GpuImageRequest,
        data: &[u8],
    ) -> Result<GpuImageHandle, GpuError> {
        let handle = self.create_image(req)?;
        let mut stats = self.stats.lock().expect("mock stats lock");
        stats.record_upload(data.len() as u64);
        Ok(handle)
    }

    fn read_texture(&self, handle: &GpuImageHandle) -> Result<Vec<u8>, GpuError> {
        let bytes = (handle.width as usize) * (handle.height as usize) * 4;
        self.record_download(bytes as u64);
        Ok(vec![0; bytes])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_selects_adapter() {
        let backend = MockBackend::default();
        let info = backend.select_adapter(&GpuOptions::default()).unwrap();
        assert_eq!(info.backend, GpuBackendKind::Mock);
    }

    #[test]
    fn rejects_oversize_buffer() {
        let backend = MockBackend::default();
        let req = GpuRequest {
            usage: GpuUsage::UPLOAD,
            format: None,
            size_bytes: backend.caps.max_buffer_size + 1,
        };
        assert!(matches!(
            backend.create_buffer(&req),
            Err(GpuError::AllocationFailed)
        ));
    }

    #[test]
    fn rejects_unaligned_row_pitch() {
        let backend = MockBackend::default();
        // width=1 -> bytes_per_row=4, alignment=256 => fail
        let req = GpuImageRequest {
            format: GpuFormat::Rgba8Unorm,
            width: 1,
            height: 1,
            samples: 1,
            usage: GpuUsage::RENDER_TARGET,
        };
        assert!(matches!(
            backend.create_image(&req),
            Err(GpuError::AllocationFailed)
        ));
    }
}
