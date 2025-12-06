use async_trait::async_trait;

use crate::{GpuBackend, GpuBufferHandle, GpuError, GpuImageHandle, GpuImageRequest, GpuRequest};

/// Optional async helpers for upload/readback. Real backends can override for zero-copy paths.
#[async_trait]
pub trait GpuAsyncBackend: GpuBackend {
    async fn upload_buffer(
        &self,
        req: &GpuRequest,
        _data: &[u8],
    ) -> Result<GpuBufferHandle, GpuError> {
        self.create_buffer(req)
    }

    async fn read_buffer(&self, _handle: &GpuBufferHandle) -> Result<Vec<u8>, GpuError> {
        self.record_download(0);
        Ok(Vec::new())
    }

    async fn create_image_async(&self, req: &GpuImageRequest) -> Result<GpuImageHandle, GpuError> {
        self.create_image(req)
    }
}
