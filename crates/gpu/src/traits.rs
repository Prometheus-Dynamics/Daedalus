#[cfg(target_os = "linux")]
use crate::ExternalFrameDescriptor;
#[cfg(feature = "gpu-wgpu")]
use crate::shader::SubmissionTracker;
use crate::{
    ExternalImportError, ExternalImportSupport, GpuAdapterInfo, GpuBackendKind, GpuCapabilities,
    GpuError, GpuImageHandle, GpuImageRequest, GpuOptions, GpuRequest, buffer::TransferStats,
    handles::GpuBufferHandle,
};
use std::any::Any;
#[cfg(feature = "gpu-wgpu")]
use std::sync::Arc;

/// GPU backend trait; no backend-specific types exposed.
///
pub trait GpuBackend: Send + Sync {
    fn kind(&self) -> GpuBackendKind;
    fn adapter_info(&self) -> GpuAdapterInfo;
    fn capabilities(&self) -> GpuCapabilities;
    fn select_adapter(&self, opts: &GpuOptions) -> Result<GpuAdapterInfo, GpuError>;
    fn as_any(&self) -> &dyn Any;
    fn create_buffer(&self, req: &GpuRequest) -> Result<GpuBufferHandle, GpuError>;
    fn create_image(&self, req: &GpuImageRequest) -> Result<GpuImageHandle, GpuError>;
    fn upload_texture(
        &self,
        _req: &GpuImageRequest,
        _data: &[u8],
    ) -> Result<GpuImageHandle, GpuError> {
        Err(GpuError::Unsupported)
    }
    fn read_texture(&self, _handle: &GpuImageHandle) -> Result<Vec<u8>, GpuError> {
        Err(GpuError::Unsupported)
    }
    fn stats(&self) -> TransferStats {
        TransferStats::default()
    }
    fn take_stats(&self) -> TransferStats {
        self.stats()
    }
    fn record_download(&self, _bytes: u64) {}

    /// Whether [`GpuBackend::import_dmabuf`] can succeed on this backend, and why not.
    ///
    /// Must be cheap and must not panic, including on machines without a GPU.
    fn dmabuf_import_support(&self) -> ExternalImportSupport {
        ExternalImportSupport::backend_cannot_import(self.kind())
    }

    /// Import an externally owned dmabuf frame as a GPU image without copying it.
    ///
    /// Takes ownership of the plane fds and the keepalive; both are released once the returned
    /// handle (and all its clones) are dropped and the GPU no longer uses the image.
    #[cfg(target_os = "linux")]
    fn import_dmabuf(
        &self,
        _desc: ExternalFrameDescriptor,
    ) -> Result<GpuImageHandle, ExternalImportError> {
        Err(ExternalImportError::Unsupported {
            reason: self
                .dmabuf_import_support()
                .reason()
                .unwrap_or("backend does not implement dmabuf import")
                .to_string(),
        })
    }

    /// wgpu-only escape hatches used by the shader dispatch path.
    ///
    /// These are object-safe so plugins can call into the host backend implementation without
    /// relying on `Any` downcasts, which are not reliable across dynamic library boundaries.
    #[cfg(feature = "gpu-wgpu")]
    fn wgpu_device_queue(&self) -> Option<(&wgpu::Device, &wgpu::Queue)> {
        None
    }

    #[cfg(feature = "gpu-wgpu")]
    fn wgpu_submission_tracker(&self) -> Option<&SubmissionTracker> {
        None
    }

    #[cfg(feature = "gpu-wgpu")]
    fn wgpu_get_texture(&self, _handle: &GpuImageHandle) -> Option<Arc<wgpu::Texture>> {
        None
    }

    /// Track a wgpu texture as a [`GpuImageHandle`]; `Ok(None)` when the backend does not track
    /// wgpu textures, `Err(GpuError::Unsupported)` when `format` has no [`crate::GpuFormat`].
    #[cfg(feature = "gpu-wgpu")]
    fn wgpu_register_texture(
        &self,
        _texture: Arc<wgpu::Texture>,
        _format: wgpu::TextureFormat,
        _width: u32,
        _height: u32,
        _usage: wgpu::TextureUsages,
    ) -> Result<Option<GpuImageHandle>, GpuError> {
        Ok(None)
    }
}

/// Optional context trait if backends need per-thread context.
///
pub trait GpuContext: Send + Sync {
    fn backend(&self) -> GpuBackendKind;
    fn adapter_info(&self) -> GpuAdapterInfo;
    fn capabilities(&self) -> GpuCapabilities;
    fn stats(&self) -> TransferStats {
        TransferStats::default()
    }
    fn take_stats(&self) -> TransferStats {
        self.stats()
    }
    fn record_download(&self, _bytes: u64) {}
}
