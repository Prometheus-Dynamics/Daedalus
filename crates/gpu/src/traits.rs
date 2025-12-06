use crate::{
    GpuAdapterInfo, GpuBackendKind, GpuCapabilities, GpuError, GpuImageHandle, GpuImageRequest,
    GpuOptions, GpuRequest, buffer::TransferStats, handles::GpuBufferHandle,
};
use std::any::Any;
#[cfg(feature = "gpu-wgpu")]
use std::sync::Arc;

/// GPU backend trait; no backend-specific types exposed.
///
/// ```no_run
/// use daedalus_gpu::{GpuBackend, GpuBackendKind};
/// fn uses_backend(backend: &dyn GpuBackend) -> GpuBackendKind {
///     backend.kind()
/// }
/// let _ = uses_backend;
/// ```
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

    /// wgpu-only escape hatches used by the shader dispatch path.
    ///
    /// These are object-safe so plugins can call into the host backend implementation without
    /// relying on `Any` downcasts, which are not reliable across dynamic library boundaries.
    #[cfg(feature = "gpu-wgpu")]
    fn wgpu_device_queue(&self) -> Option<(&wgpu::Device, &wgpu::Queue)> {
        None
    }

    #[cfg(feature = "gpu-wgpu")]
    fn wgpu_get_texture(&self, _handle: &GpuImageHandle) -> Option<Arc<wgpu::Texture>> {
        None
    }

    #[cfg(feature = "gpu-wgpu")]
    fn wgpu_register_texture(
        &self,
        _texture: Arc<wgpu::Texture>,
        _format: wgpu::TextureFormat,
        _width: u32,
        _height: u32,
        _usage: wgpu::TextureUsages,
    ) -> Option<GpuImageHandle> {
        None
    }
}

/// Optional context trait if backends need per-thread context.
///
/// ```no_run
/// use daedalus_gpu::{GpuContext, GpuBackendKind};
/// fn kind(ctx: &dyn GpuContext) -> GpuBackendKind {
///     ctx.backend()
/// }
/// let _ = kind;
/// ```
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
