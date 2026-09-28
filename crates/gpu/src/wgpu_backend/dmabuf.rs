//! dmabuf import plumbing for the wgpu backend.
//!
//! The real implementation (`dmabuf/vulkan.rs`) is compiled only with the `gpu-dmabuf` feature on
//! Linux. Otherwise device creation is the plain `request_device` path and the capability probe
//! explains why import is unavailable.

#[cfg(all(feature = "gpu-dmabuf", target_os = "linux"))]
#[path = "dmabuf/vulkan.rs"]
mod vulkan;

#[cfg(all(feature = "gpu-dmabuf", target_os = "linux"))]
pub(super) use vulkan::{import, probe_support, request_device};

#[cfg(not(all(feature = "gpu-dmabuf", target_os = "linux")))]
pub(super) async fn request_device(
    adapter: &wgpu::Adapter,
    desc: &wgpu::DeviceDescriptor<'_>,
) -> Result<(wgpu::Device, wgpu::Queue), wgpu::RequestDeviceError> {
    adapter.request_device(desc).await
}

#[cfg(not(all(feature = "gpu-dmabuf", target_os = "linux")))]
pub(super) fn probe_support(_device: &wgpu::Device) -> crate::ExternalImportSupport {
    if cfg!(target_os = "linux") {
        crate::ExternalImportSupport::unsupported(
            "daedalus-gpu was built without the `gpu-dmabuf` feature",
        )
    } else {
        crate::ExternalImportSupport::unsupported("dmabuf import requires Linux")
    }
}

/// Only reachable if `probe_support` said `Supported`, which it never does in this build.
#[cfg(all(target_os = "linux", not(feature = "gpu-dmabuf")))]
pub(super) fn import(
    _backend: &super::WgpuBackend,
    _desc: crate::ExternalFrameDescriptor,
) -> Result<crate::GpuImageHandle, crate::ExternalImportError> {
    Err(crate::ExternalImportError::Unsupported {
        reason: "daedalus-gpu was built without the `gpu-dmabuf` feature".into(),
    })
}

#[cfg(test)]
#[path = "dmabuf/tests.rs"]
mod tests;
