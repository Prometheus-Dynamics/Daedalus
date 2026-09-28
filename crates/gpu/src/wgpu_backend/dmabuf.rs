//! dmabuf import plumbing for the wgpu backend.
//!
//! The real implementation (`dmabuf/vulkan.rs` + `dmabuf/image.rs`) is compiled only with the
//! `gpu-dmabuf` feature on Linux. Otherwise device creation is the plain `request_device` path and
//! the capability probe explains why import is unavailable.

#[cfg(all(feature = "gpu-dmabuf", target_os = "linux"))]
#[path = "dmabuf/image.rs"]
mod image;
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

/// Views for sampling each plane of `texture`: for a multi-planar format such as an imported NV12
/// frame one view per plane (Y as `R8Unorm`, interleaved UV as `Rg8Unorm`), otherwise one full view.
pub fn texture_plane_views(texture: &wgpu::Texture) -> Vec<wgpu::TextureView> {
    let format = texture.format();
    let Some(planes) = format.planes() else {
        return vec![texture.create_view(&wgpu::TextureViewDescriptor::default())];
    };
    (0..planes)
        .filter_map(wgpu::TextureAspect::from_plane)
        .map(|aspect| {
            texture.create_view(&wgpu::TextureViewDescriptor {
                label: Some("plane-view"),
                format: format.aspect_specific_format(aspect),
                aspect,
                ..Default::default()
            })
        })
        .collect()
}

#[cfg(test)]
#[path = "dmabuf/tests.rs"]
mod tests;
