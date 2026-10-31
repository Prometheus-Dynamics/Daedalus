//! `#[device]` with a second `&GpuContextHandle` argument registers a context-taking transport:
//! outside an executor there is no GPU context, so the upload reports it.

use daedalus::gpu::GpuContextHandle;
use daedalus::transport::{AdaptRequest, Payload, TransportError, TypeKey};
use daedalus::{PluginRegistry, device};

#[derive(Debug)]
struct GpuU8(u8);

fn download_u8(value: &GpuU8, _gpu: &GpuContextHandle) -> Result<u8, TransportError> {
    Ok(value.0)
}

#[device(
    id = "test.device.u8",
    cpu = "test:u8",
    device = "test:u8@gpu",
    download = download_u8
)]
fn upload_u8(value: &u8, _gpu: &GpuContextHandle) -> Result<GpuU8, TransportError> {
    Ok(GpuU8(*value))
}

#[test]
fn device_macro_with_gpu_context_registers_and_needs_an_executor() {
    let mut registry = PluginRegistry::new();
    register_upload_u8_device(&mut registry).expect("register device");
    let mut request = AdaptRequest::new(TypeKey::new("test:u8@gpu"));
    request.residency = Some(daedalus::transport::Residency::Gpu);
    let path = registry
        .transport_capabilities
        .resolve_adapter_path_for_with_context(&TypeKey::new("test:u8"), &request, &[], true)
        .expect("upload path");
    assert_eq!(path.steps[0].as_str(), "test.device.u8.upload");
    let outside = registry.runtime_transport.execute_adapter_path(
        Payload::owned("test:u8", 7u8),
        &path.steps,
        &request,
    );
    assert!(
        matches!(outside, Err(TransportError::Unsupported(_))),
        "{outside:?}"
    );
}
