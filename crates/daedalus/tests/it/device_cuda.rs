//! `#[device]` transfers keyed by the CUDA residency: `f32` uploads to `test:f32@cuda` and back,
//! through the CUDA backend of the executor's handle. The values run on the device when it is
//! present; without one the test skips (passes, printing why).
#![allow(clippy::print_stderr)] // the skip reason

use daedalus::gpu::{GpuBackendKind, GpuBufferHandle, GpuContextHandle, GpuOptions};
use daedalus::transport::{AdaptRequest, Payload, Residency, TransportError, TypeKey};
use daedalus::{PluginRegistry, device};

/// An `f32` in CUDA device memory: a one-element buffer of the CUDA backend.
#[derive(Debug)]
struct CudaF32(GpuBufferHandle);

fn cuda(gpu: &GpuContextHandle) -> Result<&daedalus::gpu::CudaBackend, TransportError> {
    gpu.cuda().ok_or_else(|| {
        TransportError::Unsupported(format!(
            "needs a CUDA context, the executor has {}",
            gpu.backend_kind().as_str()
        ))
    })
}

fn download_f32(value: &CudaF32, gpu: &GpuContextHandle) -> Result<f32, TransportError> {
    let bytes = cuda(gpu)?
        .download(&value.0)
        .map_err(|e| TransportError::Unsupported(e.to_string()))?;
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| TransportError::Unsupported("CUDA f32 buffer is not 4 bytes".into()))?;
    Ok(f32::from_ne_bytes(bytes))
}

#[device(
    id = "test.cuda.f32",
    cpu = "test:f32",
    device = "test:f32@cuda",
    download = download_f32
)]
fn upload_f32(value: &f32, gpu: &GpuContextHandle) -> Result<CudaF32, TransportError> {
    let buffer = cuda(gpu)?
        .upload(&value.to_ne_bytes())
        .map_err(|e| TransportError::Unsupported(e.to_string()))?;
    Ok(CudaF32(buffer))
}

#[test]
fn device_transfers_are_keyed_by_the_cuda_residency() {
    let mut registry = PluginRegistry::new();
    register_upload_f32_device(&mut registry).expect("register device");
    // The CUDA port type resolves to its own upload, and a wgpu-keyed request does not.
    let mut request = AdaptRequest::new(TypeKey::new("test:f32@cuda"));
    request.residency = Some(Residency::Gpu);
    let path = registry
        .transport_capabilities
        .resolve_adapter_path_for_with_context(&TypeKey::new("test:f32"), &request, &[], true)
        .expect("CUDA upload path");
    assert_eq!(path.steps[0].as_str(), "test.cuda.f32.upload");
    let mut wgpu_request = AdaptRequest::new(TypeKey::new("test:f32@gpu"));
    wgpu_request.residency = Some(Residency::Gpu);
    assert!(
        registry
            .transport_capabilities
            .resolve_adapter_path_for_with_context(
                &TypeKey::new("test:f32"),
                &wgpu_request,
                &[],
                true
            )
            .is_err(),
        "no adapter produces a wgpu-keyed f32"
    );
    // Outside an executor there is no GPU context, so the transfer reports it.
    let outside = registry.runtime_transport.execute_adapter_path(
        Payload::owned("test:f32", 1.0f32),
        &path.steps,
        &request,
    );
    assert!(
        matches!(outside, Err(TransportError::Unsupported(_))),
        "{outside:?}"
    );
}

#[test]
fn the_cuda_adapters_round_trip_a_value_on_the_device() {
    let gpu = match daedalus::gpu::select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Cuda),
        ..Default::default()
    }) {
        Ok(gpu) if gpu.backend_kind() == GpuBackendKind::Cuda => gpu,
        Ok(gpu) => {
            eprintln!(
                "skipped: CUDA unavailable, selected {}",
                gpu.backend_kind().as_str()
            );
            return;
        }
        Err(e) => {
            eprintln!("skipped: {e}");
            return;
        }
    };
    let on_device = upload_f32(&2.25, &gpu).expect("upload");
    let scale = cuda(&gpu).expect("cuda");
    scale
        .scale_f32(&on_device.0, 4.0)
        .expect("scale")
        .wait()
        .expect("wait");
    assert_eq!(download_f32(&on_device, &gpu).expect("download"), 9.0);
    assert_eq!(gpu.adapter_info().backend, GpuBackendKind::Cuda);
}
