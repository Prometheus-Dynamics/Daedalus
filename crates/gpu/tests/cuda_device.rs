//! The CUDA backend on a real device: upload, the `scale_f32` kernel, download, with checked
//! values. Skips (passes, printing the reason) without libcuda or a CUDA device, so CI without an
//! NVIDIA GPU stays green.
#![allow(clippy::print_stderr)] // the skip reason

use daedalus_gpu::{CudaBackend, GpuBackend, GpuBackendKind, GpuError, GpuOptions, select_backend};

/// The backend, or `None` after printing why CUDA is unavailable here.
fn cuda_or_skip(test: &str) -> Option<CudaBackend> {
    match CudaBackend::new() {
        Ok(cuda) => Some(cuda),
        Err(e) => {
            eprintln!("skipped {test}: {e}");
            None
        }
    }
}

fn f32_bytes(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_ne_bytes()).collect()
}

fn f32_values(bytes: &[u8]) -> Vec<f32> {
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|c| f32::from_ne_bytes(*c))
        .collect()
}

#[test]
fn upload_scale_download_on_the_device() {
    let Some(cuda) = cuda_or_skip("upload_scale_download_on_the_device") else {
        return;
    };
    // 1000 values: not a multiple of the 256-thread block, so the bounds check runs.
    let input: Vec<f32> = (0..1000).map(|i| (i as f32) * 0.25 - 100.0).collect();
    let buffer = cuda.upload(&f32_bytes(&input)).expect("upload");
    let fence = cuda.scale_f32(&buffer, 3.0).expect("launch scale_f32");
    fence.wait().expect("kernel completes");
    let output = f32_values(&cuda.download(&buffer).expect("download"));
    assert_eq!(output.len(), input.len());
    for (i, (got, src)) in output.iter().zip(&input).enumerate() {
        assert_eq!(*got, src * 3.0, "element {i}");
    }
    let stats = cuda.stats();
    assert_eq!(stats.bytes_uploaded, 4000);
    assert_eq!(stats.bytes_downloaded, 4000);
}

#[test]
fn a_download_waits_for_the_kernel_queued_before_it() {
    let Some(cuda) = cuda_or_skip("a_download_waits_for_the_kernel_queued_before_it") else {
        return;
    };
    // Launch and download without waiting on the fence: the stream orders them.
    let buffer = cuda.upload(&f32_bytes(&[1.5; 64])).expect("upload");
    let _fence = cuda.scale_f32(&buffer, 4.0).expect("launch");
    let output = f32_values(&cuda.download(&buffer).expect("download"));
    assert!(output.iter().all(|v| *v == 6.0), "{output:?}");
}

#[test]
fn a_buffer_is_usable_only_through_its_own_backend() {
    let Some(cuda) = cuda_or_skip("a_buffer_is_usable_only_through_its_own_backend") else {
        return;
    };
    let other = CudaBackend::new().expect("a second backend on the same device");
    let buffer = cuda.upload(&[7u8; 16]).expect("upload");
    assert_eq!(cuda.download(&buffer).expect("its own backend").len(), 16);
    // Another backend, even on the same device, does not own the buffer.
    assert!(matches!(
        other.download(&buffer),
        Err(GpuError::InvalidInput(_))
    ));
    // A handle of no backend (a plain host handle) is refused.
    let foreign = daedalus_gpu::GpuBufferHandle::new(
        16,
        daedalus_gpu::GpuMemoryLocation::Gpu,
        daedalus_gpu::GpuUsage::DOWNLOAD,
    );
    assert!(matches!(
        cuda.download(&foreign),
        Err(GpuError::InvalidInput(_))
    ));
}

#[test]
fn select_backend_opens_cuda_when_it_is_preferred() {
    if cuda_or_skip("select_backend_opens_cuda_when_it_is_preferred").is_none() {
        return;
    }
    let ctx = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Cuda),
        ..Default::default()
    })
    .expect("a context");
    assert_eq!(ctx.backend_kind(), GpuBackendKind::Cuda);
    assert_eq!(ctx.adapter_info().backend, GpuBackendKind::Cuda);
    let cuda = ctx.cuda().expect("the CUDA backend");
    assert!(cuda.compute_capability().0 >= 1);
}
