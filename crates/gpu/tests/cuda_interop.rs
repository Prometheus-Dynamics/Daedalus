//! CUDA and Vulkan on one NVIDIA GPU: a device-local buffer allocated on the wgpu device with
//! exportable memory is imported into CUDA and scaled there in place, and Vulkan reads the result
//! from the same memory. The handoff copies nothing (the backends' transfer counters do not move),
//! so the round trip shows the two APIs addressing one allocation. Skips (passes, printing why)
//! without libcuda, a CUDA device, a wgpu device on NVIDIA, or the external-memory extension.
#![cfg(all(feature = "gpu-cuda", feature = "gpu-vulkan-raw", target_os = "linux"))]
#![allow(clippy::print_stderr)] // the skip reasons and the driver's answers

use daedalus_gpu::interop::{ShareKind, SharedBuffer};
use daedalus_gpu::{CudaBackend, GpuBackend, GpuContextHandle, select_wgpu_on_vendor};

/// PCI vendor id of NVIDIA.
const NVIDIA: u32 = 0x10DE;
const COUNT: usize = 1000;

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

/// The CUDA backend and a wgpu context on the same NVIDIA GPU, or `None` after saying why.
fn devices(test: &str) -> Option<(CudaBackend, GpuContextHandle)> {
    let cuda = match CudaBackend::new() {
        Ok(cuda) => cuda,
        Err(e) => {
            eprintln!("skipped {test}: {e}");
            return None;
        }
    };
    let ctx = match select_wgpu_on_vendor(NVIDIA) {
        Ok(ctx) => ctx,
        Err(e) => {
            eprintln!("skipped {test}: no wgpu device on NVIDIA: {e}");
            return None;
        }
    };
    Some((cuda, ctx))
}

#[test]
fn a_vulkan_buffer_is_scaled_by_cuda_and_read_back_with_no_copy() {
    let test = "a_vulkan_buffer_is_scaled_by_cuda_and_read_back_with_no_copy";
    let Some((cuda, ctx)) = devices(test) else {
        return;
    };
    let bytes = (COUNT * 4) as u64;
    let shared = match SharedBuffer::create(&ctx, bytes) {
        Ok(shared) => shared,
        Err(e) => {
            eprintln!("skipped {test}: shared buffer: {e}");
            return;
        }
    };
    // The memory is shared only with the CUDA device of the same GPU.
    if cuda.device_uuid().expect("CUDA device UUID") != shared.device_uuid() {
        eprintln!(
            "skipped {test}: the wgpu device {} is not the CUDA device",
            ctx.adapter_info().name
        );
        return;
    }
    let before_cuda = cuda.stats();
    let before_wgpu = ctx.stats();

    // Round 1: Vulkan writes, CUDA scales in place, Vulkan reads.
    let input: Vec<f32> = (0..COUNT).map(|i| i as f32 * 0.5 - 100.0).collect();
    shared.write(&f32_bytes(&input)).expect("write");
    let import = cuda
        .import_opaque_fd(shared.export_fd().expect("export"), bytes)
        .expect("CUDA imports the Vulkan memory");
    import
        .scale_f32(3.0)
        .expect("launch on the import")
        .wait()
        .expect("kernel completes");
    drop(import);
    let out = f32_values(&shared.read().expect("read"));
    for (i, (got, src)) in out.iter().zip(&input).enumerate() {
        assert_eq!(*got, src * 3.0, "round 1, element {i}");
    }

    // Round 2 on the same memory: Vulkan writes over CUDA's result, CUDA scales again.
    let again: Vec<f32> = (0..COUNT).map(|i| 1.0 + i as f32).collect();
    shared.write(&f32_bytes(&again)).expect("write");
    let import = cuda
        .import_opaque_fd(shared.export_fd().expect("export"), bytes)
        .expect("second import");
    import
        .scale_f32(0.25)
        .expect("launch")
        .wait()
        .expect("wait");
    drop(import);
    let out = f32_values(&shared.read().expect("read"));
    for (i, (got, src)) in out.iter().zip(&again).enumerate() {
        assert_eq!(*got, src * 0.25, "round 2, element {i}");
    }

    // The handoffs moved nothing through either backend's transfer counters.
    assert_eq!(cuda.stats(), before_cuda, "CUDA transfer counters moved");
    assert_eq!(ctx.stats(), before_wgpu, "wgpu transfer counters moved");
    eprintln!(
        "{test}: {COUNT} f32 on {}, two round trips, 0 copies at the CUDA handoff",
        cuda.adapter_info().name
    );
}

#[test]
fn a_dmabuf_fd_is_probed_against_the_cuda_import() {
    let test = "a_dmabuf_fd_is_probed_against_the_cuda_import";
    let Some((cuda, _ctx)) = devices(test) else {
        return;
    };
    let len = 4096usize;
    let heap = match lemnos_linux::dma::DmaHeap::open_path("/dev/dma_heap/system") {
        Ok(heap) => heap,
        Err(e) => {
            eprintln!("skipped {test}: no dma-heap: {e}");
            return;
        }
    };
    let fd = match heap.allocate(len) {
        Ok(buf) => buf.into_fd(),
        Err(e) => {
            eprintln!("skipped {test}: dma-heap allocation: {e}");
            return;
        }
    };
    // The driver either accepts a dma-buf as an opaque fd or refuses it with its own error.
    match cuda.import_opaque_fd(fd, len as u64) {
        Ok(import) => eprintln!("{test}: imported: {import:?}"),
        Err(e) => eprintln!("{test}: refused: {e}"),
    }
}

/// A dma-buf exported by Vulkan from the NVIDIA device, imported into CUDA: accepted (then checked
/// with a kernel and a read) or refused with the driver's error. Prints which.
#[test]
fn a_vulkan_dmabuf_is_probed_against_the_cuda_import() {
    let test = "a_vulkan_dmabuf_is_probed_against_the_cuda_import";
    let Some((cuda, ctx)) = devices(test) else {
        return;
    };
    let bytes = (COUNT * 4) as u64;
    let shared = match SharedBuffer::create_for(&ctx, bytes, ShareKind::DmaBuf) {
        Ok(shared) => shared,
        Err(e) => {
            eprintln!("{test}: Vulkan refused a dma-buf allocation: {e}");
            return;
        }
    };
    let fd = match shared.export_fd() {
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("{test}: Vulkan refused the dma-buf export: {e}");
            return;
        }
    };
    match cuda.import_opaque_fd(fd, bytes) {
        Ok(import) => {
            let input: Vec<f32> = (0..COUNT).map(|i| i as f32).collect();
            shared.write(&f32_bytes(&input)).expect("write");
            import.scale_f32(2.0).expect("launch").wait().expect("wait");
            drop(import);
            let out = f32_values(&shared.read().expect("read"));
            assert!(out.iter().zip(&input).all(|(o, i)| *o == i * 2.0));
            eprintln!("{test}: CUDA imported the Vulkan dma-buf; 0 copies at the handoff");
        }
        Err(e) => eprintln!("{test}: CUDA refused the Vulkan dma-buf: {e}"),
    }
}
