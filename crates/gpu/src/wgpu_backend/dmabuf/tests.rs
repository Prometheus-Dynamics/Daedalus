use crate::{GpuBackend, WgpuBackend};

/// Runs on any machine: no GPU, a non-Vulkan adapter, or a build without `gpu-dmabuf` must all
/// produce an answer (with a reason when unsupported) rather than a panic.
#[test]
fn dmabuf_support_query_never_panics() {
    #[cfg(all(feature = "gpu-dmabuf", target_os = "linux"))]
    let _gpu = support::exclusive();
    let Ok(backend) = WgpuBackend::new() else {
        return;
    };
    let support = backend.dmabuf_import_support();
    if !cfg!(all(feature = "gpu-dmabuf", target_os = "linux")) {
        assert!(!support.is_supported());
    }
    if let Some(reason) = support.reason() {
        assert!(!reason.is_empty());
    }
}

// Real zero-copy imports (`#[ignore]`d). They need a Vulkan GPU with
// `VK_EXT_external_memory_dma_buf` + `VK_EXT_image_drm_format_modifier` and a readable
// `/dev/dma_heap/*` (user in the `video` group or equivalent):
//
//     # Raspberry Pi 5 / CM5 (v3dv) or any Linux desktop GPU:
//     CARGO_BUILD_JOBS=4 cargo test -p daedalus-gpu --features gpu-dmabuf -- --ignored dmabuf
//     # Pick another heap (default /dev/dma_heap/system):
//     DAEDALUS_DMA_HEAP=/dev/dma_heap/linux,cma cargo test -p daedalus-gpu --features gpu-dmabuf -- --ignored dmabuf

#[cfg(all(feature = "gpu-dmabuf", target_os = "linux"))]
#[path = "tests/support.rs"]
mod support;

/// Single-plane `LINEAR` imports from dma-heap buffers: zero copy, offsets, rejection.
#[cfg(all(feature = "gpu-dmabuf", target_os = "linux"))]
#[path = "tests/linear.rs"]
mod linear;

/// Acquire fences in every wait mode, timeouts, and the watcher's latency.
#[cfg(all(feature = "gpu-dmabuf", target_os = "linux"))]
#[path = "tests/fences.rs"]
mod fences;

/// NV12 in one dmabuf and disjoint.
#[cfg(all(feature = "gpu-dmabuf", target_os = "linux"))]
#[path = "tests/nv12.rs"]
mod nv12;

/// Tiled and compressed (multi-memory-plane) modifiers exported by a Vulkan producer.
#[cfg(all(feature = "gpu-dmabuf", target_os = "linux"))]
#[path = "tests/modifiers.rs"]
mod modifiers;
