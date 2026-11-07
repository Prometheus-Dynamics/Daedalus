//! Heap allocations of a steady dmabuf texture import, counted on the importing thread.
//!
//! The import's own helpers allocate nothing (see `plane_helpers_do_not_allocate`). What remains
//! is per-texture bookkeeping in wgpu-core and wgpu-hal, the drop callback box and the texture's
//! `Arc`; this test pins the measured count so a regression shows.

use std::os::fd::AsFd;
use std::sync::Arc;

use super::support::{DmaBuf, exclusive, linear_import_backend};
use crate::alloc_count::allocations_in;
use crate::{
    DRM_FORMAT_MOD_LINEAR, DrmFourcc, ExternalFrameDescriptor, ExternalKeepalive, ExternalPlane,
    GpuBackend,
};

#[test]
#[allow(clippy::print_stderr)] // the measurement
#[ignore = "needs a Vulkan GPU with dmabuf import and access to /dev/dma_heap"]
fn steady_import_allocations_are_bounded() {
    let _gpu = exclusive();
    let Some(backend) = linear_import_backend() else {
        return;
    };
    let (width, height, stride) = (64u32, 32u32, 256u64);
    let buf = DmaBuf::alloc((stride * u64::from(height)) as usize).expect("dma-heap alloc");
    let keepalive: ExternalKeepalive = Arc::new(());
    let descriptor = |keepalive: &ExternalKeepalive| {
        let plane = ExternalPlane::from_borrowed(buf.fd.as_fd(), 0, stride).unwrap();
        ExternalFrameDescriptor::single_plane(width, height, DrmFourcc::XRGB8888, plane)
            .with_modifier(DRM_FORMAT_MOD_LINEAR)
            .with_keepalive(keepalive.clone())
    };
    // The first import fills the query caches; the steady state is the imports after it.
    for _ in 0..2 {
        drop(
            backend
                .import_dmabuf(descriptor(&keepalive))
                .expect("warm import"),
        );
    }
    let mut counts = Vec::new();
    for _ in 0..5 {
        let desc = descriptor(&keepalive);
        let (handle, allocs) = allocations_in(|| backend.import_dmabuf(desc));
        counts.push(allocs);
        drop(handle.expect("steady import"));
    }
    eprintln!("heap allocations per steady dmabuf import: {counts:?}");
    let steady = *counts.iter().max().unwrap();
    // Measured on the host GPU: see the commit message. A rise here means a new per-import
    // allocation on the path; the bound is that measured count, not zero (see the module docs).
    assert!(steady <= STEADY_IMPORT_ALLOCATIONS, "{counts:?}");
}

/// The steady import's heap allocations, measured on the host GPU (LINEAR XRGB8888): 50 before the
/// per-import lists became inline, 44 after. The rest is not itemized: per-texture bookkeeping in
/// wgpu-core and wgpu-hal, the drop callback box, and the texture's `Arc`.
const STEADY_IMPORT_ALLOCATIONS: u64 = 44;
