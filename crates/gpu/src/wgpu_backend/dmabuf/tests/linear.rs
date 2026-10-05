use std::os::fd::{AsFd, FromRawFd, OwnedFd};
use std::sync::Arc;

use super::support::{DmaBuf, device_poll, exclusive, import_backend, pixel};
use crate::{
    AcquireStatus, DRM_FORMAT_MOD_LINEAR, DrmFourcc, ExternalFrameDescriptor, ExternalImportError,
    ExternalKeepalive, ExternalPlane, GpuBackend, GpuFormat,
};

#[test]
#[ignore = "needs a Vulkan GPU with dmabuf import and access to /dev/dma_heap"]
fn dmabuf_import_is_zero_copy() {
    let _gpu = exclusive();
    let backend = import_backend().expect("dmabuf import support");
    let (width, height, stride) = (64u32, 32u32, 256u64);
    let buf = DmaBuf::alloc((stride * u64::from(height)) as usize).expect("dma-heap alloc");
    buf.write(|bytes| {
        for y in 0..height {
            for x in 0..width {
                let at = (y as usize) * stride as usize + (x as usize) * 4;
                bytes[at..at + 4].copy_from_slice(&pixel(x, y));
            }
        }
    });

    let guard = Arc::new(());
    let keepalive: ExternalKeepalive = guard.clone();
    let plane = ExternalPlane::from_borrowed(buf.fd.as_fd(), 0, stride).unwrap();
    let handle = backend
        .import_dmabuf(
            ExternalFrameDescriptor::single_plane(width, height, DrmFourcc::XRGB8888, plane)
                .with_modifier(DRM_FORMAT_MOD_LINEAR)
                .with_label("dma-heap-frame")
                .with_keepalive(keepalive),
        )
        .expect("import");
    assert_eq!(handle.format, GpuFormat::Bgra8Unorm);
    assert_eq!(handle.acquire_status(), AcquireStatus::Ready);

    let read = backend.read_texture(&handle).expect("readback");
    for y in 0..height {
        for x in 0..width {
            let at = ((y * width + x) * 4) as usize;
            assert_eq!(&read[at..at + 4], &pixel(x, y), "pixel ({x},{y})");
        }
    }

    // The texture aliases the dmabuf: a CPU write after import is visible to the GPU.
    buf.write(|bytes| bytes[..4].copy_from_slice(&[9, 8, 7, 6]));
    let read = backend.read_texture(&handle).expect("second readback");
    assert_eq!(&read[..4], &[9, 8, 7, 6]);

    // Plane offset: skip the first two rows of the same buffer.
    let plane = ExternalPlane::from_borrowed(buf.fd.as_fd(), 2 * stride, stride).unwrap();
    let offset_handle = backend
        .import_dmabuf(ExternalFrameDescriptor::single_plane(
            width,
            height - 2,
            DrmFourcc::XRGB8888,
            plane,
        ))
        .expect("offset import");
    let read = backend
        .read_texture(&offset_handle)
        .expect("offset readback");
    assert_eq!(&read[..4], &pixel(0, 2));

    // Clones share the image; the keepalive goes once the last one (and its release) is done.
    let clone = handle.clone();
    assert_eq!(Arc::strong_count(&guard), 2);
    drop(handle);
    drop(offset_handle);
    device_poll(&backend);
    assert_eq!(
        Arc::strong_count(&guard),
        2,
        "a clone still holds the image"
    );
    drop(clone);
    device_poll(&backend);
    assert_eq!(
        Arc::strong_count(&guard),
        1,
        "keepalive released after drop"
    );
}

#[test]
#[ignore = "needs a Vulkan GPU with dmabuf import"]
fn dmabuf_import_rejects_non_dmabuf_and_short_buffers() {
    let _gpu = exclusive();
    let Some(backend) = import_backend() else {
        return;
    };
    // A 4 KiB dmabuf cannot hold 64x32 XRGB rows at stride 256.
    let small = DmaBuf::alloc(4096).expect("dma-heap alloc");
    let plane = ExternalPlane::from_borrowed(small.fd.as_fd(), 0, 256).unwrap();
    let err = backend
        .import_dmabuf(ExternalFrameDescriptor::single_plane(
            64,
            32,
            DrmFourcc::XRGB8888,
            plane,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ExternalImportError::InvalidDescriptor { .. }),
        "{err}"
    );

    // memfd is seekable but is not a dmabuf.
    // SAFETY: plain memfd creation with a static name.
    let raw = unsafe { libc::memfd_create(c"not-a-dmabuf".as_ptr(), libc::MFD_CLOEXEC) };
    assert!(raw >= 0);
    // SAFETY: fresh fd we own.
    let memfd = unsafe { OwnedFd::from_raw_fd(raw) };
    std::fs::File::from(memfd.try_clone().unwrap())
        .set_len(256 * 32)
        .unwrap();
    let err = backend
        .import_dmabuf(ExternalFrameDescriptor::single_plane(
            64,
            32,
            DrmFourcc::XRGB8888,
            ExternalPlane::new(memfd, 0, 256),
        ))
        .unwrap_err();
    assert!(
        matches!(err, ExternalImportError::ImportFailed { .. }),
        "{err}"
    );
}
