use crate::{GpuBackend, WgpuBackend};

/// Runs on any machine: no GPU, a non-Vulkan adapter, or a build without `gpu-dmabuf` must all
/// produce an answer (with a reason when unsupported) rather than a panic.
#[test]
fn dmabuf_support_query_never_panics() {
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

/// Real zero-copy import of a dma-heap buffer. Needs a Vulkan GPU with
/// `VK_EXT_external_memory_dma_buf` + `VK_EXT_image_drm_format_modifier` and a readable
/// `/dev/dma_heap/*` (user in the `video` group or equivalent).
///
/// ```text
/// # Raspberry Pi 5 / CM5 (v3dv) or any Linux desktop GPU:
/// CARGO_BUILD_JOBS=4 cargo test -p daedalus-gpu --features gpu-dmabuf -- --ignored dmabuf
/// # Pick another heap (default /dev/dma_heap/system):
/// DAEDALUS_DMA_HEAP=/dev/dma_heap/linux,cma cargo test -p daedalus-gpu --features gpu-dmabuf -- --ignored dmabuf
/// ```
#[cfg(all(feature = "gpu-dmabuf", target_os = "linux"))]
mod hardware {
    use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
    use std::sync::Arc;

    use super::*;
    use crate::{
        DRM_FORMAT_MOD_LINEAR, DrmFourcc, ExternalFrameDescriptor, ExternalImportError,
        ExternalKeepalive, ExternalPlane, GpuFormat,
    };

    const DMA_HEAP_IOCTL_ALLOC: u64 = 0xC018_4800; // _IOWR('H', 0, dma_heap_allocation_data)
    const DMA_BUF_IOCTL_SYNC: u64 = 0x4008_6200; // _IOW('b', 0, dma_buf_sync)
    const DMA_BUF_SYNC_RW: u64 = 3;
    const DMA_BUF_SYNC_END: u64 = 4;

    #[repr(C)]
    struct DmaHeapAllocationData {
        len: u64,
        fd: u32,
        fd_flags: u32,
        heap_flags: u64,
    }

    /// A CPU-mapped dma-heap buffer.
    struct DmaBuf {
        fd: OwnedFd,
        ptr: *mut u8,
        len: usize,
    }

    impl DmaBuf {
        fn alloc(len: usize) -> std::io::Result<Self> {
            let heap = std::env::var("DAEDALUS_DMA_HEAP")
                .unwrap_or_else(|_| "/dev/dma_heap/system".into());
            let heap = std::fs::File::open(&heap)?;
            let mut data = DmaHeapAllocationData {
                len: len as u64,
                fd: 0,
                fd_flags: (libc::O_RDWR | libc::O_CLOEXEC) as u32,
                heap_flags: 0,
            };
            // SAFETY: valid heap fd and a correctly laid out argument struct.
            let rc = unsafe { libc::ioctl(heap.as_raw_fd(), DMA_HEAP_IOCTL_ALLOC as _, &mut data) };
            if rc != 0 {
                return Err(std::io::Error::last_os_error());
            }
            // SAFETY: the kernel returned a fresh fd we now own.
            let fd = unsafe { OwnedFd::from_raw_fd(data.fd as i32) };
            // SAFETY: mapping `len` bytes of a dmabuf we own.
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    fd.as_raw_fd(),
                    0,
                )
            };
            if ptr == libc::MAP_FAILED {
                return Err(std::io::Error::last_os_error());
            }
            Ok(Self {
                fd,
                ptr: ptr.cast(),
                len,
            })
        }

        /// CPU write bracketed by DMA_BUF_IOCTL_SYNC so caches are coherent for the GPU.
        fn write(&self, f: impl FnOnce(&mut [u8])) {
            let sync = |flags: u64| {
                let mut arg = flags;
                // SAFETY: valid dmabuf fd and u64 argument (struct dma_buf_sync).
                unsafe { libc::ioctl(self.fd.as_raw_fd(), DMA_BUF_IOCTL_SYNC as _, &mut arg) };
            };
            sync(DMA_BUF_SYNC_RW);
            // SAFETY: `ptr` maps `len` bytes for the lifetime of `self`.
            f(unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) });
            sync(DMA_BUF_SYNC_RW | DMA_BUF_SYNC_END);
        }
    }

    impl Drop for DmaBuf {
        fn drop(&mut self) {
            // SAFETY: unmapping the mapping created in `alloc`.
            unsafe { libc::munmap(self.ptr.cast(), self.len) };
        }
    }

    fn pixel(x: u32, y: u32) -> [u8; 4] {
        [x as u8, y as u8, (x ^ y) as u8, 0xff]
    }

    fn device_poll(backend: &WgpuBackend) {
        let _ = backend.device_queue().0.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });
    }

    #[test]
    #[ignore = "needs a Vulkan GPU with dmabuf import and access to /dev/dma_heap"]
    fn dmabuf_import_is_zero_copy() {
        let backend = WgpuBackend::new().expect("wgpu backend");
        let support = backend.dmabuf_import_support();
        assert!(
            support.is_supported(),
            "dmabuf import unsupported on {:?}: {:?}",
            backend.adapter_info().name,
            support.reason()
        );
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

        assert_eq!(Arc::strong_count(&guard), 2);
        drop(handle);
        drop(offset_handle);
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
        let backend = WgpuBackend::new().expect("wgpu backend");
        if !backend.dmabuf_import_support().is_supported() {
            return;
        }
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
}
