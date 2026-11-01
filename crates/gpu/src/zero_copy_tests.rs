//! Zero-copy GPU input and output: host-mapped storage (no staging buffer either way), storage
//! without the zero-init clear, and camera frames imported once per dmabuf (skipped without a
//! Vulkan device; the frame tests also need `/dev/dma_heap/system`).
#![allow(clippy::print_stderr)]

use crate::{
    GpuBackendKind, GpuContextHandle, GpuKernel, GpuKernels, GpuOptions, GpuStorage, select_backend,
};

fn vulkan_ctx() -> Option<GpuContextHandle> {
    let ctx = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    })
    .ok()?;
    let (device, _) = ctx.wgpu()?;
    // The host-mapped and imported buffers are Vulkan memory.
    // SAFETY: only checks which hal backend the device uses.
    let vulkan = unsafe { device.as_hal::<wgpu::hal::api::Vulkan>().is_some() };
    vulkan.then_some(ctx)
}

fn bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(29).wrapping_add(seed))
        .collect()
}

/// Upload and readback through host-mapped buffers: a kernel reads one and writes the other, the
/// CPU writes and reads them in place, and no staging buffer is ever made.
#[test]
fn host_mapped_storage_round_trips_without_staging() {
    let Some(ctx) = vulkan_ctx() else { return };
    let data = bytes(1 << 20, 3);
    let src = GpuStorage::new_host_mapped(&ctx, data.len() as u64, Some("src")).unwrap();
    let dst = GpuStorage::new_host_cached(&ctx, data.len() as u64, Some("dst")).unwrap();
    assert!(src.is_host_mapped() && dst.is_host_mapped());
    let kernels = GpuKernels::new(&ctx).unwrap();
    for round in 0..3u8 {
        let input = bytes(data.len(), round);
        src.write_mapped(0, &input).unwrap();
        kernels
            .run(GpuKernel::Copy, &src, &dst, input.len() as u64)
            .unwrap()
            .wait();
        let mut out = vec![0u8; input.len()];
        dst.read_mapped(0, &mut out).unwrap();
        assert_eq!(out, input, "round {round}");
    }
    assert_eq!(
        src.staging_buffers_created() + dst.staging_buffers_created(),
        0
    );
    // Ranges are checked; device buffers have no mapping.
    assert!(src.write_mapped(data.len() as u64 - 2, &[0; 4]).is_err());
    let device = GpuStorage::new(&ctx, 16, None).unwrap();
    assert!(device.read_mapped(0, &mut [0; 4]).is_err());
}

/// A buffer without the zero-init clear serves as a kernel output like any other.
#[test]
fn uninit_storage_takes_kernel_output() {
    let Some(ctx) = vulkan_ctx() else { return };
    let input = bytes(4096, 9);
    let src = GpuStorage::with_data(&ctx, &input, None).unwrap();
    let dst = GpuStorage::new_uninit(&ctx, 4096, None).unwrap();
    GpuKernels::new(&ctx)
        .unwrap()
        .run(GpuKernel::Copy, &src, &dst, 4096)
        .unwrap()
        .wait();
    let mut out = vec![0u8; 4096];
    dst.read_into(0, &mut out).unwrap();
    assert_eq!(out, input);
}

#[cfg(feature = "gpu-dmabuf")]
mod frames {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::time::Duration;

    use daedalus_transport::{
        FrameFormatKind, FramePlane, FrameResidency, FrameSource, SharedFrame,
    };

    use super::*;
    use crate::GpuFrameImports;

    /// A GREY frame in a dma-heap buffer (with 4 KiB of padding past the pixels).
    struct HeapFrame {
        fd: OwnedFd,
        width: u32,
        height: u32,
    }

    fn heap_frame(width: u32, height: u32, fill: &[u8]) -> Option<HeapFrame> {
        #[repr(C)]
        struct Alloc {
            len: u64,
            fd: u32,
            fd_flags: u32,
            heap_flags: u64,
        }
        let len = (width * height) as usize + 4096;
        let heap = std::fs::File::options()
            .read(true)
            .write(true)
            .open("/dev/dma_heap/system")
            .ok()?;
        let mut req = Alloc {
            len: len as u64,
            fd: 0,
            fd_flags: (libc::O_RDWR | libc::O_CLOEXEC) as u32,
            heap_flags: 0,
        };
        // SAFETY: `req` is the DMA_HEAP_IOCTL_ALLOC argument.
        if unsafe { libc::ioctl(heap.as_raw_fd(), 0xC018_4800u64 as _, &mut req) } != 0 {
            return None;
        }
        // SAFETY: a new dmabuf fd we own.
        let fd = unsafe { OwnedFd::from_raw_fd(req.fd as i32) };
        // SAFETY: map, fill and unmap the whole buffer (CPU access bracketed by DMA_BUF_IOCTL_SYNC
        // is not needed for this uncached-coherent test fill on the system heap).
        unsafe {
            let ptr = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            );
            if ptr == libc::MAP_FAILED {
                return None;
            }
            std::ptr::copy_nonoverlapping(fill.as_ptr(), ptr.cast(), fill.len().min(len));
            libc::munmap(ptr, len);
        }
        Some(HeapFrame { fd, width, height })
    }

    impl FrameSource for HeapFrame {
        fn width(&self) -> u32 {
            self.width
        }
        fn height(&self) -> u32 {
            self.height
        }
        fn format(&self) -> u32 {
            u32::from_le_bytes(*b"GREY")
        }
        fn format_kind(&self) -> FrameFormatKind {
            FrameFormatKind::Pixel
        }
        fn residency(&self) -> FrameResidency {
            FrameResidency::External
        }
        fn plane_count(&self) -> u32 {
            1
        }
        fn plane(&self, index: u32) -> Option<FramePlane> {
            (index == 0).then(|| {
                FramePlane::dmabuf(
                    self.fd.as_raw_fd(),
                    0,
                    u64::from(self.width),
                    u64::from(self.width * self.height),
                )
            })
        }
        fn plane_data(&self, _index: u32) -> Option<&[u8]> {
            None
        }
    }

    /// Frames from the same dmabuf share one import; a kernel reads the frame's bytes in place.
    #[test]
    fn frames_import_once_per_dmabuf_and_read_in_place() {
        let Some(ctx) = vulkan_ctx() else { return };
        let (w, h) = (640u32, 480u32);
        let pixels = bytes((w * h) as usize, 5);
        let Some(a) = heap_frame(w, h, &pixels) else {
            return eprintln!("skipping: no /dev/dma_heap/system access");
        };
        let a = SharedFrame::new(std::sync::Arc::new(a));
        let b = SharedFrame::new(std::sync::Arc::new(
            heap_frame(w, h, &bytes((w * h) as usize, 6)).unwrap(),
        ));
        let imports = GpuFrameImports::new(&ctx, 2);
        let kernels = GpuKernels::new(&ctx).unwrap();
        let out = GpuStorage::new_host_mapped(&ctx, u64::from(w * h), None).unwrap();
        for round in 0..3 {
            let input = match imports.import(&a, Duration::from_secs(1)) {
                Ok(input) => input,
                Err(err) => {
                    return eprintln!("skipping: this device cannot import dmabuf storage ({err})");
                }
            };
            assert_eq!(input.planes, [(0, u64::from(w))]);
            kernels
                .run(GpuKernel::Copy, &input.storage, &out, u64::from(w * h))
                .unwrap()
                .wait();
            let mut got = vec![0u8; (w * h) as usize];
            out.read_mapped(0, &mut got).unwrap();
            assert_eq!(
                got, pixels,
                "round {round}: the frame's bytes, read in place"
            );
        }
        let _ = imports.import(&b, Duration::from_secs(1)).unwrap();
        let stats = imports.stats();
        assert_eq!((stats.imports, stats.hits), (2, 2), "{stats:?}");
        // Capacity 1: alternating frames evict each other.
        let small = GpuFrameImports::new(&ctx, 1);
        for frame in [&a, &b, &a] {
            let _ = small.import(frame, Duration::from_secs(1)).unwrap();
        }
        assert_eq!(small.stats().evictions, 2);
    }
}
