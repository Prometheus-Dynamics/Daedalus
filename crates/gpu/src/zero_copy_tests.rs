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
    use std::os::fd::{AsRawFd, OwnedFd};
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
        let len = (width * height) as usize + 4096;
        let buf = lemnos_linux::dma::DmaHeap::open("system")
            .ok()?
            .allocate(len)
            .ok()?;
        // The system heap's mapping is coherent for this test fill (no CPU-access bracket).
        let n = fill.len().min(len);
        buf.map().ok()?.as_mut_slice()[..n].copy_from_slice(&fill[..n]);
        let fd = buf.into_fd();
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

    /// NV12 in one dmabuf with each plane given its own dup of the fd (as Styx's `FrameFdPlane`
    /// does): one import, the chroma read at `stride * height`.
    struct DupNv12 {
        y_fd: OwnedFd,
        uv_fd: OwnedFd,
        _buffer: HeapFrame,
        width: u32,
        height: u32,
    }

    impl FrameSource for DupNv12 {
        fn width(&self) -> u32 {
            self.width
        }
        fn height(&self) -> u32 {
            self.height
        }
        fn format(&self) -> u32 {
            u32::from_le_bytes(*b"NV12")
        }
        fn format_kind(&self) -> FrameFormatKind {
            FrameFormatKind::Pixel
        }
        fn residency(&self) -> FrameResidency {
            FrameResidency::External
        }
        fn plane_count(&self) -> u32 {
            2
        }
        fn plane(&self, index: u32) -> Option<FramePlane> {
            let (w, h) = (u64::from(self.width), u64::from(self.height));
            match index {
                0 => Some(FramePlane::dmabuf(self.y_fd.as_raw_fd(), 0, w, w * h)),
                1 => Some(FramePlane::dmabuf(
                    self.uv_fd.as_raw_fd(),
                    w * h,
                    w,
                    w * h / 2,
                )),
                _ => None,
            }
        }
        fn plane_data(&self, _index: u32) -> Option<&[u8]> {
            None
        }
    }

    #[test]
    fn nv12_with_duplicated_plane_fds_is_one_import() {
        let Some(ctx) = vulkan_ctx() else { return };
        let (w, h) = (256u32, 64u32);
        let pixels = bytes((w * h * 3 / 2) as usize, 11);
        let Some(buffer) = heap_frame(w, h * 3 / 2, &pixels) else {
            return eprintln!("skipping: no /dev/dma_heap/system access");
        };
        let frame = DupNv12 {
            y_fd: buffer.fd.try_clone().unwrap(),
            uv_fd: buffer.fd.try_clone().unwrap(),
            _buffer: buffer,
            width: w,
            height: h,
        };
        assert_ne!(
            frame.y_fd.as_raw_fd(),
            frame.uv_fd.as_raw_fd(),
            "different fd numbers"
        );
        let frame = SharedFrame::new(std::sync::Arc::new(frame));
        let imports = GpuFrameImports::new(&ctx, 2);
        let input = match imports.import(&frame, Duration::from_secs(1)) {
            Ok(input) => input,
            Err(err) => {
                return eprintln!("skipping: this device cannot import dmabuf storage ({err})");
            }
        };
        assert_eq!(input.planes(), [(0, 256), (u64::from(w * h), 256)]);
        assert_eq!(imports.stats().imports, 1);
        // The chroma plane's bytes, read through the one import.
        let out = GpuStorage::new_host_cached(&ctx, u64::from(w * h * 3 / 2), None).unwrap();
        GpuKernels::new(&ctx)
            .unwrap()
            .run(
                GpuKernel::Copy,
                &input.storage,
                &out,
                u64::from(w * h * 3 / 2),
            )
            .unwrap()
            .wait();
        let mut got = vec![0u8; (w * h * 3 / 2) as usize];
        out.read_mapped(0, &mut got).unwrap();
        assert_eq!(
            got[(w * h) as usize..],
            pixels[(w * h) as usize..],
            "chroma at stride * height"
        );
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
            assert_eq!(input.planes(), [(0, u64::from(w))]);
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

    /// Per-frame cost of a `GpuFrameImports::import` that hits the cache (the dmabuf is already
    /// imported), and the heap allocations of one hit on this thread. Run with
    /// `cargo test -p daedalus-gpu --features gpu-dmabuf -- --ignored --nocapture frame_import_hit`.
    #[test]
    #[ignore = "timing probe: needs /dev/dma_heap and a Vulkan GPU with dmabuf import"]
    fn frame_import_hit_cost() {
        let Some(ctx) = vulkan_ctx() else { return };
        let (w, h) = (640u32, 480u32);
        let Some(grey) = heap_frame(w, h, &bytes((w * h) as usize, 3)) else {
            return eprintln!("skipping: no /dev/dma_heap/system access");
        };
        let grey = SharedFrame::new(std::sync::Arc::new(grey));
        let Some(buffer) = heap_frame(w, h * 3 / 2, &bytes((w * h * 3 / 2) as usize, 4)) else {
            return eprintln!("skipping: no /dev/dma_heap/system access");
        };
        let nv12 = DupNv12 {
            y_fd: buffer.fd.try_clone().unwrap(),
            uv_fd: buffer.fd.try_clone().unwrap(),
            _buffer: buffer,
            width: w,
            height: h,
        };
        let nv12 = SharedFrame::new(std::sync::Arc::new(nv12));
        for (name, frame) in [("grey", &grey), ("nv12-dup", &nv12)] {
            let imports = GpuFrameImports::new(&ctx, 2);
            if let Err(err) = imports.import(frame, Duration::from_secs(1)) {
                eprintln!("skipping {name}: this device cannot import dmabuf storage ({err})");
                continue;
            }
            let mut best = f64::MAX;
            for _ in 0..5 {
                let start = std::time::Instant::now();
                for _ in 0..2000 {
                    let input = imports.import(frame, Duration::from_secs(1)).unwrap();
                    std::hint::black_box(&input);
                }
                best = best.min(start.elapsed().as_nanos() as f64 / 2000.0);
            }
            let (input, allocs) = crate::alloc_count::allocations_in(|| {
                imports.import(frame, Duration::from_secs(1))
            });
            std::hint::black_box(input.ok());
            let stats = imports.stats();
            eprintln!(
                "frame import hit {name}: {best:.0} ns per frame (best of 5 x 2000), \
                 {allocs} heap allocations per hit, {} hits {} imports",
                stats.hits, stats.imports
            );
        }
    }

    /// A dmabuf whose fd number is reused by a different buffer is a new import, not a hit: the
    /// cache is keyed by the dmabuf's identity (device and inode), never by the fd number.
    #[test]
    fn reused_fd_number_is_a_new_dmabuf() {
        let Some(ctx) = vulkan_ctx() else { return };
        let (w, h) = (640u32, 480u32);
        let imports = GpuFrameImports::new(&ctx, 4);
        let kernels = GpuKernels::new(&ctx).unwrap();
        let out = GpuStorage::new_host_mapped(&ctx, u64::from(w * h), None).unwrap();
        let mut seen_fds = Vec::new();
        for (round, fill) in [21u8, 22, 23].into_iter().enumerate() {
            let pixels = bytes((w * h) as usize, fill);
            let Some(buffer) = heap_frame(w, h, &pixels) else {
                return eprintln!("skipping: no /dev/dma_heap/system access");
            };
            let fd = buffer.fd.as_raw_fd();
            seen_fds.push(fd);
            let frame = SharedFrame::new(std::sync::Arc::new(buffer));
            let input = match imports.import(&frame, Duration::from_secs(1)) {
                Ok(input) => input,
                Err(err) => {
                    return eprintln!("skipping: this device cannot import dmabuf storage ({err})");
                }
            };
            kernels
                .run(GpuKernel::Copy, &input.storage, &out, u64::from(w * h))
                .unwrap()
                .wait();
            let mut got = vec![0u8; (w * h) as usize];
            out.read_mapped(0, &mut got).unwrap();
            assert_eq!(
                got, pixels,
                "round {round}: the new buffer's bytes, not the old import's"
            );
            drop(input);
            drop(frame);
        }
        let stats = imports.stats();
        assert_eq!(
            (stats.imports, stats.hits),
            (3, 0),
            "{stats:?} fds {seen_fds:?}"
        );
    }
}
