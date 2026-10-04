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

/// Real zero-copy import of dma-heap buffers (single-plane, fences, NV12). Needs a Vulkan GPU with
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
    use std::time::Duration;

    use super::*;
    use crate::{
        DRM_FORMAT_MOD_LINEAR, DmabufAccess, DrmFourcc, ExternalFrameDescriptor,
        ExternalImportError, ExternalKeepalive, ExternalPlane, GpuFormat, GpuImageHandle, GpuUsage,
        export_dmabuf_fence,
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

    /// Print why (part of) a hardware test was skipped; visible with `--nocapture`.
    #[allow(clippy::print_stderr)]
    fn skip(reason: String) {
        eprintln!("skipping: {reason}");
    }

    #[test]
    #[ignore = "needs a Vulkan GPU with dmabuf import and access to /dev/dma_heap"]
    fn dmabuf_import_waits_for_fences() {
        let backend = WgpuBackend::new().expect("wgpu backend");
        if !backend.dmabuf_import_support().is_supported() {
            skip(format!("{:?}", backend.dmabuf_import_support().reason()));
            return;
        }
        let (width, height, stride) = (64u32, 32u32, 256u64);
        let buf = DmaBuf::alloc((stride * u64::from(height)) as usize).expect("dma-heap alloc");
        buf.write(|bytes| bytes[..4].copy_from_slice(&pixel(0, 0)));
        let frame = || {
            let plane = ExternalPlane::from_borrowed(buf.fd.as_fd(), 0, stride).unwrap();
            ExternalFrameDescriptor::single_plane(width, height, DrmFourcc::XRGB8888, plane)
        };

        // Explicit fence exported from the dmabuf's implicit fences (DMA_BUF_IOCTL_EXPORT_SYNC_FILE).
        let fence = export_dmabuf_fence(buf.fd.as_fd(), DmabufAccess::Read).expect("export");
        let handle = backend
            .import_dmabuf(frame().with_acquire_fence(fence))
            .expect("import with exported fence");
        assert_eq!(&backend.read_texture(&handle).unwrap()[..4], &pixel(0, 0));

        // Same through the descriptor helper, for a writer (read+write fences).
        let handle = backend
            .import_dmabuf(
                frame()
                    .with_usage(GpuUsage::UPLOAD)
                    .with_implicit_fence()
                    .expect("implicit fence"),
            )
            .expect("import with implicit fence");
        assert_eq!(&backend.read_texture(&handle).unwrap()[..4], &pixel(0, 0));

        // A fence that never signals times out before any Vulkan object is created, and the
        // keepalive is released with the failed descriptor.
        let (never, _writer) = std::io::pipe().unwrap();
        let guard = Arc::new(());
        let err = backend
            .import_dmabuf(
                frame()
                    .with_keepalive(guard.clone())
                    .with_acquire_fence(never.into())
                    .with_acquire_timeout(Duration::from_millis(20)),
            )
            .unwrap_err();
        assert!(
            matches!(err, ExternalImportError::FenceTimeout { .. }),
            "{err}"
        );
        assert_eq!(Arc::strong_count(&guard), 1);
    }

    fn nv12_luma(x: u32, y: u32) -> u8 {
        (x * 3 + y * 5) as u8
    }

    fn nv12_chroma(x: u32, y: u32) -> [u8; 2] {
        [(x * 16 + 7) as u8, (255 - y * 16) as u8]
    }

    /// Write an NV12 frame: Y plane at `y_offset`, interleaved UV plane at `uv_offset`.
    fn write_nv12(bytes: &mut [u8], w: u32, h: u32, stride: u64, y_offset: u64, uv_offset: u64) {
        for y in 0..h {
            for x in 0..w {
                bytes[(y_offset + u64::from(y) * stride + u64::from(x)) as usize] = nv12_luma(x, y);
            }
        }
        for y in 0..h / 2 {
            for x in 0..w / 2 {
                let at = (uv_offset + u64::from(y) * stride + u64::from(x) * 2) as usize;
                bytes[at..at + 2].copy_from_slice(&nv12_chroma(x, y));
            }
        }
    }

    fn assert_nv12(backend: &WgpuBackend, handle: &GpuImageHandle) {
        assert_eq!(handle.format, GpuFormat::Nv12);
        let words = sample_nv12(backend, handle);
        for y in 0..handle.height {
            for x in 0..handle.width {
                let [u, v] = nv12_chroma(x / 2, y / 2);
                let expected = u32::from(nv12_luma(x, y)) | u32::from(u) << 8 | u32::from(v) << 16;
                let got = words[(y * handle.width + x) as usize];
                assert_eq!(
                    got, expected,
                    "pixel ({x},{y}): {got:06x} != {expected:06x}"
                );
            }
        }
    }

    #[test]
    #[ignore = "needs a Vulkan GPU with dmabuf import and access to /dev/dma_heap"]
    fn dmabuf_import_nv12_as_one_texture() {
        let backend = WgpuBackend::new().expect("wgpu backend");
        if !backend.dmabuf_import_support().is_supported() {
            skip(format!("{:?}", backend.dmabuf_import_support().reason()));
            return;
        }
        let (width, height, stride) = (64u32, 32u32, 256u64);
        let uv_offset = stride * u64::from(height);
        let buf = DmaBuf::alloc((uv_offset * 3 / 2) as usize).expect("dma-heap alloc");
        buf.write(|bytes| write_nv12(bytes, width, height, stride, 0, uv_offset));
        let plane = |offset| ExternalPlane::from_borrowed(buf.fd.as_fd(), offset, stride).unwrap();
        let result = backend.import_dmabuf(
            ExternalFrameDescriptor::new(
                width,
                height,
                DrmFourcc::NV12,
                vec![plane(0), plane(uv_offset)],
            )
            .with_implicit_fence()
            .expect("implicit fence"),
        );
        let handle = match result {
            Ok(handle) => handle,
            Err(ExternalImportError::UnsupportedFormat { reason, .. }) => {
                skip(format!(
                    "NV12 not importable on {:?}: {reason}",
                    backend.adapter_info().name
                ));
                return;
            }
            Err(err) => panic!("NV12 import failed: {err}"),
        };
        assert_nv12(&backend, &handle);
        // wgpu 29 cannot copy out of NV12 textures.
        assert_eq!(
            backend.read_texture(&handle),
            Err(crate::GpuError::Unsupported)
        );

        // Planes in two separate dmabufs: a disjoint image, when the modifier allows it.
        let y_buf = DmaBuf::alloc((stride * u64::from(height)) as usize).expect("y alloc");
        let uv_buf = DmaBuf::alloc((stride * u64::from(height) / 2) as usize).expect("uv alloc");
        let mut whole = vec![0u8; (uv_offset * 3 / 2) as usize];
        write_nv12(&mut whole, width, height, stride, 0, uv_offset);
        y_buf.write(|bytes| bytes.copy_from_slice(&whole[..uv_offset as usize]));
        uv_buf.write(|bytes| bytes.copy_from_slice(&whole[uv_offset as usize..]));
        let planes = vec![
            ExternalPlane::from_borrowed(y_buf.fd.as_fd(), 0, stride).unwrap(),
            ExternalPlane::from_borrowed(uv_buf.fd.as_fd(), 0, stride).unwrap(),
        ];
        match backend.import_dmabuf(ExternalFrameDescriptor::new(
            width,
            height,
            DrmFourcc::NV12,
            planes,
        )) {
            Ok(handle) => assert_nv12(&backend, &handle),
            Err(ExternalImportError::UnsupportedFormat { reason, .. }) => {
                skip(format!("disjoint NV12: {reason}"));
            }
            Err(err) => panic!("disjoint NV12 import failed: {err}"),
        }
    }

    /// Samples an NV12 texture through its plane views and returns `y | u << 8 | v << 16` per pixel.
    fn sample_nv12(backend: &WgpuBackend, handle: &GpuImageHandle) -> Vec<u32> {
        let (device, queue) = backend.device_queue();
        let texture = backend.get_texture(handle).expect("registered texture");
        let [y_view, uv_view]: [wgpu::TextureView; 2] = crate::texture_plane_views(&texture)
            .try_into()
            .expect("two plane views");
        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("nv12-sample"),
            source: wgpu::ShaderSource::Wgsl(
                r#"
@group(0) @binding(0) var y_tex: texture_2d<f32>;
@group(0) @binding(1) var uv_tex: texture_2d<f32>;
@group(0) @binding(2) var<storage, read_write> out: array<u32>;
@compute @workgroup_size(8, 8)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
    let dims = textureDimensions(y_tex);
    if (id.x >= dims.x || id.y >= dims.y) { return; }
    let y = textureLoad(y_tex, vec2<i32>(id.xy), 0).r;
    let uv = textureLoad(uv_tex, vec2<i32>(id.xy / 2u), 0).rg;
    out[id.y * dims.x + id.x] = u32(round(y * 255.0))
        | (u32(round(uv.x * 255.0)) << 8u)
        | (u32(round(uv.y * 255.0)) << 16u);
}
"#
                .into(),
            ),
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("nv12-sample"),
            layout: None,
            module: &shader,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });
        let size = u64::from(handle.width * handle.height) * 4;
        let out = device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("nv12-out"),
            size,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });
        let readback = device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("nv12-readback"),
            size,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("nv12-sample"),
            layout: &pipeline.get_bind_group_layout(0),
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: wgpu::BindingResource::TextureView(&y_view),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: wgpu::BindingResource::TextureView(&uv_view),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: out.as_entire_binding(),
                },
            ],
        });
        let mut encoder = device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_compute_pass(&Default::default());
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.dispatch_workgroups(handle.width.div_ceil(8), handle.height.div_ceil(8), 1);
        }
        encoder.copy_buffer_to_buffer(&out, 0, &readback, 0, size);
        queue.submit(Some(encoder.finish()));
        readback
            .slice(..)
            .map_async(wgpu::MapMode::Read, |res| res.expect("map"));
        device_poll(backend);
        let view = readback.slice(..).get_mapped_range().expect("mapped range");
        let words = bytemuck::cast_slice::<u8, u32>(&view).to_vec();
        drop(view);
        readback.unmap();
        words
    }
}
