use std::fs::File;
use std::io::Write;
use std::os::fd::{AsFd, OwnedFd};
use std::time::Duration;

use super::*;
use crate::{GpuBackend, GpuError, NoopBackend};

/// Any real fd works for descriptor-level tests; backends that touch memory reject non-dmabufs.
fn some_fd() -> OwnedFd {
    OwnedFd::from(File::open("/dev/null").expect("open /dev/null"))
}

fn xrgb_frame(width: u32, height: u32, stride: u64) -> ExternalFrameDescriptor {
    ExternalFrameDescriptor::single_plane(
        width,
        height,
        DrmFourcc::XRGB8888,
        ExternalPlane::new(some_fd(), 0, stride),
    )
}

/// NV12 with both planes in one buffer: Y at 0, UV right after it.
fn nv12_frame(width: u32, height: u32, stride: u64) -> ExternalFrameDescriptor {
    let uv_offset = stride * u64::from(height);
    ExternalFrameDescriptor::new(
        width,
        height,
        DrmFourcc::NV12,
        vec![
            ExternalPlane::new(some_fd(), 0, stride),
            ExternalPlane::new(some_fd(), uv_offset, stride),
        ],
    )
}

/// A pipe read end: signaled (readable) once the returned writer has written a byte.
fn pipe_fence() -> (OwnedFd, std::io::PipeWriter) {
    let (reader, writer) = std::io::pipe().expect("pipe");
    (reader.into(), writer)
}

#[test]
fn fourcc_codes_match_drm_fourcc_h() {
    // Values from drm_fourcc.h.
    assert_eq!(DrmFourcc::XRGB8888.0, 0x3432_5258);
    assert_eq!(DrmFourcc::ABGR8888.0, 0x3432_4241);
    assert_eq!(DrmFourcc::R8.0, 0x2020_3852);
    assert_eq!(DrmFourcc::NV12.0, 0x3231_564e);
    assert_eq!(DrmFourcc::XRGB8888.to_string(), "XR24");
    assert_eq!(DrmFourcc::R8.to_string(), "R8");
    assert_eq!(DrmFourcc(0x0102_0304).to_string(), "0x01020304");
    assert_eq!(
        DrmFourcc::XRGB8888.gpu_format(),
        Some(GpuFormat::Bgra8Unorm)
    );
    assert_eq!(
        DrmFourcc::XBGR8888.gpu_format(),
        Some(GpuFormat::Rgba8Unorm)
    );
    assert_eq!(DrmFourcc::GR88.gpu_format(), Some(GpuFormat::Rg8Unorm));
    assert_eq!(DrmFourcc::NV12.gpu_format(), Some(GpuFormat::Nv12));
    assert_eq!(DrmFourcc::YUV420.gpu_format(), None);
}

#[test]
fn validate_accepts_padded_stride_and_reports_min_len() {
    let layout = xrgb_frame(64, 4, 512).validate().unwrap();
    assert_eq!(layout.format, GpuFormat::Bgra8Unorm);
    assert_eq!(layout.min_len, vec![512 * 3 + 256]);
}

#[test]
fn validate_nv12_plane_layout() {
    // 64x4 NV12 at stride 128: Y ends at 3*128+64, UV (2 rows of 32 UV pairs) at 512+128+64.
    let layout = nv12_frame(64, 4, 128).validate().unwrap();
    assert_eq!(layout.format, GpuFormat::Nv12);
    assert_eq!(layout.min_len, vec![3 * 128 + 64, 512 + 128 + 64]);
    assert_eq!(crate::format_bytes_per_pixel(GpuFormat::Nv12), None);
    assert_eq!(
        crate::format_planes(GpuFormat::Nv12)[1].extent(64, 4),
        (32, 2)
    );

    let invalid = |desc: ExternalFrameDescriptor| match desc.validate() {
        Err(ExternalImportError::InvalidDescriptor { reason }) => reason,
        other => panic!("expected InvalidDescriptor, got {other:?}"),
    };
    assert!(invalid(nv12_frame(63, 4, 128)).contains("divisible by 2"));
    assert!(invalid(nv12_frame(64, 3, 128)).contains("divisible by 2"));
    let mut one_plane = nv12_frame(64, 4, 128);
    one_plane.planes.pop();
    assert!(invalid(one_plane).contains("exactly 2 plane(s)"));
    // The UV plane of a 64-wide frame needs 64 bytes per row.
    let mut narrow_uv = nv12_frame(64, 4, 128);
    narrow_uv.planes[1].stride = 32;
    assert!(invalid(narrow_uv).contains("plane 1"));
}

#[test]
fn validate_rejects_bad_descriptors() {
    let err = xrgb_frame(64, 4, 100).validate().unwrap_err();
    assert!(
        matches!(err, ExternalImportError::InvalidDescriptor { .. }),
        "{err}"
    );

    let err = xrgb_frame(0, 4, 256).validate().unwrap_err();
    assert!(matches!(err, ExternalImportError::InvalidDescriptor { .. }));

    let err = xrgb_frame(64, 4, 256)
        .with_modifier(DRM_FORMAT_MOD_INVALID)
        .validate()
        .unwrap_err();
    assert!(matches!(err, ExternalImportError::InvalidDescriptor { .. }));

    let mut two = xrgb_frame(64, 4, 256);
    two.planes.push(ExternalPlane::new(some_fd(), 1024, 256));
    assert!(matches!(
        two.validate().unwrap_err(),
        ExternalImportError::InvalidDescriptor { .. }
    ));

    let yuv420 = ExternalFrameDescriptor::single_plane(
        64,
        4,
        DrmFourcc::YUV420,
        ExternalPlane::new(some_fd(), 0, 64),
    );
    match yuv420.validate().unwrap_err() {
        ExternalImportError::UnsupportedFormat { fourcc, reason, .. } => {
            assert_eq!(fourcc, DrmFourcc::YUV420);
            assert!(reason.contains("each plane"), "{reason}");
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn acquire_fence_wait_outcomes() {
    // No fence: nothing to wait for.
    xrgb_frame(64, 4, 256).wait_acquire_fence().unwrap();

    // Signaled fence.
    let (fence, mut writer) = pipe_fence();
    writer.write_all(&[1]).unwrap();
    let frame = xrgb_frame(64, 4, 256).with_acquire_fence(fence);
    assert_eq!(frame.acquire_timeout, DEFAULT_ACQUIRE_TIMEOUT);
    frame.wait_acquire_fence().unwrap();

    // Never signaled: typed timeout.
    let (fence, _writer) = pipe_fence();
    let timeout = Duration::from_millis(10);
    let err = xrgb_frame(64, 4, 256)
        .with_acquire_fence(fence)
        .with_acquire_timeout(timeout)
        .wait_acquire_fence()
        .unwrap_err();
    assert_eq!(err, ExternalImportError::FenceTimeout { timeout });
    assert!(GpuError::from(err).to_string().contains("acquire fence"));

    // Hung-up without ever becoming readable: not a sync_file.
    let (fence, writer) = pipe_fence();
    drop(writer);
    let err = xrgb_frame(64, 4, 256)
        .with_acquire_fence(fence)
        .wait_acquire_fence()
        .unwrap_err();
    assert!(
        matches!(&err, ExternalImportError::InvalidDescriptor { reason } if reason.contains("sync_file")),
        "{err}"
    );
}

#[test]
fn implicit_fence_export_needs_a_dmabuf() {
    // /dev/null has no DMA_BUF_IOCTL_EXPORT_SYNC_FILE.
    assert!(export_dmabuf_fence(some_fd().as_fd(), DmabufAccess::Read).is_err());
    assert!(xrgb_frame(64, 4, 256).with_implicit_fence().is_err());
    let mut empty = xrgb_frame(64, 4, 256);
    empty.planes.clear();
    let err = empty.with_implicit_fence().unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn errors_convert_to_gpu_error() {
    let unsupported = ExternalImportError::Unsupported { reason: "x".into() };
    assert_eq!(GpuError::from(unsupported), GpuError::Unsupported);
    let failed = ExternalImportError::ImportFailed {
        reason: "boom".into(),
    };
    assert!(matches!(GpuError::from(failed), GpuError::Internal(msg) if msg.contains("boom")));
}

#[test]
fn from_borrowed_dups_the_fd() {
    let file = File::open("/dev/null").unwrap();
    let plane = ExternalPlane::from_borrowed(file.as_fd(), 16, 256).unwrap();
    assert_ne!(
        std::os::fd::AsRawFd::as_raw_fd(&plane.fd),
        std::os::fd::AsRawFd::as_raw_fd(&file)
    );
    assert_eq!((plane.offset, plane.stride), (16, 256));
}

#[cfg(feature = "gpu-dmabuf")]
#[test]
fn sync_file_signaled_is_a_non_blocking_check() {
    let (fence, mut writer) = pipe_fence();
    assert!(!sync_file_signaled(fence.as_fd()));
    writer.write_all(&[1]).unwrap();
    assert!(sync_file_signaled(fence.as_fd()));
}

#[test]
fn noop_backend_reports_unsupported() {
    let backend = NoopBackend::default();
    let support = backend.dmabuf_import_support();
    assert!(!support.is_supported());
    assert_eq!(support.acquire_fence_wait(), None);
    assert!(support.reason().unwrap().contains("noop"));
    let err = backend.import_dmabuf(xrgb_frame(64, 4, 256)).unwrap_err();
    assert!(matches!(err, ExternalImportError::Unsupported { .. }));
}

#[test]
fn capability_query_is_safe_on_any_selected_backend() {
    // Must not panic whatever backend (including a real GPU, or none) gets selected.
    let ctx = crate::select_backend(&crate::GpuOptions::default()).unwrap();
    let support = ctx.dmabuf_import_support();
    assert_eq!(support.is_supported(), ctx.supports_dmabuf_import());
    if let Some(reason) = support.reason() {
        assert!(!reason.is_empty());
    }
}

#[cfg(feature = "gpu-mock")]
mod mock {
    use std::sync::Arc;

    use super::*;
    use crate::{AcquireFenceWait, GpuBackendKind, GpuOptions, GpuUsage, MockBackend};

    #[test]
    fn mock_records_import_and_returns_gpu_handle() {
        let backend = MockBackend::default();
        let support = backend.dmabuf_import_support();
        assert!(support.is_supported() && support.reason().is_none());
        assert_eq!(support.acquire_fence_wait(), Some(AcquireFenceWait::Cpu));
        let handle = backend
            .import_dmabuf(
                xrgb_frame(640, 480, 2560)
                    .with_modifier(DRM_FORMAT_MOD_LINEAR)
                    .with_label("cam0"),
            )
            .unwrap();
        assert_eq!(handle.format, GpuFormat::Bgra8Unorm);
        assert_eq!((handle.width, handle.height), (640, 480));
        assert_eq!(handle.label.as_deref(), Some("cam0"));
        assert!(handle.usage.contains(GpuUsage::DOWNLOAD));

        let records = backend.imported_frames();
        assert_eq!(records.len(), 1);
        let rec = &records[0];
        assert_eq!(rec.image, handle.id);
        assert_eq!(rec.fourcc, DrmFourcc::XRGB8888);
        assert_eq!(rec.modifier, Some(DRM_FORMAT_MOD_LINEAR));
        assert_eq!(rec.planes, vec![(0, 2560)]);
        assert!(!rec.has_keepalive);
        assert!(!rec.had_acquire_fence);
    }

    #[test]
    fn mock_imports_nv12_as_one_image() {
        let backend = MockBackend::default();
        let handle = backend.import_dmabuf(nv12_frame(64, 4, 128)).unwrap();
        assert_eq!(handle.format, GpuFormat::Nv12);
        // Like wgpu, NV12 is sample-only: no readback.
        assert!(!handle.usage.contains(GpuUsage::DOWNLOAD));
        assert_eq!(backend.read_texture(&handle), Err(GpuError::Unsupported));
        let rec = &backend.imported_frames()[0];
        assert_eq!(rec.format, GpuFormat::Nv12);
        assert_eq!(rec.planes, vec![(0, 128), (512, 128)]);

        let err = backend
            .import_dmabuf(nv12_frame(64, 4, 128).with_usage(GpuUsage::STORAGE))
            .unwrap_err();
        assert!(
            matches!(err, ExternalImportError::UnsupportedFormat { .. }),
            "{err}"
        );
    }

    #[test]
    fn mock_waits_for_the_acquire_fence() {
        let backend = MockBackend::default();
        let (fence, mut writer) = pipe_fence();
        writer.write_all(&[1]).unwrap();
        backend
            .import_dmabuf(xrgb_frame(64, 4, 256).with_acquire_fence(fence))
            .unwrap();
        assert!(backend.imported_frames()[0].had_acquire_fence);

        let guard = Arc::new(());
        let (fence, _writer) = pipe_fence();
        let err = backend
            .import_dmabuf(
                xrgb_frame(64, 4, 256)
                    .with_keepalive(guard.clone())
                    .with_acquire_fence(fence)
                    .with_acquire_timeout(Duration::from_millis(10)),
            )
            .unwrap_err();
        assert!(matches!(err, ExternalImportError::FenceTimeout { .. }));
        assert_eq!(
            backend.imported_frames().len(),
            1,
            "failed import not recorded"
        );
        assert_eq!(
            Arc::strong_count(&guard),
            1,
            "keepalive released on failure"
        );
    }

    #[test]
    fn mock_holds_keepalive_until_every_handle_clone_drops() {
        let backend = MockBackend::default();
        let guard: Arc<String> = Arc::new("camera buffer lease".into());
        let keepalive: ExternalKeepalive = guard.clone();
        let handle = backend
            .import_dmabuf(xrgb_frame(64, 4, 256).with_keepalive(keepalive))
            .unwrap();
        assert!(backend.imported_frames()[0].has_keepalive);
        assert_eq!(Arc::strong_count(&guard), 2);
        let clone = handle.clone();
        drop(handle);
        assert_eq!(Arc::strong_count(&guard), 2, "clone still alive");
        drop(clone);
        assert_eq!(Arc::strong_count(&guard), 1, "keepalive released");
    }

    #[test]
    fn mock_rejects_invalid_and_can_be_disabled() {
        let backend = MockBackend::default();
        let err = backend.import_dmabuf(xrgb_frame(64, 4, 8)).unwrap_err();
        assert!(matches!(err, ExternalImportError::InvalidDescriptor { .. }));
        let err = backend
            .import_dmabuf(xrgb_frame(8192, 4, 32768))
            .unwrap_err();
        assert!(matches!(err, ExternalImportError::InvalidDescriptor { .. }));
        assert!(backend.imported_frames().is_empty());

        let disabled = MockBackend::default().without_dmabuf_import();
        assert!(!disabled.dmabuf_import_support().is_supported());
        let err = disabled.import_dmabuf(xrgb_frame(64, 4, 256)).unwrap_err();
        assert!(matches!(err, ExternalImportError::Unsupported { .. }));
    }

    #[test]
    fn context_handle_forwards_import() {
        let ctx = crate::select_backend(&GpuOptions {
            preferred_backend: Some(GpuBackendKind::Mock),
            ..Default::default()
        })
        .unwrap();
        assert!(ctx.supports_dmabuf_import());
        let handle = ctx.import_dmabuf(xrgb_frame(64, 4, 256)).unwrap();
        assert_eq!(handle.format, GpuFormat::Bgra8Unorm);
        // Readback works through the regular texture path.
        assert_eq!(ctx.read_texture(&handle).unwrap().len(), 64 * 4 * 4);
    }

    /// CPU-wait backends block the import until a late fence signals, and no longer.
    #[test]
    fn mock_import_waits_for_a_late_fence() {
        let backend = MockBackend::default();
        let (fence, mut writer) = pipe_fence();
        let delay = Duration::from_millis(50);
        let signal = std::thread::spawn(move || {
            std::thread::sleep(delay);
            writer.write_all(&[1]).unwrap();
        });
        let start = std::time::Instant::now();
        backend
            .import_dmabuf(xrgb_frame(64, 4, 256).with_acquire_fence(fence))
            .unwrap();
        assert!(start.elapsed() >= delay);
        signal.join().unwrap();
        assert_eq!(backend.imported_frames().len(), 1);
    }
}
