use std::fs::File;
use std::os::fd::{AsFd, OwnedFd};

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
    assert_eq!(DrmFourcc::NV12.gpu_format(), None);
}

#[test]
fn validate_accepts_padded_stride_and_reports_min_len() {
    let layout = xrgb_frame(64, 4, 512).validate().unwrap();
    assert_eq!(layout.format, GpuFormat::Bgra8Unorm);
    assert_eq!(layout.bytes_per_pixel, 4);
    assert_eq!(layout.min_len, 512 * 3 + 256);
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

    let nv12 = ExternalFrameDescriptor::single_plane(
        64,
        4,
        DrmFourcc::NV12,
        ExternalPlane::new(some_fd(), 0, 64),
    );
    match nv12.validate().unwrap_err() {
        ExternalImportError::UnsupportedFormat { fourcc, reason, .. } => {
            assert_eq!(fourcc, DrmFourcc::NV12);
            assert!(reason.contains("each plane"), "{reason}");
        }
        other => panic!("unexpected {other:?}"),
    }
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

#[test]
fn noop_backend_reports_unsupported() {
    let backend = NoopBackend::default();
    let support = backend.dmabuf_import_support();
    assert!(!support.is_supported());
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
    use crate::{GpuBackendKind, GpuOptions, GpuUsage, MockBackend};

    #[test]
    fn mock_records_import_and_returns_gpu_handle() {
        let backend = MockBackend::default();
        assert!(backend.dmabuf_import_support().is_supported());
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
}
