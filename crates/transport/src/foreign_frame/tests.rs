use super::*;
use crate::{ForeignHandle, ForeignInterface};
use alloc::sync::Arc;
use core::sync::atomic::{AtomicU32, Ordering::Relaxed};

/// NV12 in host memory plus a third, dma-buf-only plane at an offset beyond `u32`; counts CPU
/// accesses.
#[derive(Default)]
struct Nv12 {
    luma: Vec<u8>,
    chroma: Vec<u8>,
    begun: AtomicU32,
    ended: AtomicU32,
}

const FAR: u64 = 6 << 30;

impl FrameSource for Nv12 {
    fn width(&self) -> u32 {
        4
    }
    fn height(&self) -> u32 {
        2
    }
    fn format(&self) -> u32 {
        fourcc(b"NV12")
    }
    fn format_kind(&self) -> FrameFormatKind {
        FrameFormatKind::Pixel
    }
    fn sequence(&self) -> u64 {
        7
    }
    fn residency(&self) -> FrameResidency {
        FrameResidency::External
    }
    fn plane_count(&self) -> u32 {
        3
    }
    fn plane(&self, index: u32) -> Option<FramePlane> {
        match index {
            0 => Some(FramePlane::cpu(&self.luma, 4)),
            1 => Some(FramePlane::cpu(&self.chroma, 4).with_mapping(PlaneMapping::Uncached)),
            2 => Some(FramePlane::dmabuf(9, FAR, FAR + 1, FAR + 2)),
            _ => None,
        }
    }
    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        let bytes = match index {
            0 => &self.luma,
            1 => &self.chroma,
            _ => return None,
        };
        self.begun.fetch_add(1, Relaxed);
        Some(bytes)
    }
    fn end_cpu_access(&self, _index: u32) {
        self.ended.fetch_add(1, Relaxed);
    }
}

fn nv12() -> Arc<Nv12> {
    Arc::new(Nv12 {
        luma: vec![1; 8],
        chroma: vec![2; 4],
        ..Nv12::default()
    })
}

#[test]
fn metadata_never_maps_and_keeps_u64_layouts() {
    let frame = nv12();
    let handle = ForeignHandle::from_arc::<_, FrameInterface>(frame.clone());
    let view = handle.view::<FrameInterface>().unwrap();
    assert_eq!((view.width(), view.height()), (4, 2));
    assert_eq!(view.format(), u32::from_le_bytes(*b"NV12"));
    assert_eq!(view.format_kind(), FrameFormatKind::Pixel);
    assert_eq!(view.modifier(), DRM_FORMAT_MOD_LINEAR);
    assert_eq!((view.sequence(), view.timestamp_ns()), (7, 0));
    assert_eq!(view.residency(), FrameResidency::External);
    let planes: Vec<_> = view.planes().collect();
    assert_eq!(planes.len(), 3);
    assert_eq!(planes[0], FramePlane::cpu(&frame.luma, 4));
    assert_eq!(planes[1].mapping, PlaneMapping::Uncached);
    assert_eq!(planes[2], FramePlane::dmabuf(9, FAR, FAR + 1, FAR + 2));
    assert!(planes[2].offset > u64::from(u32::MAX));
    assert!(view.plane(3).is_none());
    assert_eq!(frame.begun.load(Relaxed), 0, "metadata maps nothing");
    assert_eq!(FrameInterface::info().key(), FRAME_INTERFACE_KEY);
}

#[test]
fn cpu_access_is_explicit_and_balanced() {
    let frame = nv12();
    let handle = ForeignHandle::from_arc::<_, FrameInterface>(frame.clone());
    let view = handle.view::<FrameInterface>().unwrap();
    {
        let luma = view.plane_bytes(0).unwrap();
        assert_eq!(luma.as_ptr(), frame.luma.as_ptr(), "read in place");
        assert_eq!(&*luma, &[1u8; 8][..]);
        assert_eq!(
            frame.ended.load(Relaxed),
            0,
            "access open while the guard lives"
        );
    }
    assert_eq!(frame.ended.load(Relaxed), 1);
    let planes: Vec<_> = view.cpu_planes().collect();
    assert_eq!(planes.len(), 3);
    assert_eq!(planes[1].as_deref(), Some(&[2u8; 4][..]));
    assert!(
        planes[2].is_none(),
        "the dma-buf-only plane is not CPU-readable"
    );
    assert!(view.plane_bytes(3).is_none());
    drop(planes);
    assert_eq!(
        (frame.begun.load(Relaxed), frame.ended.load(Relaxed)),
        (3, 3)
    );
}

#[test]
fn raw_values_decode_conservatively() {
    for kind in [
        FrameFormatKind::Unknown,
        FrameFormatKind::Pixel,
        FrameFormatKind::Bayer,
        FrameFormatKind::Compressed,
    ] {
        assert_eq!(FrameFormatKind::from_raw(kind as u32), kind);
    }
    assert_eq!(FrameFormatKind::from_raw(99), FrameFormatKind::Unknown);
    for mapping in [
        PlaneMapping::Cached,
        PlaneMapping::Uncached,
        PlaneMapping::WriteCombined,
        PlaneMapping::Unmapped,
    ] {
        assert_eq!(PlaneMapping::from_raw(mapping as u32), mapping);
    }
    assert_eq!(PlaneMapping::from_raw(99), PlaneMapping::Uncached);
    assert!(!PlaneMapping::Unmapped.is_cpu_readable());
}

/// SRGGB10 with libcamera's CSI-2 packing, or a pixel format with a MediaTek modifier.
struct Raw(FrameFormatKind);

impl FrameSource for Raw {
    fn width(&self) -> u32 {
        8
    }
    fn height(&self) -> u32 {
        2
    }
    fn format(&self) -> u32 {
        fourcc(b"RG10")
    }
    fn format_kind(&self) -> FrameFormatKind {
        self.0
    }
    fn modifier(&self) -> u64 {
        MIPI_FORMAT_MOD_CSI2_PACKED
    }
    fn residency(&self) -> FrameResidency {
        FrameResidency::External
    }
    fn plane_count(&self) -> u32 {
        0
    }
    fn plane(&self, _index: u32) -> Option<FramePlane> {
        None
    }
}

#[test]
fn csi2_packing_needs_the_bayer_kind() {
    assert_eq!(MIPI_FORMAT_MOD_CSI2_PACKED >> 56, 0x0b);
    for (kind, packed) in [
        (FrameFormatKind::Bayer, true),
        (FrameFormatKind::Pixel, false),
        (FrameFormatKind::Unknown, false),
    ] {
        let raw = Raw(kind);
        let view = crate::ForeignBorrow::of::<_, FrameInterface>(&raw)
            .view::<FrameInterface>()
            .unwrap();
        assert_eq!(view.format_kind(), kind);
        assert_eq!(view.is_csi2_packed(), packed, "{kind:?}");
    }
}

// The v1 declaration exactly as `daedalus:frame` v1 shipped it.
crate::foreign_interface! {
    interface FrameV1("daedalus:frame", version = 1);
    struct FrameV1VTable {
        pub width: unsafe extern "C" fn(data: *const c_void) -> u32,
        pub height: unsafe extern "C" fn(data: *const c_void) -> u32,
        pub format: unsafe extern "C" fn(data: *const c_void) -> u32,
        pub modifier: unsafe extern "C" fn(data: *const c_void) -> u64,
        pub timestamp_ns: unsafe extern "C" fn(data: *const c_void) -> u64,
        pub sequence: unsafe extern "C" fn(data: *const c_void) -> u64,
        pub residency: unsafe extern "C" fn(data: *const c_void) -> u32,
        pub plane_count: unsafe extern "C" fn(data: *const c_void) -> u32,
        pub plane_data: unsafe extern "C" fn(data: *const c_void, index: u32) -> *const u8,
        pub plane_len: unsafe extern "C" fn(data: *const c_void, index: u32) -> usize,
        pub plane_stride: unsafe extern "C" fn(data: *const c_void, index: u32) -> u32,
        pub plane_offset: unsafe extern "C" fn(data: *const c_void, index: u32) -> u32,
        pub plane_fd: unsafe extern "C" fn(data: *const c_void, index: u32) -> i32,
    }
}

#[test]
fn v1_and_v2_never_mix() {
    assert_eq!(*FrameV1::info(), FRAME_INTERFACE_V1);
    let (v1, v2) = (FRAME_INTERFACE_V1, *FrameInterface::info());
    assert_eq!(
        (v1.key(), v2.key()),
        (FRAME_INTERFACE_KEY, FRAME_INTERFACE_KEY)
    );
    assert_eq!((v1.version, v2.version), (1, 2));
    assert_ne!(v1.layout_hash, v2.layout_hash);
    let handle = ForeignHandle::from_arc::<_, FrameInterface>(nv12());
    let err = handle.view::<FrameV1>().unwrap_err();
    assert_eq!((err.expected, err.found), (v1, v2));
}
