//! v3 additions: colorimetry, metadata, v2 compatibility and `SharedFrame` ownership.

use super::*;
use crate::{ForeignOwner, Payload, Residency};
use core::sync::atomic::AtomicUsize;

/// An R8 frame with v3 metadata whose drops are counted.
struct Tagged {
    pixels: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl Drop for Tagged {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Relaxed);
    }
}

impl FrameSource for Tagged {
    fn width(&self) -> u32 {
        self.pixels.len() as u32
    }
    fn height(&self) -> u32 {
        1
    }
    fn format(&self) -> u32 {
        fourcc(b"R8  ")
    }
    fn residency(&self) -> FrameResidency {
        FrameResidency::Cpu
    }
    fn plane_count(&self) -> u32 {
        1
    }
    fn plane(&self, index: u32) -> Option<FramePlane> {
        (index == 0).then(|| FramePlane::cpu(&self.pixels, self.pixels.len() as u64))
    }
    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        (index == 0).then_some(&self.pixels[..])
    }
    fn colorimetry(&self) -> FrameColorimetry {
        FrameColorimetry::SYCC
    }
    fn metadata_u64(&self, key: u32) -> Option<u64> {
        match key {
            frame_meta::EXPOSURE_NS => Some(8_000_000),
            frame_meta::ANALOGUE_GAIN => Some(2.5f64.to_bits()),
            _ => None,
        }
    }
    fn metadata_bytes(&self, key: u32) -> Option<&[u8]> {
        (key == frame_meta::SENSOR_ID).then_some(b"imx477@1a".as_slice())
    }
}

fn tagged() -> (Arc<Tagged>, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    let frame = Arc::new(Tagged {
        pixels: vec![1, 2, 3],
        drops: drops.clone(),
    });
    (frame, drops)
}

#[test]
fn v3_reads_colorimetry_and_metadata() {
    let (frame, _) = tagged();
    let handle = ForeignHandle::from_arc::<_, FrameInterface>(frame);
    let view = handle.view::<FrameInterface>().unwrap();
    assert_eq!(view.colorimetry(), FrameColorimetry::SYCC);
    assert_eq!(view.colorimetry().color_range(), ColorRange::Full);
    assert_eq!(view.exposure_ns(), Some(8_000_000));
    assert_eq!(view.analogue_gain(), Some(2.5));
    assert_eq!(view.sensor_id(), Some("imx477@1a"));
    assert_eq!(view.metadata_u64(frame_meta::DIGITAL_GAIN), None);
    assert_eq!(view.metadata_bytes(frame_meta::VENDOR_BASE), None);
    // Defaults of a source that states nothing.
    let plain = ForeignHandle::from_arc::<_, FrameInterface>(nv12());
    let plain = plain.view::<FrameInterface>().unwrap();
    assert_eq!(plain.colorimetry(), FrameColorimetry::UNSPECIFIED);
    assert_eq!(plain.colorimetry().color_range(), ColorRange::Unknown);
    assert_eq!(plain.exposure_ns(), None);
}

#[test]
fn v2_identity_is_the_shipped_one() {
    // `daedalus:frame` v2 as Daedalus 3.0.0 shipped it: 16 accessors, this layout hash.
    assert_eq!(FRAME_INTERFACE_V2.version, 2);
    assert_eq!(FRAME_INTERFACE_V2.layout_hash, 0x0b07_a24f_4c02_e0a7);
    assert_eq!(core::mem::size_of::<FrameVTableV2>(), 16 * 8);
    assert!(is_frame_v2(&FRAME_INTERFACE_V2));
    assert!(!is_frame_v2(FrameInterface::info()));
    assert_ne!(FRAME_INTERFACE_V2, *FrameInterface::info());
}

#[test]
fn a_v3_frame_is_lent_to_v2_and_comes_back_unchanged() {
    let frame = nv12();
    let handle = ForeignHandle::from_arc::<_, FrameInterface>(frame.clone());
    let data = handle.data();
    let lent = frame_handle_to_v2(handle);
    assert!(
        lent.view::<FrameInterface>().is_err(),
        "v3 consumers refuse it"
    );
    // A v2 consumer reads every v2 accessor through the v3 table.
    let v2 = lent.view::<FrameInterfaceV2>().unwrap();
    let vt = v2.vtable();
    unsafe {
        assert_eq!(((vt.width)(v2.data()), (vt.height)(v2.data())), (4, 2));
        assert_eq!((vt.format)(v2.data()), fourcc(b"NV12"));
        assert_eq!((vt.format_kind)(v2.data()), FrameFormatKind::Pixel as u32);
        assert_eq!((vt.modifier)(v2.data()), DRM_FORMAT_MOD_LINEAR);
        assert_eq!((vt.timestamp_ns)(v2.data()), 0);
        assert_eq!((vt.sequence)(v2.data()), 7);
        assert_eq!((vt.residency)(v2.data()), FrameResidency::External as u32);
        assert_eq!((vt.plane_count)(v2.data()), 3);
        assert_eq!((vt.plane_fd)(v2.data(), 2), 9);
        assert_eq!((vt.plane_offset)(v2.data(), 2), FAR);
        assert_eq!((vt.plane_stride)(v2.data(), 2), FAR + 1);
        assert_eq!((vt.plane_len)(v2.data(), 2), FAR + 2);
        assert_eq!(
            (vt.plane_mapping)(v2.data(), 1),
            PlaneMapping::Uncached as u32
        );
        let mut len = 0;
        let ptr = (vt.plane_data)(v2.data(), 0, &mut len);
        assert_eq!((ptr, len), (frame.luma.as_ptr(), 8));
        (vt.plane_end_cpu_access)(v2.data(), 0);
    }
    assert_eq!(
        (frame.begun.load(Relaxed), frame.ended.load(Relaxed)),
        (1, 1)
    );
    // Coming back: the same v3 table and data, no adapter.
    let back = frame_handle_from_v2(lent);
    assert!(core::ptr::eq(back.interface(), FrameInterface::info()));
    assert_eq!(back.data(), data);
    assert_eq!(back.view::<FrameInterface>().unwrap().sequence(), 7);
    drop(back);
    assert_eq!(Arc::strong_count(&frame), 1, "retagging never retains");
}

#[test]
fn a_frame_made_by_a_v2_build_reads_through_v3() {
    let (frame, drops) = tagged();
    // A v2 build's vtable: v2's accessors only.
    let v3 = <Tagged as ProvideForeign<FrameInterface>>::vtable();
    let v2: &'static FrameVTableV2 = Box::leak(Box::new(FrameVTableV2 {
        width: v3.width,
        height: v3.height,
        format: v3.format,
        format_kind: v3.format_kind,
        modifier: v3.modifier,
        timestamp_ns: v3.timestamp_ns,
        sequence: v3.sequence,
        residency: v3.residency,
        plane_count: v3.plane_count,
        plane_fd: v3.plane_fd,
        plane_offset: v3.plane_offset,
        plane_stride: v3.plane_stride,
        plane_len: v3.plane_len,
        plane_mapping: v3.plane_mapping,
        plane_data: v3.plane_data,
        plane_end_cpu_access: v3.plane_end_cpu_access,
    }));
    let data = Arc::as_ptr(&frame).cast();
    // Safety: `v2`'s functions take a `Tagged`, kept alive by the owner reference.
    let handle = unsafe {
        ForeignHandle::from_raw_parts::<FrameInterfaceV2>(data, v2, ForeignOwner::from_arc(frame))
    };
    let upgraded = frame_handle_from_v2(handle);
    let view = upgraded.view::<FrameInterface>().unwrap();
    assert_eq!((view.width(), view.format()), (3, fourcc(b"R8  ")));
    assert_eq!(&*view.plane_bytes(0).unwrap(), &[1, 2, 3]);
    assert_eq!(view.plane(0).unwrap().len, 3);
    assert!(view.plane(1).is_none());
    // v3's additions are unknown for a v2 frame.
    assert_eq!(view.colorimetry(), FrameColorimetry::UNSPECIFIED);
    assert_eq!(view.exposure_ns(), None);
    assert_eq!(view.sensor_id(), None);
    assert_eq!(drops.load(Relaxed), 0);
    drop(upgraded);
    assert_eq!(drops.load(Relaxed), 1, "the v2 owner released once");
}

#[test]
fn lending_twice_is_harmless() {
    let raw = Arc::new(Raw(FrameFormatKind::Bayer));
    let handle = ForeignHandle::from_arc::<_, FrameInterface>(raw);
    // A handle already lent as v2 is not a v3 one, so it passes unchanged.
    let lent = frame_handle_to_v2(handle);
    let lent = frame_handle_to_v2(lent);
    assert!(is_frame_v2(lent.interface()));
    let back = frame_handle_from_v2(lent);
    assert!(back.view::<FrameInterface>().is_ok());
}

#[test]
fn shared_frames_release_their_owner_exactly_once() {
    let (frame, drops) = tagged();
    let shared = SharedFrame::new(frame.clone());
    assert!(shared.is_local());
    drop(frame);
    let clone = shared.clone();
    assert!(clone.ptr_eq(&shared));
    assert_eq!(clone.as_view().sensor_id(), Some("imx477@1a"));
    assert_eq!(clone.payload_residency(), Residency::Cpu);
    // Through a payload and back: a retain of the same frame.
    let payload = clone.into_payload();
    assert!(payload.foreign_handle_is_local());
    assert_eq!(payload.type_key().as_str(), FRAME_INTERFACE_KEY);
    let again = SharedFrame::from_payload(&payload).unwrap().unwrap();
    assert!(again.is_local() && again.ptr_eq(&shared));
    drop((payload, shared));
    assert_eq!(drops.load(Relaxed), 0, "`again` still holds it");
    // Received from another build: not local, still the same frame.
    let received = SharedFrame::from_handle(again.handle().clone()).unwrap();
    assert!(!received.is_local());
    let payload = received.into_payload();
    assert!(!payload.foreign_handle_is_local());
    drop((payload, again));
    assert_eq!(drops.load(Relaxed), 1);
}

#[test]
fn shared_frames_come_from_provider_payloads_and_refuse_other_versions() {
    let (frame, drops) = tagged();
    let payload = Payload::shared_with("test:tagged", frame, Residency::Cpu, None, None)
        .provide_foreign::<Tagged, FrameInterface>()
        .unwrap();
    let shared = SharedFrame::from_payload(&payload).unwrap().unwrap();
    assert!(shared.is_local(), "a handle over the owner's own Arc");
    assert_eq!(shared.as_view().width(), 3);
    assert!(SharedFrame::from_payload(&Payload::owned("x", 1_i64)).is_none());
    let lent = frame_handle_to_v2(shared.handle().clone());
    assert!(
        SharedFrame::from_handle(lent).is_err(),
        "a v2 handle is not a v3 frame"
    );
    drop((payload, shared));
    assert_eq!(drops.load(Relaxed), 1);
}
