use crate::portable::Arc;
use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;

use daedalus_core::platform::Clock;
use daedalus_transport::{FeedOutcome, Payload};

use super::*;
use crate::host_bridge::{HostBridgeManager, InboundWait};

const MS: u64 = 1_000_000;

/// A bridge on a manual clock (milliseconds).
fn bridge() -> (HostBridgeHandle, Arc<AtomicU64>) {
    let ms = Arc::new(AtomicU64::new(0));
    let manager = HostBridgeManager::new();
    manager.set_clock(Clock::new({
        let ms = ms.clone();
        move || Duration::from_millis(ms.load(Ordering::SeqCst))
    }));
    (manager.ensure_handle("host"), ms)
}

const PORTS: [&str; 3] = ["cam0", "cam1", "cam2"];

fn config(partial: PartialPolicy) -> SyncConfig {
    SyncConfig {
        partial,
        ..SyncConfig::new(Duration::from_millis(8), Duration::from_millis(50))
    }
}

fn frame(value: i64) -> Payload {
    Payload::owned("i64", value)
}

/// What the next tick takes: `(port, value)` sorted by port.
fn tick(host: &HostBridgeHandle) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    host.take_inbound_into(&mut out);
    let mut seen: Vec<_> = out
        .iter()
        .map(|entry| {
            let value = *entry.payload.get_ref::<i64>().expect("i64");
            (entry.port.as_str().to_string(), value)
        })
        .collect();
    seen.sort();
    seen
}

fn group(values: &[(usize, i64)]) -> Vec<(String, i64)> {
    values
        .iter()
        .map(|&(camera, value)| (PORTS[camera].to_string(), value))
        .collect()
}

#[test]
fn jittered_frames_group_into_whole_ticks_in_any_arrival_order() {
    let (host, _) = bridge();
    let cameras = MultiCamera::synchronized(&host, PORTS, config(PartialPolicy::Drop));
    let jitter = [0, 3, 5, 2, 7, 1];
    // Camera 2 lags a frame behind the others: arrival is out of order across cameras.
    for n in 0..6_i64 {
        let stamp = |camera: usize| (n as u64 * 33 + jitter[(n as usize + camera) % 6]) * MS;
        assert!(matches!(
            cameras.push_stamped(0, frame(n), stamp(0)),
            CameraPush::Buffered
        ));
        cameras.push_stamped(1, frame(n), stamp(1));
        if n > 0 {
            assert!(matches!(
                cameras.push_stamped(2, frame(n - 1), stamp(2) - 33 * MS),
                CameraPush::Resolved
            ));
            assert_eq!(tick(&host), group(&[(0, n - 1), (1, n - 1), (2, n - 1)]));
        }
        assert_eq!(tick(&host), []);
    }
    let stats = cameras.stats();
    assert_eq!((stats.complete_groups, stats.frames), (5, 17));
    assert!(stats.max_skew <= 7 * MS);
    let last = stats.last_group.expect("group");
    assert!(last.complete && last.cameras == CameraSet::all(3));
}

#[test]
fn a_stalled_camera_times_out_into_the_partial_policy() {
    for partial in [
        PartialPolicy::Drop,
        PartialPolicy::TickPartial,
        PartialPolicy::HoldLast,
    ] {
        let (host, ms) = bridge();
        let cameras = MultiCamera::synchronized(&host, PORTS, config(partial));
        for camera in 0..3 {
            cameras.push_stamped(camera, frame(0), 0);
        }
        assert_eq!(tick(&host).len(), 3);
        // Camera 2 stalls.
        ms.store(33, Ordering::SeqCst);
        cameras.push_stamped(0, frame(1), 33 * MS);
        cameras.push_stamped(1, frame(1), 34 * MS);
        assert_eq!(
            cameras.next_deadline(),
            Some(host.clock().now() + config(partial).timeout)
        );
        assert_eq!(cameras.poll_timeout(), Some(Duration::from_millis(50)));
        ms.store(82, Ordering::SeqCst);
        assert!(!cameras.expire(), "not due yet");
        let waiter = host.inbound_waiter();
        ms.store(83, Ordering::SeqCst);
        assert!(cameras.expire());
        assert_eq!(cameras.next_deadline(), None);
        let stats = cameras.stats();
        assert_eq!(stats.timeouts, 1);
        match partial {
            PartialPolicy::Drop => {
                assert_eq!(waiter.poll_now(), None);
                assert_eq!(tick(&host), []);
                assert_eq!((stats.dropped_groups, stats.dropped_frames), (1, 2));
            }
            PartialPolicy::TickPartial => {
                assert_eq!(waiter.poll_now(), Some(InboundWait::Ready));
                assert_eq!(tick(&host), group(&[(0, 1), (1, 1)]));
                assert_eq!(stats.partial_groups, 1);
            }
            PartialPolicy::HoldLast => {
                assert_eq!(tick(&host), group(&[(0, 1), (1, 1), (2, 0)]));
                let last = stats.last_group.expect("group");
                assert_eq!((last.reused, last.complete), (CameraSet::only(2), false));
            }
        }
        // The stalled camera's late frame is dropped.
        assert!(matches!(
            cameras.push_stamped(2, frame(1), 35 * MS),
            CameraPush::Late
        ));
    }
}

#[test]
fn a_skipped_frame_resolves_without_waiting_for_the_timeout() {
    let (host, _) = bridge();
    let cameras = MultiCamera::synchronized(&host, ["a", "b"], config(PartialPolicy::TickPartial));
    cameras.push_stamped(0, frame(0), 0);
    // Camera 1 dropped frame 0 and delivers frame 1: frame 0's group cannot complete.
    assert!(matches!(
        cameras.push_stamped(1, frame(1), 33 * MS),
        CameraPush::Resolved
    ));
    let mut out = Vec::new();
    host.take_inbound_into(&mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(cameras.stats().timeouts, 0);
    cameras.push_stamped(0, frame(1), 34 * MS);
    host.take_inbound_into(&mut out);
    assert_eq!(out.len(), 3);
    assert_eq!(cameras.stats().complete_groups, 1);
}

#[test]
fn out_of_order_frames_of_one_camera_are_sorted() {
    let (host, _) = bridge();
    let cameras = MultiCamera::synchronized(&host, ["a", "b"], config(PartialPolicy::Drop));
    cameras.push_stamped(0, frame(1), 33 * MS);
    cameras.push_stamped(0, frame(0), 0);
    assert_eq!(cameras.buffered(0), 2);
    cameras.push_stamped(1, frame(0), MS);
    assert_eq!(tick(&host), [("a".into(), 0), ("b".into(), 0)]);
    cameras.push_stamped(1, frame(1), 32 * MS);
    assert_eq!(tick(&host), [("a".into(), 1), ("b".into(), 1)]);
}

#[test]
fn per_camera_offsets_correct_clock_skew() {
    let (host, _) = bridge();
    let cameras = MultiCamera::synchronized(&host, ["a", "b"], config(PartialPolicy::Drop));
    // Camera 1's clock runs 20 ms ahead: without an offset no group completes.
    cameras.push_stamped(0, frame(0), 100 * MS);
    cameras.push_stamped(1, frame(0), 120 * MS);
    assert_eq!(tick(&host), []);
    assert_eq!(cameras.stats().dropped_groups, 1);
    cameras.set_offset(1, -20 * MS as i64);
    for n in 1..4 {
        let at = 100 * MS + n as u64 * 33 * MS;
        cameras.push_stamped(1, frame(n), at + 20 * MS + MS);
        cameras.push_stamped(0, frame(n), at);
        assert_eq!(tick(&host), [("a".into(), n), ("b".into(), n)]);
    }
    assert_eq!(cameras.stats().max_skew, MS);
}

#[test]
fn full_buffers_resolve_the_oldest_group_and_unread_groups_are_superseded() {
    let (host, _) = bridge();
    let config = SyncConfig {
        max_buffered: 2,
        ..config(PartialPolicy::TickPartial)
    };
    let cameras = MultiCamera::synchronized(&host, ["a", "b"], config);
    // Camera 1 is silent; camera 0's frames pile up to the buffer bound.
    for n in 0..5_i64 {
        cameras.push_stamped(0, frame(n), n as u64 * 33 * MS);
        assert!(cameras.buffered(0) <= 2);
    }
    let stats = cameras.stats();
    assert_eq!(stats.overflows, 3);
    assert_eq!(stats.superseded_groups, 2, "no tick took groups 0 and 1");
    assert_eq!(
        host.pending_inbound(),
        1,
        "ports hold only the latest group"
    );
    assert_eq!(tick(&host), [("a".into(), 2)]);
}

#[test]
fn custom_stamp_functions_and_refusals() {
    let (host, _) = bridge();
    let config = SyncConfig {
        stamp: |payload| payload.get_ref::<i64>().map(|v| *v as u64),
        window: 0,
        ..config(PartialPolicy::Drop)
    };
    let cameras = MultiCamera::synchronized(&host, ["a", "b"], config);
    cameras.push(0, frame(7));
    cameras.push(1, frame(7));
    assert_eq!(tick(&host), [("a".into(), 7), ("b".into(), 7)]);
    assert!(matches!(
        cameras.push(0, Payload::owned("text", "x")),
        CameraPush::Unstamped
    ));
    assert!(matches!(
        cameras.push(5, frame(8)),
        CameraPush::UnknownCamera
    ));
    assert_eq!(frame_timestamp(&frame(1)), None, "not a daedalus:frame");
    host.close_input("a");
    assert!(matches!(cameras.push(0, frame(9)), CameraPush::Closed));
}

#[test]
fn independent_cameras_hold_the_latest_and_trigger_once_per_burst() {
    let (host, ms) = bridge();
    let cameras = MultiCamera::independent(
        &host,
        ["a", "b", "c"],
        IndependentConfig {
            trigger: Some(CameraSet::only(0).with(1)),
            max_age: Some(Duration::from_millis(100)),
        },
    );
    let waiter = host.inbound_waiter();
    cameras.push(2, frame(20));
    assert_eq!(waiter.poll_now(), None, "camera 2 does not trigger");
    for n in 0..5 {
        cameras.push(0, frame(n));
    }
    assert_eq!(waiter.poll_now(), Some(InboundWait::Ready));
    assert_eq!(host.pending_inbound(), 1, "a burst stays one pending tick");
    assert_eq!(tick(&host), [("a".into(), 4), ("c".into(), 20)]);
    assert!(!host.has_pending_inbound(), "taken: no stale re-trigger");
    ms.store(50, Ordering::SeqCst);
    cameras.push(1, frame(10));
    assert_eq!(
        tick(&host),
        [("a".into(), 4), ("b".into(), 10), ("c".into(), 20)]
    );
    assert_eq!(cameras.present(), CameraSet::all(3));
    // Cameras 0 and 2 stall past max_age; camera 1's frame is younger.
    assert_eq!(cameras.poll_timeout(), Some(Duration::from_millis(50)));
    ms.store(120, Ordering::SeqCst);
    assert!(cameras.expire());
    assert_eq!(cameras.present(), CameraSet::only(1));
    assert_eq!(tick(&host), [("b".into(), 10)]);
    let stats = cameras.stats();
    assert_eq!((stats.frames, stats.expired_frames), (7, 2));
    assert!(matches!(
        cameras.push(0, frame(5)),
        Some(FeedOutcome::Accepted { .. })
    ));
}
