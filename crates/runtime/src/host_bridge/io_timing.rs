//! Host-side feed and take timing for frame-overhead reports (`HostGraph::frame_overhead`).
//!
//! Off by default: each feed or take then costs one relaxed load. Timings read the platform
//! clock.

use core::sync::atomic::Ordering::Relaxed;
use core::time::Duration;

use daedalus_core::platform::Instant;

use super::HostBridgeHandle;
use crate::portable::{AtomicBool, AtomicU64};

#[derive(Default)]
pub(crate) struct HostIoTiming {
    enabled: AtomicBool,
    push_ns: AtomicU64,
    take_ns: AtomicU64,
}

/// Host-bridge time spent feeding and taking payloads (see [`HostBridgeHandle::take_io_time`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HostIoTime {
    /// Feeds: `feed_payload`, `push*`.
    pub push: Duration,
    /// Takes: `try_pop*`, `drain*`, plus [`HostBridgeHandle::record_take_time`].
    pub take: Duration,
}

impl HostIoTiming {
    /// Run a feed, timing it when enabled.
    #[inline]
    pub(super) fn timed_push<T>(&self, feed: impl FnOnce() -> T) -> T {
        self.timed(&self.push_ns, feed)
    }

    /// Run a take, timing it when enabled.
    #[inline]
    pub(super) fn timed_take<T>(&self, take: impl FnOnce() -> T) -> T {
        self.timed(&self.take_ns, take)
    }

    #[inline]
    fn timed<T>(&self, slot: &AtomicU64, run: impl FnOnce() -> T) -> T {
        #[cfg(feature = "alloc-probe")]
        let _scope = crate::alloc_probe::enter(crate::alloc_probe::AllocScope::Host);
        if !self.enabled.load(Relaxed) {
            return run();
        }
        let start = Instant::now();
        let result = run();
        slot.fetch_add(start.elapsed().as_nanos() as u64, Relaxed);
        result
    }
}

impl HostBridgeHandle {
    /// Time feeds and takes on this bridge (`HostGraph::enable_frame_overhead` turns it on).
    pub fn set_io_timing(&self, enabled: bool) {
        let timing = &self.shared.io_timing;
        timing.enabled.store(enabled, Relaxed);
        timing.push_ns.store(0, Relaxed);
        timing.take_ns.store(0, Relaxed);
    }

    pub fn io_timing_enabled(&self) -> bool {
        self.shared.io_timing.enabled.load(Relaxed)
    }

    /// Feed and take time since the previous call (or since timing was enabled), resetting both.
    pub fn take_io_time(&self) -> HostIoTime {
        let timing = &self.shared.io_timing;
        HostIoTime {
            push: Duration::from_nanos(timing.push_ns.swap(0, Relaxed)),
            take: Duration::from_nanos(timing.take_ns.swap(0, Relaxed)),
        }
    }

    /// Take time recorded since the previous [`Self::take_io_time`], without resetting it.
    pub fn pending_take_time(&self) -> Duration {
        Duration::from_nanos(self.shared.io_timing.take_ns.load(Relaxed))
    }

    /// Count host-side output handling done outside the bridge (payload inspection, ...) as take
    /// time; ignored while timing is off.
    pub fn record_take_time(&self, duration: Duration) {
        let timing = &self.shared.io_timing;
        if timing.enabled.load(Relaxed) {
            timing
                .take_ns
                .fetch_add(duration.as_nanos() as u64, Relaxed);
        }
    }
}
