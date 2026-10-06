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
    pub(super) push_ns: AtomicU64,
    pub(super) take_ns: AtomicU64,
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
    #[inline]
    pub(super) fn enabled(&self) -> bool {
        self.enabled.load(Relaxed)
    }
}

/// Run a feed or take and add its duration to `slot`. Out of line: only called while timing is
/// on, so the untimed paths stay as they were.
#[cold]
#[inline(never)]
pub(super) fn timed<T>(slot: &AtomicU64, run: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let result = run();
    slot.fetch_add(start.elapsed().as_nanos() as u64, Relaxed);
    result
}

/// Attribute this thread's allocations to host-bridge calls until the guard drops
/// (`alloc-probe`; nothing otherwise).
#[inline]
pub(super) fn host_alloc_scope() -> impl Sized {
    #[cfg(feature = "alloc-probe")]
    {
        crate::alloc_probe::enter(crate::alloc_probe::AllocScope::Host)
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
