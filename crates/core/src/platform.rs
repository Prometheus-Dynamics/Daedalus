//! What the build target provides, and the portable fallbacks for what it lacks.
//!
//! Threads are a Cargo feature of `daedalus-runtime`/`daedalus-engine` (`threads`), not a
//! target probe.
//!
//! - [`Instant`]: `std::time::Instant` wherever the target has a monotonic OS clock (the same
//!   type, so native builds are unaffected). Elsewhere (`no_std`, `wasm32-unknown-unknown`) it is
//!   a portable instant read from the clock installed with `set_clock`; until one is
//!   installed every instant is zero, so optional timing reads zero durations.
//! - [`Clock`]: where runtime and engine timing reads [`Instant`]s. The default is the platform
//!   clock above; [`Clock::new`] injects another one per engine/executor (tests, simulated
//!   time, a target timer). `set_clock` remains only as the process-wide fallback for
//!   timestamps taken without an engine at hand: payload lineage and host-bridge events.
//! - [`Arc`]: the `Arc` in Daedalus signatures. `alloc::sync::Arc` wherever the target has
//!   pointer-sized compare-and-swap; elsewhere (`thumbv6m`, `riscv32imc`, which have no
//!   `alloc::sync`) `portable_atomic_util::Arc`.
//!
//! See "Portability" in docs/development.md.

#[cfg(target_has_atomic = "ptr")]
pub use alloc::sync::Arc;
#[cfg(not(target_has_atomic = "ptr"))]
pub use portable_atomic_util::Arc;

/// Whether [`Instant`] reads a monotonic OS clock (otherwise the `set_clock` clock).
pub const OS_CLOCK: bool = cfg!(all(
    feature = "std",
    not(all(target_family = "wasm", target_os = "unknown"))
));

#[cfg(all(
    feature = "std",
    not(all(target_family = "wasm", target_os = "unknown"))
))]
pub use std::time::Instant;

#[cfg(not(all(
    feature = "std",
    not(all(target_family = "wasm", target_os = "unknown"))
)))]
pub use fallback::{Instant, set_clock};

#[cfg(not(all(
    feature = "std",
    not(all(target_family = "wasm", target_os = "unknown"))
)))]
mod fallback {
    use core::ops::{Add, AddAssign, Sub, SubAssign};
    use core::time::Duration;

    static CLOCK: spin::Once<fn() -> Duration> = spin::Once::new();

    /// Install the process-wide clock behind [`Instant::now`] (and so [`super::Clock::default`]):
    /// time since an arbitrary, fixed origin (e.g. a hardware timer or `performance.now()`).
    /// Only the first call takes effect.
    ///
    /// Engines take their own clock (`EngineConfig::with_clock`); this fallback is what payload
    /// lineage and host-bridge event timestamps, taken without an engine at hand, read.
    pub fn set_clock(now: fn() -> Duration) {
        CLOCK.call_once(|| now);
    }

    /// Portable monotonic instant (the `std::time::Instant` subset Daedalus uses), read from the
    /// clock installed with [`set_clock`].
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Instant(pub(super) Duration);

    impl Instant {
        pub fn now() -> Self {
            Self(CLOCK.get().map_or(Duration::ZERO, |now| now()))
        }

        pub fn elapsed(&self) -> Duration {
            Self::now().saturating_duration_since(*self)
        }

        pub fn duration_since(&self, earlier: Self) -> Duration {
            self.saturating_duration_since(earlier)
        }

        pub fn saturating_duration_since(&self, earlier: Self) -> Duration {
            self.0.saturating_sub(earlier.0)
        }

        pub fn checked_duration_since(&self, earlier: Self) -> Option<Duration> {
            self.0.checked_sub(earlier.0)
        }

        pub fn checked_add(&self, duration: Duration) -> Option<Self> {
            self.0.checked_add(duration).map(Self)
        }

        pub fn checked_sub(&self, duration: Duration) -> Option<Self> {
            self.0.checked_sub(duration).map(Self)
        }
    }

    impl Add<Duration> for Instant {
        type Output = Self;

        fn add(self, duration: Duration) -> Self {
            Self(self.0 + duration)
        }
    }

    impl AddAssign<Duration> for Instant {
        fn add_assign(&mut self, duration: Duration) {
            self.0 += duration;
        }
    }

    impl Sub<Duration> for Instant {
        type Output = Self;

        fn sub(self, duration: Duration) -> Self {
            Self(self.0 - duration)
        }
    }

    impl SubAssign<Duration> for Instant {
        fn sub_assign(&mut self, duration: Duration) {
            self.0 -= duration;
        }
    }

    impl Sub for Instant {
        type Output = Duration;

        fn sub(self, earlier: Self) -> Duration {
            self.saturating_duration_since(earlier)
        }
    }
}

use core::time::Duration;

/// A monotonic clock: the platform clock ([`Clock::default`]) or a custom one ([`Clock::new`]).
///
/// The default reads [`Instant::now`] directly, so native timing is unchanged. A custom clock is
/// a function returning the time since an arbitrary fixed origin; its readings are [`Instant`]s
/// too, comparable with each other but not with readings of another clock. Measure with
/// [`Clock::elapsed`], not `Instant::elapsed` (which reads the platform clock).
#[derive(Clone, Default)]
pub struct Clock(Option<crate::portable::Arc<CustomClock<dyn Fn() -> Duration + Send + Sync>>>);

struct CustomClock<F: ?Sized> {
    /// Platform instant that custom readings are offset from (a std `Instant` cannot be built
    /// from a `Duration`).
    #[cfg(all(
        feature = "std",
        not(all(target_family = "wasm", target_os = "unknown"))
    ))]
    origin: Instant,
    now: F,
}

impl Clock {
    /// A clock reading `now`: the time since an arbitrary, fixed origin (e.g. a hardware timer,
    /// `performance.now()`, or a simulated clock in tests).
    pub fn new(now: impl Fn() -> Duration + Send + Sync + 'static) -> Self {
        Self(Some(crate::portable::arc_dyn!(CustomClock {
            #[cfg(all(
                feature = "std",
                not(all(target_family = "wasm", target_os = "unknown"))
            ))]
            origin: Instant::now(),
            now,
        })))
    }

    /// Whether this is the platform clock.
    pub fn is_platform(&self) -> bool {
        self.0.is_none()
    }

    /// The current instant on this clock.
    #[inline]
    pub fn now(&self) -> Instant {
        let Some(custom) = &self.0 else {
            return Instant::now();
        };
        let since_origin = (custom.now)();
        #[cfg(all(
            feature = "std",
            not(all(target_family = "wasm", target_os = "unknown"))
        ))]
        return custom.origin + since_origin;
        #[cfg(not(all(
            feature = "std",
            not(all(target_family = "wasm", target_os = "unknown"))
        )))]
        return Instant(since_origin);
    }

    /// Time since `earlier` (a reading of this clock); zero if the clock went backwards.
    #[inline]
    pub fn elapsed(&self, earlier: Instant) -> Duration {
        self.now().saturating_duration_since(earlier)
    }
}

/// Clocks compare by identity: both the platform clock, or the same custom clock.
impl PartialEq for Clock {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (None, None) => true,
            (Some(a), Some(b)) => crate::portable::Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Eq for Clock {}

impl core::fmt::Debug for Clock {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(if self.is_platform() {
            "Clock::Platform"
        } else {
            "Clock::Custom"
        })
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use core::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    #[test]
    fn custom_clock_drives_readings() {
        let ticks = Arc::new(AtomicU64::new(5));
        let clock = Clock::new({
            let ticks = ticks.clone();
            move || Duration::from_millis(ticks.load(Ordering::Relaxed))
        });
        let start = clock.now();
        ticks.store(30, Ordering::Relaxed);
        assert_eq!(clock.elapsed(start), Duration::from_millis(25));
        assert_eq!(clock.now() - start, Duration::from_millis(25));
        assert!(!clock.is_platform());
        assert_eq!(clock, clock.clone());
        assert_ne!(clock, Clock::default());
        assert!(Clock::default().is_platform());
    }
}
