//! What the build target provides, and the portable fallbacks for what it lacks.
//!
//! - [`THREADS`]: whether `std::thread` works. Where it does not (`wasm32-unknown-unknown`,
//!   `wasm32-wasip1`, `no_std`), executors run `Parallel`/`Adaptive` serially and blocking
//!   waits return at once.
//! - [`Instant`]: `std::time::Instant` wherever the target has a monotonic OS clock (the same
//!   type, so native builds are unaffected). Elsewhere (`no_std`, `wasm32-unknown-unknown`) it is
//!   a portable instant read from the clock installed with `set_clock`; until one is
//!   installed every instant is zero, so optional timing reads zero durations.
//!
//! See "Portability" in docs/development.md.

/// Whether `std::thread` can spawn threads on this target.
pub const THREADS: bool = cfg!(all(
    feature = "std",
    not(all(
        target_family = "wasm",
        any(target_os = "unknown", not(target_feature = "atomics"))
    ))
));

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

    /// Install the monotonic clock behind [`Instant::now`]: time since an arbitrary, fixed
    /// origin (e.g. a hardware timer or `performance.now()`). Only the first call takes effect.
    pub fn set_clock(now: fn() -> Duration) {
        CLOCK.call_once(|| now);
    }

    /// Portable monotonic instant (the `std::time::Instant` subset Daedalus uses), read from the
    /// clock installed with [`set_clock`].
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Instant(Duration);

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
