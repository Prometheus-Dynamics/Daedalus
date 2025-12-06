use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Monotonic logical tick for deterministic ordering.
///
/// ```
/// use daedalus_core::clock::Tick;
/// let t = Tick::new(7);
/// assert_eq!(t.value(), 7);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Tick(u64);

impl Tick {
    pub const ZERO: Tick = Tick(0);

    pub fn new(value: u64) -> Self {
        Tick(value)
    }

    pub fn value(self) -> u64 {
        self.0
    }

    pub fn increment(self) -> Self {
        Tick(self.0.saturating_add(1))
    }
}

/// Deterministic logical clock that can be manually advanced.
///
/// ```
/// use daedalus_core::clock::TickClock;
/// let clock = TickClock::default();
/// let t1 = clock.tick();
/// let t2 = clock.advance(4);
/// assert!(t2.value() > t1.value());
/// ```
#[derive(Debug)]
pub struct TickClock {
    current: AtomicU64,
}

impl TickClock {
    pub fn new(start: Tick) -> Self {
        Self {
            current: AtomicU64::new(start.value()),
        }
    }

    pub fn default_start() -> Self {
        Self::new(Tick::ZERO)
    }

    /// Returns the current tick without advancing.
    pub fn now_tick(&self) -> Tick {
        Tick(self.current.load(Ordering::Relaxed))
    }

    /// Advances the clock by `delta` and returns the new tick.
    pub fn advance(&self, delta: u64) -> Tick {
        let next = self.current.fetch_add(delta, Ordering::Relaxed) + delta;
        Tick(next)
    }

    /// Advance by one tick.
    pub fn tick(&self) -> Tick {
        self.advance(1)
    }
}

impl Default for TickClock {
    fn default() -> Self {
        Self::default_start()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_ticks() {
        let clock = TickClock::default();
        assert_eq!(clock.now_tick(), Tick::ZERO);
        let t1 = clock.tick();
        let t2 = clock.tick();
        assert!(t2.value() > t1.value());
    }
}
