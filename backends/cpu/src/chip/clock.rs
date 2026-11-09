//! Where a graph reads the time from.

/// A source of time for `tick_with`.
pub trait Clock {
    fn now_ns(&self) -> u64;
}
