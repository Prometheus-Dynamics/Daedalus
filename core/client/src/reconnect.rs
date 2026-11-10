//! The delay between connection attempts: it doubles from a quarter of a second and stops
//! growing at 8 s. Nothing here ever gives up.

use std::time::Duration;

/// The first delay after a failed attempt.
pub const BACKOFF_START: Duration = Duration::from_millis(250);
/// The longest delay between two attempts.
pub const BACKOFF_CAP: Duration = Duration::from_secs(8);

/// How long to wait before the next attempt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Backoff {
    failures: u32,
}

impl Backoff {
    pub fn new() -> Backoff {
        Backoff::default()
    }

    /// The delay after `failures` failed attempts in a row.
    pub fn delay_for(failures: u32) -> Duration {
        let doublings = failures.saturating_sub(1).min(16);
        (BACKOFF_START * (1 << doublings)).min(BACKOFF_CAP)
    }

    /// Counts a failed attempt and gives the delay before the next one.
    pub fn failed(&mut self) -> Duration {
        self.failures = self.failures.saturating_add(1);
        Backoff::delay_for(self.failures)
    }

    pub fn succeeded(&mut self) {
        self.failures = 0;
    }

    pub fn failures(&self) -> u32 {
        self.failures
    }
}

/// What the client waits on between attempts. A test supplies its own, so no test sleeps.
pub trait Clock: Send + Sync {
    fn sleep(&self, delay: Duration);
}

/// The real clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn sleep(&self, delay: Duration) {
        std::thread::sleep(delay);
    }
}
