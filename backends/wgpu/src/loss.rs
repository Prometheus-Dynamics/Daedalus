//! What happens after a device is lost: when to try opening one again.
//!
//! The failure rule: never give up, wait a little longer after each failure up to a cap, and
//! never turn the backend off for failing. Turning it off is something the host does on purpose.

use std::time::{Duration, Instant};

/// The wait after the first failed attempt to open a device.
pub const RETRY_FIRST: Duration = Duration::from_millis(250);
/// The longest wait between attempts, however many have failed.
pub const RETRY_CAP: Duration = Duration::from_secs(8);

/// How long to wait after `failures` failed attempts in a row before the next one: nothing
/// before the first failure, then 250 ms doubling, never more than 8 s.
pub fn retry_delay(failures: u32) -> Duration {
    match failures {
        0 => Duration::ZERO,
        // 250 ms << 5 is the cap already, so nothing here can overflow.
        1..=5 => RETRY_FIRST * (1 << (failures - 1)),
        _ => RETRY_CAP,
    }
    .min(RETRY_CAP)
}

/// The attempts to open a device since the last one that worked.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Retry {
    failures: u32,
    not_before: Option<Instant>,
}

impl Retry {
    /// How long until the next attempt is due. `None` when it is due now.
    pub(crate) fn wait(&self, now: Instant) -> Option<Duration> {
        let left = self.not_before?.saturating_duration_since(now);
        (!left.is_zero()).then_some(left)
    }

    pub(crate) fn failed(&mut self, now: Instant) {
        self.failures = self.failures.saturating_add(1);
        self.not_before = Some(now + retry_delay(self.failures));
    }

    pub(crate) fn worked(&mut self) {
        *self = Retry::default();
    }

    #[cfg(test)]
    pub(crate) fn failures(&self) -> u32 {
        self.failures
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{RETRY_CAP, RETRY_FIRST, Retry, retry_delay};

    #[test]
    fn the_delay_doubles_from_250_ms_and_stops_at_eight_seconds() {
        let delays: Vec<u64> = (0..8).map(|n| retry_delay(n).as_millis() as u64).collect();
        assert_eq!(delays, [0, 250, 500, 1000, 2000, 4000, 8000, 8000]);
        for failures in 0..=10_000 {
            assert!(retry_delay(failures) <= RETRY_CAP, "{failures}");
        }
        assert_eq!(retry_delay(u32::MAX), RETRY_CAP);
        assert_eq!(
            (RETRY_FIRST, RETRY_CAP),
            (Duration::from_millis(250), Duration::from_secs(8))
        );
    }

    #[test]
    fn after_a_hundred_failures_the_delay_is_the_cap_and_it_still_retries() {
        let mut retry = Retry::default();
        let mut now = Instant::now();
        assert_eq!(retry.wait(now), None, "the first attempt is due at once");
        for _ in 0..100 {
            retry.failed(now);
            let wait = retry.wait(now).expect("a wait after a failure");
            assert!(wait <= RETRY_CAP);
            // Not due a moment before, due when the wait has passed. There is no state in
            // which it stops being due: nothing turns itself off.
            assert!(retry.wait(now + wait - Duration::from_millis(1)).is_some());
            now += wait;
            assert_eq!(retry.wait(now), None);
        }
        assert_eq!(retry.failures(), 100);
        retry.failed(now);
        assert_eq!(retry.wait(now), Some(RETRY_CAP));
        retry.worked();
        assert_eq!(
            (retry.failures(), retry.wait(now)),
            (0, None),
            "one success starts over"
        );
    }
}
