//! The wait between a worker's failure and its next start: 100 ms, doubled per failure, capped at
//! 8 s. Nothing here gives up.

use std::time::Duration;

pub const BASE_DELAY: Duration = Duration::from_millis(100);
pub const MAX_DELAY: Duration = Duration::from_secs(8);

/// The wait after `failures` failures in a row.
pub fn delay_after(failures: u32) -> Duration {
    let mut delay = BASE_DELAY;
    for _ in 0..failures.saturating_sub(1).min(16) {
        delay = (delay * 2).min(MAX_DELAY);
    }
    delay.min(MAX_DELAY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wait_starts_at_100_ms_doubles_per_failure_and_stops_at_8_s() {
        let ms = |n| Duration::from_millis(n);
        assert_eq!(BASE_DELAY, ms(100));
        assert_eq!(MAX_DELAY, ms(8000));
        let waits: Vec<Duration> = (0..=9).map(delay_after).collect();
        assert_eq!(
            waits,
            [100, 100, 200, 400, 800, 1600, 3200, 6400, 8000, 8000].map(ms)
        );
        assert_eq!(delay_after(u32::MAX), MAX_DELAY);
    }
}
