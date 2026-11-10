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
