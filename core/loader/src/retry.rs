//! Retrying a load. The backoff starts at 250 ms, doubles, and never exceeds 8 s. It never stops
//! on its own: only a stop flag set by hand ends it.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::{LoadError, Loader, Plugin};

/// The first delay after a failure.
pub(crate) const BASE: Duration = Duration::from_millis(250);

/// The longest delay between two attempts.
pub(crate) const CAP: Duration = Duration::from_secs(8);

/// Counts the failures of one load, and says how long to wait before the next attempt.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Retry {
    failures: u32,
}

impl Retry {
    pub fn new() -> Retry {
        Retry::default()
    }

    /// The delay after `failures` failures in a row: `BASE` doubled per failure, at most `CAP`.
    pub fn delay_for(failures: u32) -> Duration {
        let mut delay = BASE;
        for _ in 0..failures.min(16) {
            delay = (delay * 2).min(CAP);
        }
        delay.min(CAP)
    }

    /// Records a failure and returns the delay before the next attempt.
    pub fn failed(&mut self) -> Duration {
        let delay = Self::delay_for(self.failures);
        self.failures = self.failures.saturating_add(1);
        delay
    }

    /// Clears the count after a load works.
    pub fn succeeded(&mut self) {
        self.failures = 0;
    }

    pub fn failures(&self) -> u32 {
        self.failures
    }
}

/// Loads `path`, retrying with backoff until it loads or `stop` is set. `sleep` receives each
/// delay; the caller decides how to wait. `stop` is checked before every attempt, and setting it
/// is the only way to end the retries.
pub fn load_with_retry(
    loader: &mut Loader,
    path: &Path,
    stop: &AtomicBool,
    retry: &mut Retry,
    mut sleep: impl FnMut(Duration),
) -> Result<Plugin, LoadError> {
    loop {
        if stop.load(Ordering::SeqCst) {
            return Err(LoadError::Stopped {
                path: path.to_path_buf(),
            });
        }
        match loader.open(path) {
            Ok(plugin) => {
                retry.succeeded();
                return Ok(plugin);
            }
            Err(_) => sleep(retry.failed()),
        }
    }
}
