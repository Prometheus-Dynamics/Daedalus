//! The only waits this backend makes, each with a bound, and the errors the driver reports
//! outside any call.
//!
//! Nothing waits while a region runs: a step polls. A wait here is for setup, shutdown, a
//! readback the host asked for, and tests. One that outlives its bound fails, and marks the
//! device lost so it is replaced.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

/// The bound on a wait unless the host sets another.
pub const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// The most uncaptured errors kept between two takes. Later ones are dropped.
const MAX_PENDING: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WaitError {
    /// The wait outlived its bound. The device is marked lost.
    TimedOut { what: &'static str, after: Duration },
    /// The device reported errors while the work ran. The text is theirs, joined.
    Device(String),
}

impl WaitError {
    pub fn is_timeout(&self) -> bool {
        matches!(self, WaitError::TimedOut { .. })
    }
}

impl fmt::Display for WaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WaitError::TimedOut { what, after } => {
                write!(f, "the device didn't finish {what} within {after:?}")
            }
            WaitError::Device(text) => f.write_str(text),
        }
    }
}

impl std::error::Error for WaitError {}

/// Errors the device reported outside any call that could return them. The handler records them,
/// and the next run on the device takes them and fails, so a node never uses output the driver
/// flagged. The flag keeps the common path to one atomic load.
#[derive(Default)]
pub(crate) struct PendingErrors {
    any: AtomicBool,
    messages: Mutex<Vec<String>>,
}

impl PendingErrors {
    pub(crate) fn record(&self, message: String) {
        let mut messages = self.messages.lock().unwrap_or_else(PoisonError::into_inner);
        if messages.len() < MAX_PENDING {
            messages.push(message);
        }
        self.any.store(true, Ordering::Release);
    }

    pub(crate) fn pending(&self) -> bool {
        self.any.load(Ordering::Acquire)
    }

    /// What was recorded, joined, and then forgotten. `None` while there is nothing.
    pub(crate) fn take(&self) -> Option<String> {
        if !self.any.load(Ordering::Acquire) {
            return None;
        }
        let mut messages = self.messages.lock().unwrap_or_else(PoisonError::into_inner);
        let joined = messages.join("; ");
        messages.clear();
        self.any.store(false, Ordering::Release);
        (!joined.is_empty()).then_some(joined)
    }
}

/// Waits for everything submitted on `device`, up to `bound`. `false` on a timeout.
pub(crate) fn wait_idle(device: &wgpu::Device, bound: Duration) -> bool {
    let poll = wgpu::PollType::Wait {
        submission_index: None,
        timeout: Some(bound),
    };
    device.poll(poll).is_ok()
}

/// The error an error scope caught, once the device has answered. Setup only.
pub(crate) fn scope_error(scope: wgpu::ErrorScopeGuard) -> Option<String> {
    pollster::block_on(scope.pop()).map(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{DEFAULT_WAIT_TIMEOUT, PendingErrors, WaitError};

    #[test]
    fn the_bound_defaults_to_five_seconds() {
        assert_eq!(DEFAULT_WAIT_TIMEOUT, Duration::from_secs(5));
        assert_eq!(crate::Options::default().wait_timeout, DEFAULT_WAIT_TIMEOUT);
    }

    #[test]
    fn a_timed_out_wait_is_recognised_and_other_errors_are_not() {
        let timed_out = WaitError::TimedOut {
            what: "a readback",
            after: DEFAULT_WAIT_TIMEOUT,
        };
        assert!(timed_out.is_timeout());
        assert_eq!(
            timed_out.to_string(),
            "the device didn't finish a readback within 5s"
        );
        assert!(!WaitError::Device("validation".into()).is_timeout());
    }

    #[test]
    fn uncaptured_errors_are_returned_once_by_the_next_take() {
        let pending = PendingErrors::default();
        assert_eq!(pending.take(), None);
        pending.record("validation: a".into());
        pending.record("out of memory".into());
        assert_eq!(
            pending.take().as_deref(),
            Some("validation: a; out of memory")
        );
        assert_eq!(pending.take(), None);
        for at in 0..40 {
            pending.record(format!("{at}"));
        }
        assert_eq!(
            pending.take().map(|text| text.split("; ").count()),
            Some(16)
        );
    }
}
