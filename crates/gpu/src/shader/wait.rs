//! Bounded GPU waits, and the errors the waits and the driver's uncaptured errors turn into. A
//! wait that outlives [`gpu_wait_timeout`] fails its dispatch as a node error (and marks the
//! context lost, so its source replaces it under the backoff) instead of hanging the node.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::Mutex;

use crate::GpuError;

/// The text every timed-out wait's error carries (see [`is_timeout`]).
const TIMED_OUT: &str = "gpu wait timed out";

/// The default bound on a GPU wait.
pub(crate) const DEFAULT_GPU_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// The bound on GPU waits: [`DEFAULT_GPU_WAIT_TIMEOUT`], or `DAEDALUS_GPU_WAIT_TIMEOUT_MS`
/// (milliseconds, at least 1) read once, at the first wait.
pub(crate) fn gpu_wait_timeout() -> Duration {
    static CONFIGURED: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *CONFIGURED.get_or_init(|| {
        std::env::var("DAEDALUS_GPU_WAIT_TIMEOUT_MS")
            .ok()
            .and_then(|ms| ms.trim().parse::<u64>().ok())
            .map_or(DEFAULT_GPU_WAIT_TIMEOUT, |ms| {
                Duration::from_millis(ms.max(1))
            })
    })
}

/// The error for a wait that outlived the bound while waiting for `what`.
pub(crate) fn timed_out(what: &str) -> GpuError {
    GpuError::Internal(format!(
        "{TIMED_OUT} ({what}) after {:?}",
        gpu_wait_timeout()
    ))
}

/// Whether `err` is a timed-out wait (the context is then treated as lost).
pub(crate) fn is_timeout(err: &GpuError) -> bool {
    matches!(err, GpuError::Internal(message) if message.starts_with(TIMED_OUT))
}

/// The most uncaptured errors kept between two dispatches; later ones are counted, not kept.
const MAX_PENDING: usize = 16;

/// Uncaptured wgpu errors, recorded by the device's handler and returned by the next dispatch on
/// that device. The flag keeps the dispatch path to one atomic load while there are none.
#[derive(Default)]
pub(crate) struct PendingErrors {
    any: AtomicBool,
    messages: Mutex<Vec<String>>,
}

impl PendingErrors {
    pub(crate) fn record(&self, message: String) {
        let mut messages = self.messages.lock();
        if messages.len() < MAX_PENDING {
            messages.push(message);
        }
        self.any.store(true, Ordering::Release);
    }

    /// The recorded messages, joined, and cleared; `None` while there are none.
    pub(crate) fn take(&self) -> Option<String> {
        if !self.any.load(Ordering::Acquire) {
            return None;
        }
        let mut messages = self.messages.lock();
        let joined = messages.join("; ");
        messages.clear();
        self.any.store(false, Ordering::Release);
        (!joined.is_empty()).then_some(joined)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bound_defaults_to_five_seconds() {
        assert_eq!(gpu_wait_timeout(), DEFAULT_GPU_WAIT_TIMEOUT);
    }

    #[test]
    fn a_timed_out_wait_is_recognised_and_other_errors_are_not() {
        assert!(is_timeout(&timed_out("throttle")));
        assert!(!is_timeout(&GpuError::Internal(
            "submission poll failed".into()
        )));
        assert!(!is_timeout(&GpuError::NoContext));
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
    }
}
