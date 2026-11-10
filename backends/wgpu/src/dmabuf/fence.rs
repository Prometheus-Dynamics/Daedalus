//! Waiting for a producer to finish writing before the device reads.
//!
//! A producer that is still writing gives a fence with its image: a sync_file that signals when
//! the writes are done. Where the device can take a sync_file as a semaphore, the import's first
//! submit waits on it and the host waits for nothing. Elsewhere the host waits here, with a
//! bound. A fence that has signaled already costs neither.

use std::time::Duration;

use super::layout::ImportError;

/// The bound on a host wait for a producer's fence, unless the image says otherwise.
pub const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(1);

/// How an unsignaled fence is waited for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FenceWait {
    /// The device's submit waits on it, as a semaphore. The host doesn't wait at all.
    SyncFd,
    /// The host waits before importing, up to the image's timeout.
    Cpu,
}

impl FenceWait {
    /// What a device does with a fence: its own wait where it can import a sync_file, and the
    /// host's otherwise.
    pub const fn resolve(device_imports_sync_fd: bool) -> FenceWait {
        if device_imports_sync_fd {
            FenceWait::SyncFd
        } else {
            FenceWait::Cpu
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            FenceWait::SyncFd => "sync-fd",
            FenceWait::Cpu => "cpu",
        }
    }
}

/// What happened to a fence before an import.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Acquired {
    /// There was no fence, or it had signaled already.
    Ready,
    /// The host waited, and it signaled.
    Waited,
    /// It is still pending, and the device's first submit waits on it.
    OnDevice,
}

/// Decides what to do about a fence, given whether it has signaled and how this device waits.
/// `wait` is the host's bounded wait, called only when the host has to: it says whether the
/// fence signaled in time.
pub fn acquire(
    signaled: Option<bool>,
    how: FenceWait,
    timeout: Duration,
    wait: impl FnOnce(Duration) -> bool,
) -> Result<Acquired, ImportError> {
    match (signaled, how) {
        (None | Some(true), _) => Ok(Acquired::Ready),
        (Some(false), FenceWait::SyncFd) => Ok(Acquired::OnDevice),
        (Some(false), FenceWait::Cpu) => {
            if wait(timeout) {
                Ok(Acquired::Waited)
            } else {
                Err(ImportError::FenceTimeout(timeout))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Acquired, DEFAULT_ACQUIRE_TIMEOUT, FenceWait, acquire};
    use crate::dmabuf::layout::ImportError;

    #[test]
    fn acquire_fence_modes_resolve_per_device() {
        assert_eq!(FenceWait::resolve(true), FenceWait::SyncFd);
        assert_eq!(FenceWait::resolve(false), FenceWait::Cpu);
        assert_eq!(
            (FenceWait::SyncFd.as_str(), FenceWait::Cpu.as_str()),
            ("sync-fd", "cpu")
        );
        assert_eq!(DEFAULT_ACQUIRE_TIMEOUT, Duration::from_secs(1));
    }

    #[test]
    fn acquire_fence_wait_outcomes() {
        let never = |_: Duration| -> bool { panic!("nothing to wait for") };
        let second = Duration::from_secs(1);
        // No fence, or one that has signaled: no wait anywhere.
        for how in [FenceWait::SyncFd, FenceWait::Cpu] {
            assert_eq!(acquire(None, how, second, never), Ok(Acquired::Ready));
            assert_eq!(acquire(Some(true), how, second, never), Ok(Acquired::Ready));
        }
        // Pending, on a device that waits itself: the host doesn't.
        assert_eq!(
            acquire(Some(false), FenceWait::SyncFd, second, never),
            Ok(Acquired::OnDevice)
        );
        // Pending, with the host waiting: it gets the image's own timeout, and a fence that
        // doesn't signal in time is an error that says how long was waited.
        let waited = acquire(
            Some(false),
            FenceWait::Cpu,
            Duration::from_millis(7),
            |timeout| {
                assert_eq!(timeout, Duration::from_millis(7));
                true
            },
        );
        assert_eq!(waited, Ok(Acquired::Waited));
        let late = acquire(Some(false), FenceWait::Cpu, second, |_| false);
        assert_eq!(late, Err(ImportError::FenceTimeout(second)));
        assert_eq!(
            late.unwrap_err().to_string(),
            "the producer's fence didn't signal within 1s"
        );
    }
}
