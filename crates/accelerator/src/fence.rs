use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::AcceleratorError;

/// Completion of a submission.
pub trait Fence: Send + Sync {
    /// Whether the work is done (or failed); never blocks.
    fn is_signaled(&self) -> bool;

    /// Block until the work is done, or `timeout` passes ([`AcceleratorError::Timeout`]). A
    /// failed submission returns its error here.
    fn wait(&self, timeout: Option<Duration>) -> Result<(), AcceleratorError>;

    /// A `sync_file` that signals with this fence, for a GPU or another accelerator to wait on
    /// without a CPU round trip; `None` if the accelerator cannot export one.
    #[cfg(unix)]
    fn export_sync_fd(&self) -> Option<std::os::fd::OwnedFd> {
        None
    }
}

/// A shared fence, as [`crate::Accelerator::submit`] returns it.
pub type FenceHandle = Arc<dyn Fence>;

/// Something a submission waits for before it runs.
pub enum Wait {
    /// A fence of this or another accelerator.
    Fence(FenceHandle),
    /// A `sync_file` from another device (a GPU submission, a camera's buffer fence).
    #[cfg(unix)]
    SyncFd(std::os::fd::OwnedFd),
}

impl Wait {
    /// Wait on the CPU: for accelerators that cannot wait on the device. A `SyncFd` without a way
    /// to poll it here is [`AcceleratorError::Unsupported`].
    pub fn wait_on_cpu(&self, timeout: Option<Duration>) -> Result<(), AcceleratorError> {
        match self {
            Wait::Fence(fence) => fence.wait(timeout),
            #[cfg(unix)]
            Wait::SyncFd(_) => Err(AcceleratorError::unsupported("a CPU wait on a sync_file")),
        }
    }
}

impl core::fmt::Debug for Wait {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Wait::Fence(fence) => f
                .debug_struct("Fence")
                .field("signaled", &fence.is_signaled())
                .finish(),
            #[cfg(unix)]
            Wait::SyncFd(fd) => f.debug_tuple("SyncFd").field(fd).finish(),
        }
    }
}

/// A fence that is already signaled (work that completed inside `submit`).
#[derive(Clone, Copy, Debug, Default)]
pub struct SignaledFence;

impl Fence for SignaledFence {
    fn is_signaled(&self) -> bool {
        true
    }

    fn wait(&self, _timeout: Option<Duration>) -> Result<(), AcceleratorError> {
        Ok(())
    }
}

/// A fence an accelerator signals from its completion thread or interrupt handler.
#[derive(Debug, Default)]
pub struct CpuFence {
    state: Mutex<Option<Result<(), AcceleratorError>>>,
    signaled: Condvar,
}

impl CpuFence {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// Complete the fence with the work's outcome; later calls are ignored.
    pub fn signal(&self, outcome: Result<(), AcceleratorError>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.is_none() {
            *state = Some(outcome);
            self.signaled.notify_all();
        }
    }
}

impl Fence for CpuFence {
    fn is_signaled(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }

    fn wait(&self, timeout: Option<Duration>) -> Result<(), AcceleratorError> {
        let deadline = timeout.map(|t| (t, Instant::now() + t));
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if let Some(outcome) = state.as_ref() {
                return outcome.clone();
            }
            state = match deadline {
                None => self.signaled.wait(state).unwrap_or_else(|e| e.into_inner()),
                Some((after, at)) => {
                    let left = at.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Err(AcceleratorError::Timeout { after });
                    }
                    self.signaled
                        .wait_timeout(state, left)
                        .unwrap_or_else(|e| e.into_inner())
                        .0
                }
            };
        }
    }
}
