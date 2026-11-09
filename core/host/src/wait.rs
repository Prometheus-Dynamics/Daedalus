//! Waiting for input without running a tick: a future any runtime can poll, a blocking wait, and
//! the readiness behind the eventfd. Wakers are stored under the lock and woken after it is
//! released.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use std::vec::Vec;

/// Why a wait ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InboundWait {
    /// Input is pending.
    Ready,
    /// The timeout passed with nothing pending.
    TimedOut,
    /// The inputs closed and nothing is pending.
    Closed,
    /// A stop handle asked the waiter to stop.
    Stopped,
}

#[derive(Default)]
struct State {
    pending: bool,
    closed: bool,
    stopped: bool,
    wakers: Vec<Waker>,
}

/// Readiness shared by every feed and every waiter.
#[derive(Default)]
pub struct InboundSignal {
    state: Mutex<State>,
    cond: Condvar,
    #[cfg(target_os = "linux")]
    pub(crate) fd: std::sync::OnceLock<crate::inbound_fd::Readiness>,
}

impl InboundSignal {
    pub fn new() -> InboundSignal {
        InboundSignal::default()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wake_all(&self, wakers: Vec<Waker>) {
        for waker in wakers {
            waker.wake();
        }
        self.cond.notify_all();
    }

    /// Says input arrived.
    pub fn notify(&self) {
        let wakers = {
            let mut state = self.lock();
            state.pending = true;
            core::mem::take(&mut state.wakers)
        };
        self.wake_all(wakers);
        self.signal_fd();
    }

    /// Says no more input will come. A waiter sees `Closed` once nothing is pending.
    pub fn close(&self) {
        let wakers = {
            let mut state = self.lock();
            state.closed = true;
            core::mem::take(&mut state.wakers)
        };
        self.wake_all(wakers);
        self.signal_fd();
    }

    /// Wakes every waiter with `Stopped` until [`InboundSignal::reset`].
    pub(crate) fn stop(&self) {
        let wakers = {
            let mut state = self.lock();
            state.stopped = true;
            core::mem::take(&mut state.wakers)
        };
        self.wake_all(wakers);
        self.signal_fd();
    }

    pub(crate) fn reset(&self) {
        self.lock().stopped = false;
    }

    fn signal_fd(&self) {
        #[cfg(target_os = "linux")]
        if let Some(fd) = self.fd.get() {
            fd.signal();
        }
    }

    /// Clears the pending flag. Input that arrives afterwards sets it again.
    pub fn clear_pending(&self) {
        self.lock().pending = false;
    }

    /// Whether input is pending.
    pub fn is_pending(&self) -> bool {
        self.lock().pending
    }

    fn outcome(state: &State) -> Option<InboundWait> {
        if state.stopped {
            Some(InboundWait::Stopped)
        } else if state.pending {
            Some(InboundWait::Ready)
        } else if state.closed {
            Some(InboundWait::Closed)
        } else {
            None
        }
    }

    /// Blocks until input is pending, the inputs close, a stop arrives, or `timeout` passes.
    pub fn wait(&self, timeout: Option<Duration>) -> InboundWait {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        let mut state = self.lock();
        loop {
            if let Some(outcome) = Self::outcome(&state) {
                return outcome;
            }
            match deadline {
                None => {
                    state = self
                        .cond
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return InboundWait::TimedOut;
                    }
                    (state, _) = self
                        .cond
                        .wait_timeout(state, left)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            }
        }
    }

    /// A future that resolves the same way `wait` returns.
    pub fn waiter(self: &Arc<Self>) -> InboundWaiter {
        InboundWaiter {
            signal: Arc::clone(self),
        }
    }
}

/// A runtime-agnostic future for inbound readiness.
pub struct InboundWaiter {
    signal: Arc<InboundSignal>,
}

impl Future for InboundWaiter {
    type Output = InboundWait;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<InboundWait> {
        let mut state = self.signal.lock();
        if let Some(outcome) = InboundSignal::outcome(&state) {
            return Poll::Ready(outcome);
        }
        if !state.wakers.iter().any(|known| known.will_wake(cx.waker())) {
            state.wakers.push(cx.waker().clone());
        }
        Poll::Pending
    }
}
