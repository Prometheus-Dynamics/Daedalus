//! Inbound wakeups for event-driven hosts.
//!
//! Hosts that drive a graph from their own loop need to know *when* new input arrived without
//! polling. [`InboundWaiter`] offers a runtime-agnostic `Future` (wakers stored under the same
//! buffer lock and woken after it is released) and, with `threads`, a blocking wait on the
//! bridge's existing `Condvar`. No extra lock is introduced; see `docs/host-bridge-lock-granularity.md`.

use crate::portable::Arc;
use crate::prelude::*;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
#[cfg(feature = "threads")]
use std::time::{Duration, Instant};

use super::{HostBridgeBuffers, HostBridgeHandle, HostBridgeShared, has_pending_inbound_locked};

/// Why an inbound wait finished.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InboundWait {
    /// At least one inbound payload is queued on the bridge.
    Ready,
    /// The bridge was closed and no inbound payloads remain.
    Closed,
    /// [`HostBridgeHandle::wake_inbound_waiters`] was called after the waiter was created.
    Woken,
    /// The timeout elapsed first (blocking waits only).
    TimedOut,
}

/// A one-shot wait for inbound host payloads.
///
/// Create it with [`HostBridgeHandle::inbound_waiter`]. Any explicit wakeup issued *after*
/// creation completes the wait with [`InboundWait::Woken`], so a host can create the waiter,
/// check its own stop flag, then wait, without missing a stop signal raised in between.
///
/// `.await` it from any async runtime (it never blocks the executor thread), or, with `threads`,
/// block a dedicated thread in [`InboundWaiter::wait`].
#[must_use = "an InboundWaiter does nothing until waited on or awaited"]
pub struct InboundWaiter {
    shared: Arc<HostBridgeShared>,
    epoch: u64,
    waker_id: Option<u64>,
}

impl core::fmt::Debug for InboundWaiter {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("InboundWaiter")
            .field("epoch", &self.epoch)
            .field("registered", &self.waker_id.is_some())
            .finish()
    }
}

fn inbound_state_locked(guard: &HostBridgeBuffers, epoch: u64) -> Option<InboundWait> {
    if has_pending_inbound_locked(guard) {
        Some(InboundWait::Ready)
    } else if guard.closed {
        Some(InboundWait::Closed)
    } else if guard.wake_epoch != epoch {
        Some(InboundWait::Woken)
    } else {
        None
    }
}

pub(super) fn take_inbound_wakers(guard: &mut HostBridgeBuffers) -> Vec<Waker> {
    if guard.inbound_wakers.is_empty() {
        return Vec::new();
    }
    guard
        .inbound_wakers
        .drain(..)
        .map(|(_, waker)| waker)
        .collect()
}

pub(super) fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

impl InboundWaiter {
    fn new(shared: Arc<HostBridgeShared>) -> Self {
        let epoch = shared.buffers.lock().wake_epoch;
        Self {
            shared,
            epoch,
            waker_id: None,
        }
    }

    /// Non-blocking check: `Some` when the wait would complete immediately.
    pub fn poll_now(&self) -> Option<InboundWait> {
        let guard = self.shared.buffers.lock();
        inbound_state_locked(&guard, self.epoch)
    }

    /// Block the current thread until inbound data is queued, the bridge closes, an explicit
    /// wakeup is issued, or `timeout` elapses (`None` waits indefinitely).
    ///
    /// Do not call this while holding locks that node execution needs.
    #[cfg(feature = "threads")]
    pub fn wait(self, timeout: Option<Duration>) -> InboundWait {
        let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
        let mut guard = self.shared.buffers.lock();
        loop {
            if let Some(state) = inbound_state_locked(&guard, self.epoch) {
                return state;
            }
            match deadline {
                None => self.shared.ready.wait(&mut guard),
                Some(deadline) => {
                    if self
                        .shared
                        .ready
                        .wait_until(&mut guard, deadline)
                        .timed_out()
                    {
                        return inbound_state_locked(&guard, self.epoch)
                            .unwrap_or(InboundWait::TimedOut);
                    }
                }
            }
        }
    }
}

impl Future for InboundWaiter {
    type Output = InboundWait;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut guard = this.shared.buffers.lock();
        if let Some(state) = inbound_state_locked(&guard, this.epoch) {
            if let Some(id) = this.waker_id.take() {
                guard.inbound_wakers.retain(|(waker_id, _)| *waker_id != id);
            }
            return Poll::Ready(state);
        }
        let id = match this.waker_id {
            Some(id) => id,
            None => {
                let id = guard.next_waker_id;
                guard.next_waker_id = guard.next_waker_id.wrapping_add(1);
                this.waker_id = Some(id);
                id
            }
        };
        match guard
            .inbound_wakers
            .iter_mut()
            .find(|(waker_id, _)| *waker_id == id)
        {
            Some((_, waker)) => {
                if !waker.will_wake(cx.waker()) {
                    waker.clone_from(cx.waker());
                }
            }
            None => guard.inbound_wakers.push((id, cx.waker().clone())),
        }
        Poll::Pending
    }
}

impl Drop for InboundWaiter {
    fn drop(&mut self) {
        if let Some(id) = self.waker_id.take() {
            let mut guard = self.shared.buffers.lock();
            guard.inbound_wakers.retain(|(waker_id, _)| *waker_id != id);
        }
    }
}

impl HostBridgeHandle {
    /// Create a waiter for inbound host payloads. See [`InboundWaiter`].
    pub fn inbound_waiter(&self) -> InboundWaiter {
        InboundWaiter::new(self.shared.clone())
    }

    /// Blocking wait with a typed reason. `None` waits indefinitely.
    #[cfg(feature = "threads")]
    pub fn wait_inbound(&self, timeout: Option<Duration>) -> InboundWait {
        self.inbound_waiter().wait(timeout)
    }

    /// Wake every blocking and async inbound waiter without queuing data.
    ///
    /// Waiters created before this call complete with [`InboundWait::Woken`]; use it to interrupt
    /// a host drive loop (for example to stop it).
    pub fn wake_inbound_waiters(&self) {
        let mut guard = self.shared.buffers.lock();
        guard.wake_epoch = guard.wake_epoch.wrapping_add(1);
        self.shared.notify_all();
        let wakers = take_inbound_wakers(&mut guard);
        drop(guard);
        wake_all(wakers);
    }

    /// Whether the whole bridge has been closed.
    pub fn is_closed(&self) -> bool {
        self.shared.buffers.lock().closed
    }
}

#[cfg(test)]
mod tests {
    use alloc::task::Wake;
    use core::sync::atomic::{AtomicBool, Ordering};
    #[cfg(feature = "threads")]
    use std::{thread, time::Duration};

    use super::*;
    use crate::host_bridge::HostBridgeManager;

    struct FlagWaker(AtomicBool);

    impl Wake for FlagWaker {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    #[cfg(feature = "threads")]
    fn blocking_wait_times_out_then_sees_ready() {
        let handle = HostBridgeManager::new().ensure_handle("host");
        assert_eq!(
            handle.wait_inbound(Some(Duration::from_millis(5))),
            InboundWait::TimedOut
        );
        handle.push("in", 1i64);
        assert_eq!(
            handle.wait_inbound(Some(Duration::ZERO)),
            InboundWait::Ready
        );
    }

    #[test]
    #[cfg(feature = "threads")]
    fn blocking_wait_wakes_on_feed_from_other_thread() {
        let handle = HostBridgeManager::new().ensure_handle("host");
        let feeder = handle.clone();
        let join = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            feeder.push("in", 7i64);
        });
        assert_eq!(handle.wait_inbound(None), InboundWait::Ready);
        join.join().unwrap();
    }

    #[test]
    #[cfg(feature = "threads")]
    fn explicit_wake_and_close_complete_waits() {
        let handle = HostBridgeManager::new().ensure_handle("host");
        let waiter = handle.inbound_waiter();
        handle.wake_inbound_waiters();
        assert_eq!(waiter.wait(None), InboundWait::Woken);

        let waker_handle = handle.clone();
        let join = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            waker_handle.close();
        });
        assert_eq!(handle.wait_inbound(None), InboundWait::Closed);
        assert!(handle.is_closed());
        join.join().unwrap();
    }

    #[test]
    fn future_registers_waker_and_is_woken_by_feed() {
        let handle = HostBridgeManager::new().ensure_handle("host");
        let flag = Arc::new(FlagWaker(AtomicBool::new(false)));
        let waker = Waker::from(flag.clone());
        let mut cx = Context::from_waker(&waker);
        let mut waiter = handle.inbound_waiter();

        assert!(Pin::new(&mut waiter).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut waiter).poll(&mut cx).is_pending());
        assert_eq!(handle.shared.buffers.lock().inbound_wakers.len(), 1);

        handle.push("in", 1i64);
        assert!(flag.0.load(Ordering::SeqCst));
        assert_eq!(
            Pin::new(&mut waiter).poll(&mut cx),
            Poll::Ready(InboundWait::Ready)
        );
        assert!(handle.shared.buffers.lock().inbound_wakers.is_empty());
    }

    #[test]
    fn dropped_future_unregisters_waker() {
        let handle = HostBridgeManager::new().ensure_handle("host");
        let flag = Arc::new(FlagWaker(AtomicBool::new(false)));
        let waker = Waker::from(flag);
        let mut cx = Context::from_waker(&waker);
        let mut waiter = handle.inbound_waiter();
        assert!(Pin::new(&mut waiter).poll(&mut cx).is_pending());
        drop(waiter);
        assert!(handle.shared.buffers.lock().inbound_wakers.is_empty());
    }
}
