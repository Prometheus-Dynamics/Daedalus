//! At most a fixed number of copies in flight at once.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

/// Copies in flight on one device by default.
pub(crate) const MAX_INFLIGHT_COPIES: u32 = 8;

pub(crate) struct CopyLimiter {
    limit: u32,
    state: Mutex<State>,
    freed: Condvar,
}

#[derive(Default)]
struct State {
    in_flight: u32,
    next_waiter: u64,
    waiters: Vec<(u64, Waker)>,
}

/// One copy's place. Dropping it frees the place.
pub(crate) struct CopyGuard<'a> {
    limiter: &'a CopyLimiter,
}

impl CopyLimiter {
    pub(crate) fn new(limit: u32) -> Self {
        CopyLimiter {
            limit: limit.max(1),
            state: Mutex::new(State::default()),
            freed: Condvar::new(),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Blocks until a place is free.
    pub(crate) fn acquire(&self) -> CopyGuard<'_> {
        let mut state = self.state();
        while state.in_flight >= self.limit {
            state = self
                .freed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        state.in_flight += 1;
        CopyGuard { limiter: self }
    }

    /// Resolves when a place is free.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the async readback of E8 uses it")
    )]
    pub(crate) fn acquire_async(&self) -> CopyAcquire<'_> {
        CopyAcquire {
            limiter: self,
            waiter: None,
            acquired: false,
        }
    }

    fn release(&self) {
        let waiters = {
            let mut state = self.state();
            state.in_flight = state.in_flight.saturating_sub(1);
            std::mem::take(&mut state.waiters)
        };
        for (_, waker) in waiters {
            waker.wake();
        }
        self.freed.notify_one();
    }
}

impl Drop for CopyGuard<'_> {
    fn drop(&mut self) {
        self.limiter.release();
    }
}

pub(crate) struct CopyAcquire<'a> {
    limiter: &'a CopyLimiter,
    waiter: Option<u64>,
    acquired: bool,
}

impl<'a> Future for CopyAcquire<'a> {
    type Output = CopyGuard<'a>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let limiter = self.limiter;
        let mut state = limiter.state();
        if state.in_flight < limiter.limit {
            state.in_flight += 1;
            if let Some(id) = self.waiter.take() {
                state.waiters.retain(|(waiter, _)| *waiter != id);
            }
            drop(state);
            self.acquired = true;
            return Poll::Ready(CopyGuard { limiter });
        }
        match self.waiter {
            Some(id) => {
                if let Some((_, waker)) = state.waiters.iter_mut().find(|(waiter, _)| *waiter == id)
                {
                    waker.clone_from(cx.waker());
                } else {
                    // A release took the list and woke this future. Wait for the next one.
                    state.waiters.push((id, cx.waker().clone()));
                }
            }
            None => {
                let id = state.next_waiter;
                state.next_waiter = state.next_waiter.wrapping_add(1);
                state.waiters.push((id, cx.waker().clone()));
                drop(state);
                self.waiter = Some(id);
            }
        }
        Poll::Pending
    }
}

impl Drop for CopyAcquire<'_> {
    fn drop(&mut self) {
        if self.acquired {
            return;
        }
        if let Some(id) = self.waiter.take() {
            self.limiter
                .state()
                .waiters
                .retain(|(waiter, _)| *waiter != id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    use super::CopyLimiter;

    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn waker() -> (Arc<Count>, Waker) {
        let count = Arc::new(Count(AtomicUsize::new(0)));
        (Arc::clone(&count), Waker::from(count))
    }

    #[test]
    fn copy_limiter_async_acquire_yields_when_full() {
        let limiter = CopyLimiter::new(1);
        let held = limiter.acquire();
        let (woken, waker) = waker();
        let mut cx = Context::from_waker(&waker);
        let mut waiting = pin!(limiter.acquire_async());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        assert_eq!(
            limiter.state().waiters.len(),
            1,
            "one waiter, however often polled"
        );
        drop(held);
        assert_eq!(woken.0.load(Ordering::SeqCst), 1);
        let Poll::Ready(guard) = waiting.as_mut().poll(&mut cx) else {
            panic!("a place is free");
        };
        assert_eq!(limiter.state().in_flight, 1);
        drop(guard);
        assert_eq!(limiter.state().in_flight, 0);
    }

    #[test]
    fn copy_limiter_async_acquire_wakes_active_waiter_after_cancelled_waiter() {
        let limiter = CopyLimiter::new(1);
        let held = limiter.acquire();
        let (cancelled_woken, cancelled_waker) = waker();
        let (active_woken, active_waker) = waker();
        {
            let mut cancelled = pin!(limiter.acquire_async());
            let mut cx = Context::from_waker(&cancelled_waker);
            assert!(cancelled.as_mut().poll(&mut cx).is_pending());
        }
        assert!(
            limiter.state().waiters.is_empty(),
            "a dropped waiter is gone"
        );
        let mut active = pin!(limiter.acquire_async());
        let mut cx = Context::from_waker(&active_waker);
        assert!(active.as_mut().poll(&mut cx).is_pending());
        drop(held);
        assert_eq!(cancelled_woken.0.load(Ordering::SeqCst), 0);
        assert_eq!(active_woken.0.load(Ordering::SeqCst), 1);
        assert!(active.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn a_blocking_acquire_waits_for_a_place() {
        let limiter = CopyLimiter::new(2);
        let (first, second) = (limiter.acquire(), limiter.acquire());
        std::thread::scope(|scope| {
            let third = scope.spawn(|| drop(limiter.acquire()));
            drop(first);
            third.join().expect("the third copy got a place");
        });
        drop(second);
        assert_eq!(limiter.state().in_flight, 0);
    }
}
