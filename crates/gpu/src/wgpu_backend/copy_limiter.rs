use std::sync::{Condvar, Mutex};
#[cfg(feature = "gpu-async")]
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

/// Simple semaphore to cap inflight copy operations.
pub(super) struct CopyLimiter {
    limit: u32,
    state: Mutex<CopyLimiterState>,
    cv: Condvar,
}

#[derive(Default)]
struct CopyLimiterState {
    in_flight: u32,
    #[cfg(feature = "gpu-async")]
    next_waiter_id: u64,
    #[cfg(feature = "gpu-async")]
    waiters: Vec<CopyWaiter>,
}

#[cfg(feature = "gpu-async")]
struct CopyWaiter {
    id: u64,
    waker: Waker,
}

impl CopyLimiter {
    pub(super) fn new(limit: u32) -> Self {
        Self {
            limit,
            state: Mutex::new(CopyLimiterState::default()),
            cv: Condvar::new(),
        }
    }

    pub(super) fn acquire(&self) -> CopyGuard<'_> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while state.in_flight >= self.limit {
            state = self
                .cv
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        state.in_flight += 1;
        CopyGuard { limiter: self }
    }

    #[cfg(feature = "gpu-async")]
    pub(super) fn acquire_async(&self) -> CopyAcquireFuture<'_> {
        CopyAcquireFuture {
            limiter: self,
            waiter_id: None,
            acquired: false,
        }
    }

    fn release(&self) {
        #[cfg(feature = "gpu-async")]
        {
            let waiters = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                state.in_flight = state.in_flight.saturating_sub(1);
                state.waiters.drain(..).collect::<Vec<_>>()
            };
            for waiter in waiters {
                waiter.waker.wake();
            }
        }

        #[cfg(not(feature = "gpu-async"))]
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.in_flight = state.in_flight.saturating_sub(1);
        }
        self.cv.notify_one();
    }

    #[cfg(all(test, feature = "gpu-async"))]
    pub(super) fn in_flight(&self) -> u32 {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .in_flight
    }

    #[cfg(all(test, feature = "gpu-async"))]
    pub(super) fn waiter_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .waiters
            .len()
    }
}

#[cfg(feature = "gpu-async")]
pub(super) struct CopyAcquireFuture<'a> {
    limiter: &'a CopyLimiter,
    waiter_id: Option<u64>,
    acquired: bool,
}

#[cfg(feature = "gpu-async")]
impl<'a> Future for CopyAcquireFuture<'a> {
    type Output = CopyGuard<'a>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self
            .limiter
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.in_flight < self.limiter.limit {
            state.in_flight += 1;
            if let Some(id) = self.waiter_id.take() {
                state.waiters.retain(|waiter| waiter.id != id);
            }
            drop(state);
            self.acquired = true;
            return Poll::Ready(CopyGuard {
                limiter: self.limiter,
            });
        }

        match self.waiter_id {
            Some(id) => {
                if let Some(waiter) = state.waiters.iter_mut().find(|waiter| waiter.id == id)
                    && !waiter.waker.will_wake(cx.waker())
                {
                    waiter.waker = cx.waker().clone();
                }
            }
            None => {
                let id = state.next_waiter_id;
                state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
                state.waiters.push(CopyWaiter {
                    id,
                    waker: cx.waker().clone(),
                });
                drop(state);
                self.waiter_id = Some(id);
            }
        }
        Poll::Pending
    }
}

#[cfg(feature = "gpu-async")]
impl Drop for CopyAcquireFuture<'_> {
    fn drop(&mut self) {
        if self.acquired {
            return;
        }
        let Some(id) = self.waiter_id.take() else {
            return;
        };
        let mut state = self
            .limiter
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.waiters.retain(|waiter| waiter.id != id);
    }
}

pub(super) struct CopyGuard<'a> {
    limiter: &'a CopyLimiter,
}

impl Drop for CopyGuard<'_> {
    fn drop(&mut self) {
        self.limiter.release();
    }
}
