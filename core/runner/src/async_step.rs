//! Async nodes (std only): a body returns `Pending` with a fence backed by a future. The fence is
//! Ready when the future completes. The runner polls it through its normal fence path, so nothing
//! embeds an executor and a tick never blocks on one.

#![cfg(feature = "std")]

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::boxed::Box;

use daedalus_abi::{Fence, FencePoll};

/// A future a step body runs. It owns the work; the step owns this while it is pending.
pub struct FutureFence {
    future: Option<Pin<Box<dyn Future<Output = bool> + Send>>>,
}

impl FutureFence {
    pub fn new() -> FutureFence {
        FutureFence { future: None }
    }

    /// Starts `future` and returns the fence to hand back in `StepEnd::Pending`. The future
    /// resolves to true for success, or false to report a fault of this step.
    ///
    /// The fence points at this value, so it must stay in place until the fence is done, and a
    /// body keeps it in its own state to make that so.
    pub fn start<F>(&mut self, future: F) -> Fence
    where
        F: Future<Output = bool> + Send + 'static,
    {
        self.future = Some(Box::pin(future));
        Fence {
            poll: poll_future,
            data: core::ptr::from_mut(self).cast(),
        }
    }
}

impl Default for FutureFence {
    fn default() -> Self {
        FutureFence::new()
    }
}

/// # Safety
///
/// `data` must come from [`FutureFence::start`] and the `FutureFence` must still be in place.
// SAFETY: the caller upholds the `# Safety` contract documented above.
unsafe extern "C" fn poll_future(data: *mut ()) -> FencePoll {
    // SAFETY: as the caller promises, `data` is a `FutureFence` that hasn't moved.
    let state = unsafe { &mut *data.cast::<FutureFence>() };
    let Some(future) = state.future.as_mut() else {
        return FencePoll::Ready;
    };
    let mut cx = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut cx) {
        Poll::Pending => FencePoll::Pending,
        Poll::Ready(ok) => {
            state.future = None;
            if ok {
                FencePoll::Ready
            } else {
                FencePoll::Failed
            }
        }
    }
}
