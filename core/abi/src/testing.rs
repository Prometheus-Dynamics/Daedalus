//! Running a step from a test that isn't allowed `unsafe`.
//!
//! The only caller of a step in production is the runner. A conformance suite needs to run steps
//! too, and it lives in a crate that may not write `unsafe`. These functions trust a step to have
//! come from `Step::of`, `Step::boxed` or a backend's `compile`, which is why they sit behind
//! `test-run` and are not for production use.

use crate::{Fence, FencePoll, Slots, Step, StepCx, StepEnd};

/// Calls the step once.
pub fn run_once(step: &mut Step, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
    // SAFETY: `slots` and `cx` are valid and exclusive for this call, and the step's `run` and
    // `data` belong together, as this module's feature promises.
    unsafe { (step.run)(step.data, slots, cx) }
}

/// Frees what the step owns. Call it once, when finished with the step.
pub fn release(step: Step) {
    step.discard();
}

/// Polls a pending step's fence until it is not pending, for at most `polls` polls, and says what
/// the last poll found. A step's output is read once it says so: a host-mapped buffer is read in
/// place, so the work that wrote it has to finish first.
pub fn wait_fence(fence: &Fence, polls: u64) -> FencePoll {
    let mut last = FencePoll::Pending;
    for _ in 0..polls {
        // SAFETY: the fence is one a step returned, and the step's contract keeps its work until
        // the fence is ready, as the runner relies on when it polls.
        last = unsafe { fence.poll() };
        if last != FencePoll::Pending {
            break;
        }
        core::hint::spin_loop();
    }
    last
}

/// What a step under test gave [`StepCx::detail`], for a test that isn't allowed `unsafe`.
#[cfg(feature = "std")]
pub struct KeptDetail(std::sync::Mutex<std::string::String>);

#[cfg(feature = "std")]
impl KeptDetail {
    /// Everything written so far, in order.
    pub fn text(&self) -> std::string::String {
        match self.0.lock() {
            Ok(text) => text.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

/// Gives `cx` somewhere to keep what a step writes with [`StepCx::detail`], and hands it back to
/// read. It is never freed, so that it outlives whatever step holds the context: a test's cost.
#[cfg(feature = "std")]
pub fn keep_detail(cx: &mut StepCx) -> &'static KeptDetail {
    extern "C" fn write(context: *mut core::ffi::c_void, _: u32, text: *const u8, len: usize) {
        if text.is_null() {
            return;
        }
        // SAFETY: `context` is the `KeptDetail` leaked below, and `text` is `len` bytes the
        // caller holds for this call.
        let (kept, bytes) = unsafe {
            (
                &*context.cast::<KeptDetail>(),
                core::slice::from_raw_parts(text, len),
            )
        };
        if let Ok(mut held) = kept.0.lock() {
            held.push_str(&std::string::String::from_utf8_lossy(bytes));
        }
    }
    let kept: &'static KeptDetail = std::boxed::Box::leak(std::boxed::Box::new(KeptDetail(
        std::sync::Mutex::new(std::string::String::new()),
    )));
    cx.detail = crate::DetailSink {
        context: core::ptr::from_ref(kept).cast_mut().cast(),
        write: Some(write),
    };
    kept
}
