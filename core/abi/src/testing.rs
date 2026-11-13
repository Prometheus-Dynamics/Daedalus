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

#[cfg(all(feature = "std", not(daedalus_abi_baseline)))]
pub use crate::kept::{KeptDetail, keep_detail};
