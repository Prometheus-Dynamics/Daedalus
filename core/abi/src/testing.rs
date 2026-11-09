//! Running a step from a test that isn't allowed `unsafe`.
//!
//! The only caller of a step in production is the runner. A conformance suite needs to run steps
//! too, and it lives in a crate that may not write `unsafe`. These functions trust a step to have
//! come from `Step::of`, `Step::boxed` or a backend's `compile`, which is why they sit behind
//! `test-run` and are not for production use.

use crate::{Slots, Step, StepCx, StepEnd};

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
