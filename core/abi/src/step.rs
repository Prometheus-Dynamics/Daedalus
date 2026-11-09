//! One unit of work: a function pointer and its data.

use crate::{Fence, Slots, StepCx, guard_or};

/// How a step ended.
#[repr(C, u32)]
#[derive(Clone, Copy, Debug)]
pub enum StepEnd {
    Done,
    /// Started, and finishes when the fence is ready.
    Pending(Fence),
    Skipped,
    /// Failed. The detail is in [`StepCx`].
    Fault,
}

#[repr(C)]
#[derive(Debug)]
pub struct Step {
    pub run: unsafe extern "C" fn(*mut (), *mut Slots, *mut StepCx) -> StepEnd,
    pub data: *mut (),
    /// Frees `data`, once, when the plan is dropped. `None` for generated code.
    pub release: Option<unsafe extern "C" fn(*mut ())>,
}

// SAFETY: a step runs on one thread at a time, and whoever fills in `run` and `data` promises
// they can move between threads. `Step::of` stores only a word, and `Step::boxed` requires
// `Send`.
unsafe impl Send for Step {}

/// A step body written without `unsafe`. `word` is whatever [`Step::of`] was given, usually an
/// index into a table the backend owns.
pub trait StepWord: 'static {
    fn run(word: usize, slots: &mut Slots, cx: &mut StepCx) -> StepEnd;
}

// SAFETY: must be called with valid, exclusive `slots` and `cx`, which the runner guarantees.
unsafe extern "C" fn run_word<F: StepWord>(
    data: *mut (),
    slots: *mut Slots,
    cx: *mut StepCx,
) -> StepEnd {
    // SAFETY: the runner passes valid pointers it holds exclusively for this call.
    let (slots, cx) = unsafe { (&mut *slots, &mut *cx) };
    guard_or(StepEnd::Fault, || F::run(data.addr(), slots, cx))
}

impl Step {
    /// A step that carries one word and allocates nothing.
    pub const fn of<F: StepWord>(word: usize) -> Step {
        Step {
            run: run_word::<F>,
            data: core::ptr::without_provenance_mut(word),
            release: None,
        }
    }
}

/// A step body that owns its data, written without `unsafe`.
pub trait OwnedStep: Send + 'static {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd;
}

#[cfg(feature = "alloc")]
// SAFETY: must be called with the `data` from `Step::boxed` and valid, exclusive `slots` and `cx`.
unsafe extern "C" fn run_owned<D: OwnedStep>(
    data: *mut (),
    slots: *mut Slots,
    cx: *mut StepCx,
) -> StepEnd {
    // SAFETY: `data` came from `Box::into_raw` in `Step::boxed` and isn't released until the plan
    // is dropped. The runner passes valid pointers it holds exclusively for this call.
    let (body, slots, cx) = unsafe { (&mut *data.cast::<D>(), &mut *slots, &mut *cx) };
    guard_or(StepEnd::Fault, || body.run(slots, cx))
}

impl Step {
    /// Frees the step without running it, as a planner does with a discarded compile.
    pub fn discard(self) {
        if let Some(release) = self.release {
            // SAFETY: `release` is called once, on the `data` it was stored with, and `self` is
            // consumed so it can't be called again.
            unsafe { release(self.data) }
        }
    }
}

#[cfg(feature = "alloc")]
// SAFETY: must be called once, with the `data` from `Step::boxed`.
unsafe extern "C" fn release_owned<D: OwnedStep>(data: *mut ()) {
    // SAFETY: `data` came from `Box::into_raw` in `Step::boxed`, and `release` runs once.
    drop(unsafe { alloc::boxed::Box::from_raw(data.cast::<D>()) });
}

#[cfg(feature = "alloc")]
impl Step {
    /// A step that owns `body`. The allocation is made once, while planning.
    pub fn boxed<D: OwnedStep>(body: D) -> Step {
        Step {
            run: run_owned::<D>,
            data: alloc::boxed::Box::into_raw(alloc::boxed::Box::new(body)).cast(),
            release: Some(release_owned::<D>),
        }
    }
}
