//! Host bytes onto the device.

use std::sync::Arc;

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{CompileCx, Refusal};
use daedalus_runner::{UNSTAMPED, put_typed, take_typed};

use crate::adapter::Options;
use crate::buffer::Storage;
use crate::context::Context;
use crate::memory::{SPACE_DEVICE, SPACE_HOST_MAPPED};
use crate::transfer::pooled;

/// Copies a host value's bytes into a buffer of the step's own, reused run after run.
struct UploadStep {
    /// Where the buffers are made: device memory, or host-mapped memory written in place.
    space: &'static str,
    /// The device, found the first time the step runs.
    context: Option<Arc<Context>>,
    kept: Vec<Arc<Storage>>,
}

fn fault(cx: &mut StepCx, reason: &str) -> StepEnd {
    cx.fault(None, reason);
    StepEnd::Fault
}

impl OwnedStep for UploadStep {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let Some(input) = slots.cell(0) else {
            return fault(cx, "an upload has no input");
        };
        let storage = match input.bytes() {
            Some([]) => return fault(cx, "an upload was given no bytes"),
            Some(bytes) => {
                let context = match &self.context {
                    Some(context) => Arc::clone(context),
                    None => {
                        // The device the backend opened, or the one the process shares.
                        let found = Context::latest()
                            .map_or_else(|| Context::open(&Options::default()), Ok);
                        let Ok(context) = found else {
                            return fault(cx, "an upload has no device to go to");
                        };
                        self.context = Some(Arc::clone(&context));
                        context
                    }
                };
                // A buffer is a whole number of words, and the tail is zeroes.
                let size = (bytes.len() as u64).next_multiple_of(4);
                let space = self.space;
                let made = pooled(&mut self.kept, size, || {
                    Storage::placed_on(Arc::clone(&context), size, space)
                });
                let Ok(storage) = made else {
                    return fault(cx, "an upload couldn't make its buffer");
                };
                let whole = bytes.len() / 4 * 4;
                let (head, tail) = bytes.split_at(whole);
                let mut last = [0; 4];
                for (to, from) in last.iter_mut().zip(tail) {
                    *to = *from;
                }
                let wrote = storage.write(0, head).is_ok() || head.is_empty();
                let wrote =
                    wrote && (tail.is_empty() || storage.write(whole as u64, &last).is_ok());
                if !wrote {
                    return fault(cx, "an upload couldn't write its buffer");
                }
                context.moved.upload(bytes.len() as u64);
                storage
            }
            // Already on a device: nothing to copy.
            None => match take_typed::<Storage>(input) {
                Some(storage) => storage,
                None => return fault(cx, "an upload was given a value with no bytes"),
            },
        };
        let Some(out) = slots.cell(1) else {
            return fault(cx, "an upload has no output");
        };
        put_typed(out, UNSTAMPED.0, UNSTAMPED.1, storage);
        StepEnd::Done
    }
}

fn step(space: &'static str) -> Step {
    Step::boxed(UploadStep {
        space,
        context: None,
        kept: Vec::new(),
    })
}

pub(crate) fn make(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(step(SPACE_DEVICE))
}

pub(crate) fn make_mapped(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(step(SPACE_HOST_MAPPED))
}
