//! Device buffers to where the host can read them.
//!
//! A readback is a copy on the device, from device memory into a host-mapped buffer, and it
//! completes through a fence like any other work. The step never waits and never maps: the host
//! reads the mapped buffer in place once the fence has resolved.

use std::sync::Arc;

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{CompileCx, Refusal};
use daedalus_runner::{UNSTAMPED, put_typed, take_typed, typed_view};

use crate::buffer::Storage;
use crate::fence::{Ticket, TicketState};
use crate::memory::SPACE_HOST_MAPPED;
use crate::transfer::pooled;

fn fault(cx: &mut StepCx, reason: &str) -> StepEnd {
    cx.fault(None, reason);
    StepEnd::Fault
}

/// Copies a device buffer into a host-mapped one of the step's own.
struct ReadbackStep {
    kept: Vec<Arc<Storage>>,
}

impl OwnedStep for ReadbackStep {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let cells = slots.as_mut_slice();
        let [input, out, ..] = cells else {
            return fault(cx, "a readback needs an input and an output");
        };
        let Some(source) = typed_view::<Storage>(input) else {
            return fault(
                cx,
                "a readback was given something that isn't a device buffer",
            );
        };
        // The host can read it where it is.
        if source.is_host_mapped() {
            let Some(same) = take_typed::<Storage>(input) else {
                return fault(cx, "a readback lost its input");
            };
            put_typed(out, UNSTAMPED.0, UNSTAMPED.1, same);
            return StepEnd::Done;
        }
        let context = Arc::clone(&source.context);
        if let Some(reported) = context.errors.take() {
            return fault(cx, &reported);
        }
        if context.is_lost() {
            return fault(cx, "the device is lost");
        }
        let size = source.size;
        let made = pooled(&mut self.kept, size, || {
            Storage::placed_on(Arc::clone(&context), size, SPACE_HOST_MAPPED)
        });
        let Ok(target) = made else {
            return fault(cx, "a readback couldn't make its buffer");
        };
        let mut encoder = context
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor::default());
        encoder.copy_buffer_to_buffer(source.buffer(), 0, target.buffer(), 0, size);
        let state = TicketState::new();
        let finished = Arc::clone(&state);
        {
            let _held = context.submit.lock();
            context.queue.submit([encoder.finish()]);
            context
                .queue
                .on_submitted_work_done(move || finished.finish());
        }
        context.moved.download(size);
        put_typed(out, UNSTAMPED.0, UNSTAMPED.1, target);
        StepEnd::Pending(Ticket::new(state, context, None).into_fence())
    }
}

/// Hands a buffer the host can already read to the host, as it is.
struct MapStep;

impl OwnedStep for MapStep {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let Some(storage) = slots.cell(0).and_then(take_typed::<Storage>) else {
            return fault(cx, "a mapped buffer was expected");
        };
        let Some(out) = slots.cell(1) else {
            return fault(cx, "a map has no output");
        };
        put_typed(out, UNSTAMPED.0, UNSTAMPED.1, storage);
        StepEnd::Done
    }
}

pub(crate) fn make(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(ReadbackStep { kept: Vec::new() }))
}

pub(crate) fn make_map(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(MapStep))
}
