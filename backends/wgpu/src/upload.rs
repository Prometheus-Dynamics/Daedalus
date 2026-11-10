//! Host bytes onto the device.

use std::sync::Arc;

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{CompileCx, Refusal};
use daedalus_runner::{UNSTAMPED, put_typed, take_typed};

use crate::buffer::Storage;
use crate::context::Context;
use crate::memory::{SPACE_DEVICE, SPACE_HOST_MAPPED};
use crate::transfer::pooled;

/// Copies a host value's bytes into a buffer of the step's own, reused run after run.
struct UploadStep {
    /// Where the buffers are made: device memory, or host-mapped memory written in place.
    space: &'static str,
    /// Which device the buffers are made on: 0 for the default, or a named device's place.
    slot: usize,
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
                    Some(context) if !context.is_lost() => Arc::clone(context),
                    // None yet, or the one it had was lost: find the device again.
                    _ => {
                        self.kept.clear();
                        let Ok(context) = crate::devices::context(self.slot) else {
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
            None => match take_typed::<Storage>(input) {
                // On another device, in memory the host reads: its bytes are copied across.
                // This is the last leg of the only way from one device to another.
                Some(foreign) if self.is_foreign(&foreign) => match self.across(&foreign) {
                    Ok(storage) => storage,
                    Err(reason) => return fault(cx, reason),
                },
                // Already on the device: nothing to copy.
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

impl UploadStep {
    /// Whether `storage` is on a device other than this step's: a named one when this is the
    /// default's step, or any other when this is a named one's.
    fn is_foreign(&self, storage: &Storage) -> bool {
        crate::devices::slot_of(storage.context.key) != self.slot
    }

    /// A buffer on this step's device holding the bytes of `foreign`, read through the host.
    fn across(&mut self, foreign: &Storage) -> Result<Arc<Storage>, &'static str> {
        let Ok(context) = crate::devices::context(self.slot) else {
            return Err("an upload has no device to go to");
        };
        let size = foreign.size;
        let mut bytes = vec![0; size as usize];
        foreign
            .read_into(0, &mut bytes)
            .map_err(|_| "an upload couldn't read the other device's buffer")?;
        let space = self.space;
        let storage = pooled(&mut self.kept, size, || {
            Storage::placed_on(Arc::clone(&context), size, space)
        })
        .map_err(|_| "an upload couldn't make its buffer")?;
        storage
            .write(0, &bytes)
            .map_err(|_| "an upload couldn't write its buffer")?;
        foreign.context.moved.download(size);
        context.moved.upload(size);
        Ok(storage)
    }
}

fn step(space: &'static str, slot: usize) -> Step {
    Step::boxed(UploadStep {
        space,
        slot,
        context: None,
        kept: Vec::new(),
    })
}

fn make<const SLOT: usize>(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(step(SPACE_DEVICE, SLOT))
}

fn make_mapped<const SLOT: usize>(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(step(SPACE_HOST_MAPPED, SLOT))
}

type Make = fn(&CompileCx<'_>) -> Result<Step, Refusal>;

/// The makers of the upload into device memory and of the one into host-mapped memory, for
/// the device in `slot`. A transfer's maker is a plain function, so which device it is for is
/// in its type.
pub(crate) fn makes(slot: usize) -> (Make, Make) {
    match slot {
        1 => (make::<1>, make_mapped::<1>),
        2 => (make::<2>, make_mapped::<2>),
        3 => (make::<3>, make_mapped::<3>),
        _ => (make::<0>, make_mapped::<0>),
    }
}

const _: () = assert!(crate::devices::MAX_NAMED == 3, "one maker per named device");
