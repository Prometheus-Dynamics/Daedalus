//! Joins: when a step with required inputs may run. A step with none skips all of this. Optional,
//! fan-in and undeclared inputs are never in the required list, so they never block a step.

use daedalus_abi::Slot;

use crate::exec::{Env, SlotParts};
use crate::step::{Queue, SlotKind, StepMeta};
use crate::{RunError, Runner, spanned};

/// What happens when a required input is missing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Fire {
    /// The step isn't called, and whatever arrived on the other inputs is dropped.
    #[default]
    Any,
    /// The step isn't called and everything stays queued, across ticks, until every required
    /// input has a value.
    All,
}

/// Whether a queue counts as having a value for its step: a held slot counts while it holds one.
fn present(queue: &Queue) -> bool {
    if queue.kind == Some(SlotKind::Held) {
        queue.len > 0
    } else {
        queue.unconsumed > 0
    }
}

/// One test per required input. False means the step is not called this tick; for `Fire::Any` the
/// arrivals on its inputs have been dropped.
pub(crate) fn join_ready<'t, E: Env<'t>>(env: &mut E, meta: StepMeta) -> bool {
    let mut ready = true;
    for &slot in spanned(env.table(), meta.required) {
        ready &= env.with_queue(slot as usize, |queue, _| present(queue));
    }
    if !ready && meta.fire == Fire::Any {
        for &slot in spanned(env.table(), meta.inputs) {
            env.with_queue(slot as usize, |queue, parts: SlotParts| {
                if queue.unconsumed > 0 {
                    // SAFETY: `with_queue` holds what protects this queue, and this step consumes
                    // it. Nothing was moved out of it, because the step never ran.
                    unsafe { parts.commit(queue, false) };
                }
            });
        }
    }
    ready
}

impl Runner<'_> {
    /// Whether `slot` is a held input.
    pub fn is_held(&self, slot: u32) -> bool {
        self.queue(slot)
            .is_some_and(|queue| queue.kind == Some(SlotKind::Held))
    }

    /// Drops what a held slot holds, releasing it. A slot with nothing held is left as it is.
    pub fn clear_held(&mut self, slot: u32) -> Result<(), RunError> {
        if (slot as usize) >= self.slots {
            return Err(RunError::NoSlot { slot });
        }
        let parts = SlotParts {
            storage: self.storage.as_mut_ptr(),
            inline: self.inline.as_mut_ptr(),
        };
        // SAFETY: `&mut self` is exclusive, and `slot` was checked above.
        unsafe { parts.drop_all(&mut self.queues[slot as usize]) };
        Ok(())
    }

    /// Replaces a held slot's value with a raw one that owns nothing. Consumers are not woken: a
    /// held value is context, not work.
    pub fn replace_held(&mut self, slot: u32, value: Slot) -> Result<(), RunError> {
        self.clear_held(slot)?;
        self.push(slot, value)
    }
}
