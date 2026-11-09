//! A value leaving one runner for another. A typed payload's concrete type is erased here, so it
//! travels as its raw slot, and every extra holder is a reference count and never a copy.

#![cfg(feature = "alloc")]

use daedalus_abi::Slot;
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_value::Value;

use crate::error::RunError;
use crate::{Runner, owned};

enum Held {
    Plain(Value),
    /// Owns one reference to the payload.
    Typed(Slot),
}

/// A value on its way to one or more other runners.
pub struct Forward(Option<Held>);

// SAFETY: a typed slot came from `typed_raw`, which requires `T: Send + Sync`, and the plain owners
// are `Send`. The handle owns its reference.
unsafe impl Send for Forward {}

impl Forward {
    /// A second holder of the same payload.
    pub fn share(&self) -> Forward {
        Forward(self.0.as_ref().map(|held| match held {
            Held::Plain(value) => Held::Plain(value.clone_inline()),
            Held::Typed(raw) => {
                // SAFETY: `raw` owns a live reference, so taking another is sound. The new handle
                // releases it once.
                unsafe { owned::retain(raw) };
                Held::Typed(*raw)
            }
        }))
    }
}

impl Drop for Forward {
    fn drop(&mut self) {
        if let Some(Held::Typed(raw)) = &self.0 {
            // SAFETY: this handle owns one reference and is the only one to release it.
            unsafe { owned::release(raw) };
        }
    }
}

impl Runner<'_> {
    /// Takes the oldest value out of `slot`, ready to hand to other runners.
    pub fn take_forward(&mut self, slot: u32) -> Option<Forward> {
        let index = self.queue_index(slot).ok()?;
        let queue = self.queues[index];
        if queue.len == 0 {
            return None;
        }
        let raw = self.storage[queue.head_index()];
        let held = if owned::is_typed(&raw) {
            // The queue's reference moves to the handle, because the pop below forgets it.
            Held::Typed(raw)
        } else {
            // SAFETY: the queue owns this entry, and the pop below forgets it, so the reference
            // moves to the value.
            Held::Plain(unsafe { owned::decode(&raw) })
        };
        self.queues[index].pop_head();
        Some(Forward(Some(held)))
    }

    /// Stores a forwarded value at the back of `slot`, as this plan's type `ty` in `space`. Two
    /// plans number the same type differently, so the caller has already compared their keys.
    pub fn feed_forward(
        &mut self,
        slot: u32,
        ty: TypeIndex,
        space: SpaceIndex,
        mut forward: Forward,
    ) -> Result<(), RunError> {
        let index = self.queue_index(slot)?;
        if self.queues[index].is_full() {
            return Err(RunError::Overflow { slot });
        }
        match forward.0.take() {
            Some(Held::Plain(mut value)) => {
                value.ty = ty;
                value.space = space;
                self.feed_value(slot, value)
            }
            Some(Held::Typed(mut raw)) => {
                raw.type_index = ty.0;
                raw.space = u32::from(space.0);
                self.queues[index].push(self.storage, raw);
                Ok(())
            }
            None => Ok(()),
        }
    }
}
