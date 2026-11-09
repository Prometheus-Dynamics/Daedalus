//! What a body's wrapper uses to read and write its cells. A wrapper binds each cell once, so the
//! borrows here are what make that sound.

#![cfg(feature = "alloc")]

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::any::TypeId;

use daedalus_abi::{Slot, SlotState};
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_value::OwnerKind;

use crate::nested::drop_owned;
use crate::owned::{INLINE_BYTES, InlineCell, typed_ops};

/// Moves the reference out of a typed cell, which is left `Moved` so nothing releases it again.
pub fn take_typed<T: 'static>(slot: &mut Slot) -> Option<Arc<T>> {
    if slot.state != SlotState::Full as u32 || slot.owner != OwnerKind::Typed as u32 {
        return None;
    }
    // SAFETY: a full typed cell's `cap` was written by `typed_raw`, so it names this table.
    let ops = unsafe { typed_ops(slot.cap) };
    if (ops.type_id)() != TypeId::of::<T>() {
        return None;
    }
    slot.state = SlotState::Moved as u32;
    // SAFETY: the table says `data` came from `Arc::<T>::into_raw`. The cell gave up its one
    // reference on the line above.
    Some(unsafe { Arc::from_raw(slot.data.cast::<T>().cast_const()) })
}

/// A value that lives outside any queue: a constant, fixed while planning and read every tick.
pub struct Held {
    slot: Slot,
    // The inline bytes `slot` points at. Boxed, so moving a `Held` doesn't move them.
    _bytes: Box<InlineCell>,
}

// SAFETY: `slot` owns either the boxed bytes beside it or a reference to a `T: Send + Sync`, which
// `typed_raw` requires. Nothing else can reach them.
unsafe impl Send for Held {}

impl Held {
    /// A constant written by `fill`, which writes the slot from the buffer it's given.
    pub fn new(fill: impl FnOnce(&mut InlineCell, &mut Slot)) -> Held {
        let mut bytes = Box::new([0_u8; INLINE_BYTES]);
        let mut slot = Slot::EMPTY;
        fill(&mut bytes, &mut slot);
        Held {
            slot,
            _bytes: bytes,
        }
    }

    /// The value. It stays valid for as long as this borrow.
    pub fn slot(&self) -> &Slot {
        &self.slot
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        drop_owned(self.slot);
    }
}

/// The numbers a wrapper writes on a typed output, which the runner replaces at publish.
pub const UNSTAMPED: (TypeIndex, SpaceIndex) = (TypeIndex(0), SpaceIndex(0));
