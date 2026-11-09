//! Reading and writing a small value kept in the cell's own bytes. Needs no allocator.

use core::mem::size_of;
use core::ptr;

use daedalus_abi::{Slot, SlotState};
use daedalus_value::OwnerKind;

use crate::owned::{INLINE_BYTES, InlineCell};

/// The `T` an inline cell holds. `None` if the cell isn't inline or isn't a `T`'s size.
pub fn read_inline<T: Copy>(slot: &Slot) -> Option<T> {
    let fits = slot.owner == OwnerKind::Inline as u32 && slot.len == size_of::<T>();
    // SAFETY: an inline cell's `data` is valid for `len` bytes, which is a `T`'s size. `T` is
    // `Copy`, and the plan proved this port carries a `T`, so these bytes are one.
    fits.then(|| unsafe { ptr::read_unaligned(slot.data.cast::<T>().cast_const()) })
}

/// Writes `value` into `slot` as an inline value held in `buf`. The runner stamps its type.
///
/// # Panics
///
/// If a `T` doesn't fit in [`INLINE_BYTES`].
pub fn write_inline<T: Copy>(value: &T, buf: &mut InlineCell, slot: &mut Slot) {
    assert!(size_of::<T>() <= INLINE_BYTES);
    // SAFETY: `buf` holds `INLINE_BYTES` bytes, which is at least a `T`, and doesn't overlap the
    // caller's `value`. Copying raw bytes keeps any padding uninitialized, as it was.
    unsafe {
        ptr::copy_nonoverlapping(
            (value as *const T).cast::<u8>(),
            buf.as_mut_ptr(),
            size_of::<T>(),
        )
    };
    store(
        slot,
        Slot {
            state: SlotState::Full as u32,
            space: 0,
            type_index: 0,
            owner: OwnerKind::Inline as u32,
            version: 0,
            reserved: 0,
            data: buf.as_mut_ptr(),
            len: size_of::<T>(),
            cap: size_of::<T>(),
        },
    );
}

/// Replaces `slot`, releasing whatever it held.
#[cfg(feature = "alloc")]
#[inline]
fn store(slot: &mut Slot, value: Slot) {
    crate::values::put(slot, value);
}

/// Replaces `slot`. With no allocator, cells only ever hold inline values, so nothing is released.
#[cfg(not(feature = "alloc"))]
#[inline]
fn store(slot: &mut Slot, value: Slot) {
    *slot = value;
}
