//! Owned values in raw slots: encoding a `Value` into a `Slot`, decoding it back, and releasing
//! what a slot owns.
//!
//! A queue entry owns its payload. A `Full` cell of kind Shared or Typed owns one reference. A
//! step's view of an input is a copy that owns nothing; the queue still owns the entry until the
//! step consumes it. Bytes of an inline value live in the runner's inline buffer for that entry.

use core::ptr;

use daedalus_abi::{Handle, Slot, SlotState};
use daedalus_types::{SlotIndex, SpaceIndex, TypeIndex};
use daedalus_value::{InlineBytes, Owner, OwnerKind, Value};

/// Bytes an inline value can hold, and so the size of one inline buffer.
pub const INLINE_BYTES: usize = 16;

/// The bytes of one inline value, one per queue entry.
pub type InlineCell = [u8; INLINE_BYTES];

/// A typed payload that cannot be encoded from a `Value`: its concrete type is erased there, and
/// the raw form needs it. Typed payloads enter through the typed feed instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Untyped;

fn kind_word(kind: OwnerKind) -> u32 {
    kind as u32
}

/// Encodes `value` as a raw slot. An inline value's bytes are copied into `buf`, and the slot points
/// there.
pub fn encode(value: Value, buf: &mut InlineCell) -> Result<Slot, Untyped> {
    let Value { ty, space, owner } = value;
    let kind = owner.kind();
    let (data, len, cap) = match owner {
        Owner::Static(bytes) => (bytes.as_ptr().cast_mut(), bytes.len(), bytes.len()),
        Owner::Inline(inline) => {
            let bytes = inline.as_slice();
            buf[..bytes.len()].copy_from_slice(bytes);
            (buf.as_mut_ptr(), bytes.len(), bytes.len())
        }
        Owner::Slot(index) => (ptr::null_mut(), index.0 as usize, 0),
        Owner::Host(handle) => (
            ptr::without_provenance_mut(handle.index() as usize),
            handle.generation() as usize,
            0,
        ),
        #[cfg(feature = "alloc")]
        Owner::Shared(bytes) => {
            let raw = alloc::sync::Arc::into_raw(bytes);
            (raw.cast::<u8>().cast_mut(), raw.len(), raw.len())
        }
        #[cfg(feature = "alloc")]
        Owner::Typed(_) => return Err(Untyped),
    };
    Ok(Slot {
        state: SlotState::Full as u32,
        space: u32::from(space.0),
        type_index: ty.0,
        owner: kind_word(kind),
        version: 0,
        reserved: 0,
        data,
        len,
        cap,
    })
}

/// Copies an inline value's bytes into `buf` and points the slot at them. Other kinds pass through.
/// Used when a body's output moves into a queue, so the bytes outlive the step's own buffer.
pub fn place(mut slot: Slot, buf: &mut InlineCell) -> Slot {
    if slot.owner == kind_word(OwnerKind::Inline) {
        let len = slot.len.min(INLINE_BYTES);
        // SAFETY: a body writes an inline value's `data` pointer to bytes valid for `len` bytes until
        // the step returns, and this runs before the step's bytes can go away. `buf` is exclusive
        // and holds `INLINE_BYTES` bytes, so `len` fits in it.
        unsafe { ptr::copy(slot.data, buf.as_mut_ptr(), len) };
        slot.data = buf.as_mut_ptr();
        slot.len = len;
        slot.cap = len;
    }
    slot
}

/// Takes what a raw slot owns as a `Value`.
///
/// # Safety
///
/// `slot` must be a `Full` queue entry or a cell the caller owns, and this call takes over its
/// reference, so the caller must not release it again.
// SAFETY: the caller upholds the `# Safety` contract documented above.
pub unsafe fn decode(slot: &Slot) -> Value {
    let owner = match slot.owner {
        x if x == kind_word(OwnerKind::Static) => {
            // SAFETY: a static payload's bytes live forever, and the slot names them by `data` and `len`.
            Owner::Static(unsafe { core::slice::from_raw_parts(slot.data, slot.len) })
        }
        x if x == kind_word(OwnerKind::Inline) => {
            // SAFETY: an inline payload's bytes are valid for `len` bytes, as `encode` or `place` left them.
            let bytes = unsafe { core::slice::from_raw_parts(slot.data.cast_const(), slot.len) };
            Owner::Inline(InlineBytes::from_slice(bytes).unwrap_or_default())
        }
        x if x == kind_word(OwnerKind::Slot) => Owner::Slot(SlotIndex(slot.len as u32)),
        x if x == kind_word(OwnerKind::Host) => {
            Owner::Host(Handle::new(slot.data.addr() as u32, slot.len as u32))
        }
        #[cfg(feature = "alloc")]
        x if x == kind_word(OwnerKind::Shared) => {
            // SAFETY: a shared payload came from `Arc::into_raw` on an `Arc<[u8]>` of `len` bytes, and
            // the caller's reference moves into the returned value.
            let bytes = unsafe {
                alloc::sync::Arc::from_raw(ptr::slice_from_raw_parts(
                    slot.data.cast_const(),
                    slot.len,
                ))
            };
            Owner::Shared(bytes)
        }
        #[cfg(feature = "alloc")]
        x if x == kind_word(OwnerKind::Typed) => {
            // SAFETY: a typed payload's table was written with the payload, so it matches the type.
            let ops = unsafe { typed_ops(slot.cap) };
            // SAFETY: the caller's reference moves into the rebuilt `Arc`.
            Owner::Typed(unsafe { (ops.rebuild)(slot.data) })
        }
        _ => Owner::Static(&[]),
    };
    Value {
        ty: TypeIndex(slot.type_index),
        space: SpaceIndex(slot.space as u16),
        owner,
    }
}

/// Releases what a raw slot owns, if it owns anything. Once per reference.
///
/// # Safety
///
/// `slot` must be a `Full` entry that owns its reference, and nothing else may release it.
// SAFETY: the caller upholds the `# Safety` contract documented above.
pub unsafe fn release(slot: &Slot) {
    #[cfg(feature = "alloc")]
    {
        if slot.owner == kind_word(OwnerKind::Shared) {
            // SAFETY: the slot owns the reference, and the caller releases it only once.
            drop(unsafe {
                alloc::sync::Arc::from_raw(ptr::slice_from_raw_parts(
                    slot.data.cast_const(),
                    slot.len,
                ))
            });
        } else if slot.owner == kind_word(OwnerKind::Typed) {
            // SAFETY: the table was written with the payload, and the slot owns the reference.
            let ops = unsafe { typed_ops(slot.cap) };
            // SAFETY: as above, the reference is released once.
            unsafe { (ops.release)(slot.data) };
        }
    }
    #[cfg(not(feature = "alloc"))]
    let _ = slot;
}

#[cfg(feature = "alloc")]
pub use typed::{TypedOps, typed_ops, typed_raw};

#[cfg(feature = "alloc")]
mod typed {
    use alloc::sync::Arc;
    use core::any::Any;
    use core::marker::PhantomData;
    use core::ptr;

    use daedalus_abi::{Slot, SlotState};
    use daedalus_types::{SpaceIndex, TypeIndex};

    use super::kind_word;
    use daedalus_value::OwnerKind;

    /// The two functions a typed payload needs, monomorphised for its concrete type `T`. A slot's
    /// `cap` holds the address of one of these.
    pub struct TypedOps {
        /// Drops the reference, and nothing else.
        pub(super) release: unsafe fn(*mut u8),
        /// Takes another reference, and nothing else.
        pub(super) retain: unsafe fn(*mut u8),
        /// Turns the reference into an `Arc<dyn Any>` for a `Value`.
        pub(super) rebuild: unsafe fn(*mut u8) -> Arc<dyn Any + Send + Sync>,
    }

    struct Ops<T>(PhantomData<fn() -> T>);

    impl<T: Send + Sync + 'static> Ops<T> {
        const OPS: TypedOps = TypedOps {
            release: release_typed::<T>,
            retain: retain_typed::<T>,
            rebuild: rebuild_typed::<T>,
        };
    }

    /// # Safety
    ///
    /// `data` must come from `Arc::<T>::into_raw`, and be released once.
    // SAFETY: the caller upholds the `# Safety` contract documented above.
    unsafe fn release_typed<T>(data: *mut u8) {
        // SAFETY: as the caller promises, `data` is one reference's worth from `Arc::into_raw`.
        drop(unsafe { Arc::from_raw(data.cast::<T>().cast_const()) });
    }

    /// # Safety
    ///
    /// `data` must come from `Arc::<T>::into_raw` and still be live.
    // SAFETY: the caller upholds the `# Safety` contract documented above.
    unsafe fn retain_typed<T>(data: *mut u8) {
        // SAFETY: as the function's contract says, `data` is a live `Arc<T>` pointer.
        unsafe { Arc::increment_strong_count(data.cast_const().cast::<T>()) };
    }

    /// # Safety
    ///
    /// As `release_typed`, and the reference moves into the returned `Arc`.
    // SAFETY: the caller upholds the `# Safety` contract documented above.
    unsafe fn rebuild_typed<T: Send + Sync + 'static>(data: *mut u8) -> Arc<dyn Any + Send + Sync> {
        // SAFETY: as the caller promises.
        unsafe { Arc::from_raw(data.cast::<T>().cast_const()) }
    }

    /// The table for `T`, read back from a slot's `cap`.
    ///
    /// # Safety
    ///
    /// `cap` must have been written by [`typed_raw`] for a payload whose table this is.
    // SAFETY: the caller upholds the `# Safety` contract documented above.
    pub unsafe fn typed_ops(cap: usize) -> &'static TypedOps {
        // SAFETY: `typed_raw` wrote the address of a `'static` table, exposed with `as usize`.
        unsafe { &*ptr::with_exposed_provenance::<TypedOps>(cap) }
    }

    /// The raw form of `value` for a slot of type `ty` in `space`. The reference moves into the
    /// slot, and the slot releases it.
    pub fn typed_raw<T: Send + Sync + 'static>(
        ty: TypeIndex,
        space: SpaceIndex,
        value: Arc<T>,
    ) -> Slot {
        let table: &'static TypedOps = &Ops::<T>::OPS;
        Slot {
            state: SlotState::Full as u32,
            space: u32::from(space.0),
            type_index: ty.0,
            owner: kind_word(OwnerKind::Typed),
            version: 0,
            reserved: 0,
            data: Arc::into_raw(value).cast::<u8>().cast_mut(),
            len: 0,
            cap: (table as *const TypedOps).expose_provenance(),
        }
    }
}

/// Takes a new reference to what `slot` owns, so a second holder can release it independently.
///
/// # Safety
///
/// `slot` must be a `Full` queue entry or a cell the caller owns.
pub unsafe fn retain(slot: &Slot) {
    #[cfg(feature = "alloc")]
    {
        if slot.owner == kind_word(OwnerKind::Shared) {
            let raw = ptr::slice_from_raw_parts(slot.data.cast_const(), slot.len);
            // SAFETY: the slot owns a reference to a live `Arc<[u8]>`.
            unsafe { alloc::sync::Arc::increment_strong_count(raw) };
        } else if slot.owner == kind_word(OwnerKind::Typed) {
            // SAFETY: the table was written with the payload.
            let ops = unsafe { typed_ops(slot.cap) };
            // SAFETY: the slot owns a reference, and `retain` takes another.
            unsafe { (ops.retain)(slot.data) };
        }
    }
    #[cfg(not(feature = "alloc"))]
    let _ = slot;
}
