//! A value one node of a plugin hands another node of the same plugin, as it is.
//!
//! Both nodes are this build's code, so the Rust value is sound to share between them, and
//! nothing is encoded. The value goes in a box of this build's own, handed over as the data of
//! a hand-over record: the host counts the record and releases it through the C release below,
//! as it does any plugin's bytes, and the record keeps the library loaded until then. The host
//! never looks inside the box.
//!
//! A reader takes a value only from a record whose bytes point at this build's own marker, so a
//! record another build made is never read as one of these. The box says its type, and a value
//! of another type is not read either. Either way the input has no usable value, and the step
//! refuses it in words.

#![cfg(feature = "alloc")]

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::any::TypeId;
use core::marker::PhantomData;
use core::ptr;
use core::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use daedalus_abi::{BytesC, HANDOVER_PENDING, HandedRecord, OWNER_HANDED, Slot, SlotState, StepCx};

/// What the bytes of every record this build hands over point at. Its address is this build's,
/// and no other build has it.
static MARKER: u8 = 0;

/// How many values are handed over and not yet released, in this build.
static HELD: AtomicUsize = AtomicUsize::new(0);

/// How many values this build has handed from one of its nodes to another and the host has not
/// yet released. Zero once no cell holds one: before the library may unmap.
pub fn inside_values_held() -> usize {
    HELD.load(Ordering::Acquire)
}

/// The box a value travels in. The header comes first, so the release can retire a box of any
/// type through it.
#[repr(C)]
struct Boxed<T> {
    retire: unsafe fn(*mut ()),
    type_id: TypeId,
    value: Option<T>,
    /// The shelf of the output that handed it, where the box goes back once released. A box on
    /// its shelf has none, so a shelf never keeps itself alive.
    home: Option<Arc<Shelf<T>>>,
}

/// Where an output keeps a released box for its next value, so a value handed every tick
/// allocates no box once the first is back. One box: a reader that holds a value longer leaves
/// the shelf empty, and the next value gets a new box.
pub struct InsideShelf<T>(Arc<Shelf<T>>);

struct Shelf<T> {
    parked: AtomicPtr<Boxed<T>>,
    _of: PhantomData<T>,
}

impl<T> Default for InsideShelf<T> {
    fn default() -> Self {
        InsideShelf(Arc::new(Shelf {
            parked: AtomicPtr::new(ptr::null_mut()),
            _of: PhantomData,
        }))
    }
}

impl<T> Drop for Shelf<T> {
    fn drop(&mut self) {
        let parked = *self.parked.get_mut();
        if !parked.is_null() {
            // SAFETY: a parked box is from `Box::into_raw`, empty, and only the shelf has it.
            drop(unsafe { Box::from_raw(parked) });
        }
    }
}

/// Drops a `Boxed<T>`'s value, and puts the box back on its shelf, or frees it when it has no
/// shelf or the shelf has one already.
///
/// # Safety
/// `data` must be a `Boxed<T>` from `Box::into_raw` that nothing else uses, retired once.
unsafe fn retire<T>(data: *mut ()) {
    let boxed = data.cast::<Boxed<T>>();
    // SAFETY: as the caller promises, the box is live and this call has it alone.
    let home = unsafe {
        (*boxed).value = None;
        (*boxed).home.take()
    };
    let parked = home.as_ref().is_some_and(|home| {
        home.parked
            .compare_exchange(ptr::null_mut(), boxed, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    });
    if !parked {
        // SAFETY: as the caller promises; the box is on no shelf.
        drop(unsafe { Box::from_raw(boxed) });
    }
    // The box's count on its shelf goes last: the last one frees a box parked on it.
    drop(home);
}

/// The release the host calls once, when the record's last count goes.
///
/// # Safety
/// `data` must be a box [`hand_inside`] made, released once.
unsafe extern "C" fn release(data: *mut ()) {
    // SAFETY: every box begins with its header, and `hand_inside` set `retire` for the box's own
    // type.
    let retire = unsafe { (*data.cast::<Boxed<()>>()).retire };
    // SAFETY: `retire` is the box's own, and this is its one call.
    unsafe { retire(data) };
    HELD.fetch_sub(1, Ordering::AcqRel);
}

/// Hands `value` over in `slot`, output `at` of the step, for a reader of this build, in the box
/// `shelf` has back when it has one. Gives the value back when the host gave no room to hand
/// output `at` over.
pub fn hand_inside<T: Send + Sync + 'static>(
    value: T,
    shelf: Option<&InsideShelf<T>>,
    cx: &mut StepCx,
    at: usize,
    slot: &mut Slot,
) -> Result<(), T> {
    let Some(room) = cx.handover_slot(at) else {
        return Err(value);
    };
    let home = shelf.map(|shelf| Arc::clone(&shelf.0));
    let parked = home.as_ref().map_or(ptr::null_mut(), |home| {
        home.parked.swap(ptr::null_mut(), Ordering::AcqRel)
    });
    let boxed = if parked.is_null() {
        Box::into_raw(Box::new(Boxed {
            retire: retire::<T>,
            type_id: TypeId::of::<T>(),
            value: Some(value),
            home,
        }))
    } else {
        // SAFETY: a box taken off the shelf is empty and this call's alone, and its header is
        // already this type's.
        unsafe {
            (*parked).value = Some(value);
            (*parked).home = home;
        }
        parked
    };
    HELD.fetch_add(1, Ordering::AcqRel);
    *room = BytesC {
        ptr: ptr::addr_of!(MARKER),
        len: 0,
        data: boxed.cast(),
        release: Some(release),
    };
    crate::put(
        slot,
        Slot {
            state: SlotState::Full as u32,
            owner: OWNER_HANDED,
            reserved: HANDOVER_PENDING,
            ..Slot::EMPTY
        },
    );
    Ok(())
}

/// The record a full handed cell holds, when this build made its value.
fn record(slot: &Slot) -> Option<&HandedRecord> {
    if slot.owner != OWNER_HANDED || slot.state != SlotState::Full as u32 {
        return None;
    }
    let record = ptr::with_exposed_provenance::<HandedRecord>(slot.cap);
    // SAFETY: a full handed cell holds a count on a live record at `cap`.
    let record = unsafe { record.as_ref()? };
    (record.bytes.ptr == ptr::addr_of!(MARKER) && !record.bytes.data.is_null()).then_some(record)
}

/// The box of type `T` a record of this build holds. `None` for a value of another type.
fn boxed<T: 'static>(record: &HandedRecord) -> Option<*mut Boxed<T>> {
    let data = record.bytes.data.cast::<Boxed<()>>();
    // SAFETY: a record of this build holds a box [`hand_inside`] made, whose header comes first.
    let type_id = unsafe { (*data).type_id };
    (type_id == TypeId::of::<T>()).then_some(record.bytes.data.cast::<Boxed<T>>())
}

/// Whether a full cell holds a value this build handed over: what a reader asks before it reads
/// the cell's bytes.
pub fn is_inside(slot: &Slot) -> bool {
    record(slot).is_some()
}

/// Borrows the value a cell holds, when this build handed it over and it is a `T`, or an
/// `Arc<T>` (a list an output wrote in place, kept to be written again).
pub fn inside_view<T: 'static>(slot: &Slot) -> Option<&T> {
    let record = record(slot)?;
    // SAFETY: the cell's count keeps the record, and so the box, alive for the borrow of the
    // cell; nothing changes a box while a cell other than the taker's holds it.
    unsafe {
        if let Some(boxed) = boxed::<T>(record) {
            return (*boxed).value.as_ref();
        }
        (*boxed::<Arc<T>>(record)?).value.as_deref()
    }
}

/// Takes the value a cell holds, when this build handed it over and it is a `T` or an `Arc<T>`:
/// moved out when the cell holds the record's only count, and cloned with `clone` otherwise. The cell is left
/// empty either way.
pub fn inside_take<T: 'static>(slot: &mut Slot, clone: Option<fn(&T) -> T>) -> Option<T> {
    let record = record(slot)?;
    let only = record.refs.load(Ordering::Acquire) == 1;
    let value = match boxed::<T>(record) {
        // SAFETY: this cell holds the record's only count, so nothing else reads the box.
        Some(boxed) if only => unsafe { (*boxed).value.take() },
        // SAFETY: as in `inside_view`.
        Some(boxed) => {
            unsafe { (*boxed).value.as_ref() }.and_then(|shared| clone.map(|clone| clone(shared)))
        }
        None => {
            let boxed = boxed::<Arc<T>>(record)?;
            // SAFETY: as in `inside_view`; the list is moved out only when nothing else holds
            // it, and its box keeps its own count otherwise.
            let shared = unsafe { (*boxed).value.as_ref() }?;
            match Arc::strong_count(shared) == 1 && only {
                // SAFETY: as above, this cell's is the only count on the box.
                true => {
                    unsafe { (*boxed).value.take() }.and_then(|list| Arc::try_unwrap(list).ok())
                }
                false => clone.map(|clone| clone(&**shared)),
            }
        }
    };
    // The cell gives up its count; the last one frees the box through `release`.
    drop(crate::take_handed(slot));
    value
}
