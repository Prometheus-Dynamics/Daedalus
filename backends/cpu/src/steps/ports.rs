//! Reading a parameter out of its cell and writing a result into one.

use core::ops::Deref;

#[cfg(feature = "alloc")]
use alloc::sync::Arc;
use daedalus_abi::{Slot, SlotState};
use daedalus_runner::InlineCell;
#[cfg(feature = "alloc")]
use daedalus_runner::{UNSTAMPED, put_typed, take_typed, typed_view};

use daedalus_value::OwnerKind;

use super::keep::{Keep, Keeper, take_carried};
use super::{ConstTable, Kind};

const INLINE: u32 = OwnerKind::Inline as u32;

/// A value held behind a reference. There are none without an allocator.
#[cfg(not(feature = "alloc"))]
fn typed_view<T: 'static>(_: &Slot) -> Option<&T> {
    None
}

#[inline]
fn is_full(slot: &Slot) -> bool {
    slot.state == SlotState::Full as u32
}

/// Whether input `at` has a value, from its edge or a constant.
#[inline]
pub fn present<C: ConstTable>(cell: &Slot, consts: &C, at: usize) -> bool {
    is_full(cell) || consts.slot(at).is_some()
}

/// Borrows input `at`. A small value is copied to `hold`, since its bytes may not be aligned.
#[inline]
pub fn read<'a, T: 'static, C: ConstTable>(
    cell: &'a Slot,
    consts: &'a C,
    at: usize,
    kind: Kind<T>,
    hold: &'a mut Option<T>,
) -> Option<&'a T> {
    let slot = if is_full(cell) {
        cell
    } else {
        consts.slot(at)?
    };
    if slot.owner == INLINE {
        *hold = (kind.read?)(slot);
        return hold.as_ref();
    }
    // A handle is read as another handle to the same value, kept for the call.
    if let Some(handle) = kind.handle {
        *hold = (handle.share)(slot);
        return hold.as_ref();
    }
    typed_view::<T>(slot)
}

/// Takes input `at` by value. A constant is copied, since it stays for the next tick.
#[inline]
pub fn take<T: 'static, C: ConstTable>(
    cell: &mut Slot,
    consts: &C,
    at: usize,
    kind: Kind<T>,
) -> Option<T> {
    if let Some(handle) = kind.handle {
        // A constant stays for the next tick, so it is shared. An edge's handle is taken as it
        // is: nothing is unwrapped and nothing is made.
        return match is_full(cell) {
            true => (handle.take)(cell),
            false => (handle.share)(consts.slot(at)?),
        };
    }
    if !is_full(cell) {
        let slot = consts.slot(at)?;
        if slot.owner == INLINE {
            return (kind.read?)(slot);
        }
        return typed_view::<T>(slot).and_then(|value| kind.clone.map(|clone| clone(value)));
    }
    if cell.owner == INLINE {
        return (kind.read?)(cell);
    }
    // A value the planner kept in a carrier is moved out of it, whatever its type's `Clone`.
    if let Some(taken) = take_carried::<T>(cell) {
        return taken;
    }
    if kind.copy {
        return typed_view::<T>(cell).and_then(|value| kind.clone.map(|clone| clone(value)));
    }
    take_shared(cell, kind)
}

#[cfg(feature = "alloc")]
fn take_shared<T: 'static>(cell: &mut Slot, kind: Kind<T>) -> Option<T> {
    match Arc::try_unwrap(take_typed::<T>(cell)?) {
        Ok(value) => Some(value),
        Err(shared) => kind.clone.map(|clone| clone(&shared)),
    }
}

#[cfg(not(feature = "alloc"))]
fn take_shared<T: 'static>(_: &mut Slot, _: Kind<T>) -> Option<T> {
    None
}

/// Stores `value` in `slot`: in `buf` if it's small, otherwise behind a reference that `held`
/// keeps as `keep` says.
#[inline]
pub fn encode<T: Send + Sync + 'static, S: Keeper<T>>(
    value: T,
    kind: Kind<T>,
    keep: Keep,
    held: &mut S,
    buf: &mut InlineCell,
    slot: &mut Slot,
) {
    if let Some(handle) = kind.handle {
        return (handle.put)(value, slot);
    }
    match kind.write {
        Some(write) => write(&value, buf, slot),
        None => held.publish(value, keep, slot),
    }
}

/// Takes the handle a cell holds. A value kept in a carrier is moved into a new handle.
#[cfg(feature = "alloc")]
pub fn take_handle<U: Send + Sync + 'static>(cell: &mut Slot) -> Option<Arc<U>> {
    if let Some(handle) = take_typed::<U>(cell) {
        return Some(handle);
    }
    take_carried::<U>(cell)?.map(Arc::new)
}

/// Another handle to what a cell holds, leaving the cell's own where it is.
#[cfg(feature = "alloc")]
pub fn share_handle<U: Send + Sync + 'static>(cell: &Slot) -> Option<Arc<U>> {
    daedalus_runner::share_typed(cell)?.downcast::<U>().ok()
}

/// Puts a handle in a cell as it is: the value isn't moved and no handle is made.
#[cfg(feature = "alloc")]
pub fn put_handle<U: Send + Sync + 'static>(value: Arc<U>, slot: &mut Slot) {
    put_typed(slot, UNSTAMPED.0, UNSTAMPED.1, value);
}

/// Records why a step's input was there but unusable.
#[cold]
pub fn unusable(cx: &mut daedalus_abi::StepCx, node: u32) -> daedalus_abi::StepEnd {
    cx.fault(Some(node), "an input held a value of another form");
    daedalus_abi::StepEnd::Fault
}

/// The values on a fan-in input, in port number order.
pub struct FanIn<'a, T: 'static> {
    from: Fan<'a, T>,
}

enum Fan<'a, T: 'static> {
    Cells(&'a [Slot], Kind<T>),
    Values(&'a [Option<T>]),
}

/// One value of a fan-in.
pub enum FanItem<'a, T> {
    Copied(T),
    Borrowed(&'a T),
}

impl<T> Deref for FanItem<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        match self {
            FanItem::Copied(value) => value,
            FanItem::Borrowed(value) => value,
        }
    }
}

impl<'a, T: 'static> FanIn<'a, T> {
    #[doc(hidden)]
    pub fn new(cells: &'a [Slot], kind: Kind<T>) -> FanIn<'a, T> {
        FanIn {
            from: Fan::Cells(cells, kind),
        }
    }

    /// A fan-in over values already at hand, for generated code.
    pub fn values(values: &'a [Option<T>]) -> FanIn<'a, T> {
        FanIn {
            from: Fan::Values(values),
        }
    }

    /// How many ports this fan-in has in this graph, with or without a value this tick.
    pub fn len(&self) -> usize {
        match self.from {
            Fan::Cells(cells, _) => cells.len(),
            Fan::Values(values) => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The value on port `index`, if there is one.
    pub fn get(&self, index: usize) -> Option<FanItem<'a, T>> {
        match self.from {
            Fan::Cells(cells, kind) => item(cells.get(index)?, kind),
            Fan::Values(values) => values.get(index)?.as_ref().map(FanItem::Borrowed),
        }
    }

    /// Every value present, in port number order.
    pub fn iter(&self) -> impl Iterator<Item = FanItem<'a, T>> + use<'a, '_, T> {
        (0..self.len()).filter_map(|index| self.get(index))
    }
}

fn item<'a, T: 'static>(cell: &'a Slot, kind: Kind<T>) -> Option<FanItem<'a, T>> {
    if !is_full(cell) {
        return None;
    }
    if cell.owner == INLINE {
        return (kind.read?)(cell).map(FanItem::Copied);
    }
    typed_view::<T>(cell).map(FanItem::Borrowed)
}
