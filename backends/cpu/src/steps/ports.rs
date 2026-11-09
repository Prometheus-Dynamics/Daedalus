//! Reading a parameter out of its cell and writing a result into one.

use alloc::sync::Arc;
use core::ops::Deref;

use daedalus_abi::{Slot, SlotState};
use daedalus_runner::{Held, InlineCell, UNSTAMPED, put_typed, take_typed, typed_view};

use daedalus_value::OwnerKind;

use super::Kind;

const INLINE: u32 = OwnerKind::Inline as u32;

#[inline]
fn is_full(slot: &Slot) -> bool {
    slot.state == SlotState::Full as u32
}

fn constant(consts: &[Option<Held>], at: usize) -> Option<&Slot> {
    consts.get(at)?.as_ref().map(Held::slot)
}

/// Whether input `at` has a value, from its edge or a constant.
#[inline]
pub fn present(cell: &Slot, consts: &[Option<Held>], at: usize) -> bool {
    is_full(cell) || constant(consts, at).is_some()
}

/// Borrows input `at`. A small value is copied to `hold`, since its bytes may not be aligned.
#[inline]
pub fn read<'a, T: 'static>(
    cell: &'a Slot,
    consts: &'a [Option<Held>],
    at: usize,
    kind: Kind<T>,
    hold: &'a mut Option<T>,
) -> Option<&'a T> {
    let slot = if is_full(cell) {
        cell
    } else {
        constant(consts, at)?
    };
    if slot.owner == INLINE {
        *hold = (kind.read?)(slot);
        return hold.as_ref();
    }
    typed_view::<T>(slot)
}

/// Takes input `at` by value. A constant is copied, since it stays for the next tick.
#[inline]
pub fn take<T: 'static>(
    cell: &mut Slot,
    consts: &[Option<Held>],
    at: usize,
    kind: Kind<T>,
) -> Option<T> {
    if !is_full(cell) {
        let slot = constant(consts, at)?;
        if slot.owner == INLINE {
            return (kind.read?)(slot);
        }
        return typed_view::<T>(slot).and_then(|value| kind.clone.map(|clone| clone(value)));
    }
    if cell.owner == INLINE {
        return (kind.read?)(cell);
    }
    match Arc::try_unwrap(take_typed::<T>(cell)?) {
        Ok(value) => Some(value),
        Err(shared) => kind.clone.map(|clone| clone(&shared)),
    }
}

/// Stores `value` in `slot`: in `buf` if it's small, otherwise behind a reference.
#[inline]
pub fn encode<T: Send + Sync + 'static>(
    value: T,
    kind: Kind<T>,
    buf: &mut InlineCell,
    slot: &mut Slot,
) {
    match kind.write {
        Some(write) => write(&value, buf, slot),
        None => put_typed(slot, UNSTAMPED.0, UNSTAMPED.1, Arc::new(value)),
    }
}

/// Records why a step's input was there but unusable.
#[cold]
pub fn unusable(cx: &mut daedalus_abi::StepCx, node: u32) -> daedalus_abi::StepEnd {
    cx.fault(Some(node), "an input held a value of another form");
    daedalus_abi::StepEnd::Fault
}

/// The values on a fan-in input, in port number order.
pub struct FanIn<'a, T: 'static> {
    cells: &'a [Slot],
    kind: Kind<T>,
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
        FanIn { cells, kind }
    }

    /// How many ports this fan-in has in this graph, with or without a value this tick.
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// The value on port `index`, if there is one.
    pub fn get(&self, index: usize) -> Option<FanItem<'a, T>> {
        item(self.cells.get(index)?, self.kind)
    }

    /// Every value present, in port number order.
    pub fn iter(&self) -> impl Iterator<Item = FanItem<'a, T>> + use<'a, '_, T> {
        self.cells.iter().filter_map(|cell| item(cell, self.kind))
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
