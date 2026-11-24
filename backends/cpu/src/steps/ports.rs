//! Reading a parameter out of its cell and writing a result into one.

use core::ops::Deref;

#[cfg(feature = "alloc")]
use alloc::sync::Arc;
use daedalus_abi::{Slot, SlotState};
use daedalus_runner::InlineCell;
#[cfg(feature = "alloc")]
use daedalus_runner::{UNSTAMPED, put_typed, take_typed, typed_view};

use daedalus_value::OwnerKind;

#[cfg(feature = "alloc")]
use super::Wire;
use super::keep::{Keep, Keeper, take_carried};
use super::{ConstTable, ItemsView, Kind, Listed};

/// Whether the wire mask says fixed input `at` reads `cpu.wire`. Never, in a build with no
/// allocator: it has no `cpu.wire`, and the check folds away.
#[inline]
fn wired(wire: u32, at: usize) -> bool {
    cfg!(feature = "alloc") && wire & wire_bit(at) != 0
}

/// The bit of a step's wire mask for its fan-in ports, all of which read `cpu.wire` or none.
pub const FAN_IN_WIRE: u32 = 1 << 31;

/// The bit of a step's wire mask for fixed input `at`: set when the plan has the input read a
/// value in `cpu.wire`, whose cells hold their type's wire form. Decided at compile time, so a
/// cell's stamp is never consulted.
#[inline]
pub const fn wire_bit(at: usize) -> u32 {
    match at {
        0..31 => 1 << at,
        _ => 0,
    }
}

/// A value read from a `cpu.wire` cell by its type's `WireForm`. `None` for a type with no wire
/// form, or bytes that aren't one.
#[inline]
fn read_wire<T>(slot: &Slot, kind: Kind<T>) -> Option<T> {
    let wire = kind.wire?;
    (wire.read)(slot.bytes()?)
}

const INLINE: u32 = OwnerKind::Inline as u32;

/// The value a cell holds in place: in the type's own in-place form, or, for a type that has
/// none, its wire form, which a node of the same plugin writes in place when it fits.
#[inline]
fn inline_value<T>(slot: &Slot, kind: Kind<T>) -> Option<T> {
    match kind.read {
        Some(read) => read(slot),
        None => read_wire(slot, kind),
    }
}

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
    wire: u32,
) -> Option<&'a T> {
    let slot = if is_full(cell) {
        cell
    } else {
        consts.slot(at)?
    };
    // A constant is never a wire form.
    if wired(wire, at) && is_full(cell) {
        *hold = read_wire(slot, kind);
        return hold.as_ref();
    }
    if slot.owner == INLINE {
        *hold = inline_value(slot, kind);
        return hold.as_ref();
    }
    // A value another node of this plugin handed over as it is, a handle among them.
    #[cfg(feature = "alloc")]
    if daedalus_runner::is_inside(slot) {
        return daedalus_runner::inside_view::<T>(slot);
    }
    // A handle is read as another handle to the same value, kept for the call.
    if let Some(handle) = kind.handle {
        *hold = (handle.share)(slot);
        return hold.as_ref();
    }
    if slot.owner == daedalus_abi::OWNER_HANDED {
        *hold = (kind.from_bytes?)(slot.bytes()?);
        return hold.as_ref();
    }
    typed_view::<T>(slot)
}

/// Borrows input `at`, a list, as a slice of its items. A list in `cpu.wire` whose items are
/// numbers is viewed where its bytes are, when they are aligned on this machine: `view` says how.
/// Any other is read as [`read`] reads it.
#[inline]
pub fn read_items<'a, L: Listed + 'static, C: ConstTable>(
    cell: &'a Slot,
    consts: &'a C,
    at: usize,
    kind: Kind<L>,
    view: ItemsView<L::Item>,
    hold: &'a mut Option<L>,
    wire: u32,
) -> Option<&'a [L::Item]> {
    let slot = if is_full(cell) {
        cell
    } else {
        consts.slot(at)?
    };
    if wired(wire, at)
        && is_full(cell)
        && let Some(view) = view
        && let Some(items) = slot.bytes().and_then(view)
    {
        return Some(items);
    }
    read(cell, consts, at, kind, hold, wire).map(Listed::items)
}

/// Reads input `at`, a list, as a [`ListView`](super::ListView): item by item from its wire form
/// where it is, when it is in `cpu.wire` and its items have a fixed size. Any other is read as
/// [`read`] reads it, and the view borrows the list.
#[cfg(feature = "alloc")]
#[inline]
pub fn read_list<'a, T: daedalus_types::WireForm + Clone + 'static, C: ConstTable>(
    cell: &'a Slot,
    consts: &'a C,
    at: usize,
    kind: Kind<alloc::vec::Vec<T>>,
    hold: &'a mut Option<alloc::vec::Vec<T>>,
    wire: u32,
) -> Option<super::ListView<'a, T>> {
    if wired(wire, at)
        && is_full(cell)
        && let Some(view) = cell.bytes().and_then(super::ListView::wire)
    {
        return Some(view);
    }
    read(cell, consts, at, kind, hold, wire).map(|list| super::ListView::of(list))
}

/// Takes the handed bytes a full cell holds, by move.
pub(crate) fn take_handed_bytes(cell: &mut Slot) -> Option<daedalus_abi::SharedBytes> {
    daedalus_runner::take_handed(cell)
}

/// Another handle on the handed bytes a full cell holds. The cell keeps its own.
pub(crate) fn share_handed_bytes(cell: &Slot) -> Option<daedalus_abi::SharedBytes> {
    daedalus_runner::share_handed(cell)
}

/// Puts handed bytes in a cell as they are: the handle's count moves into it.
pub(crate) fn put_handed_bytes(value: daedalus_abi::SharedBytes, slot: &mut Slot) {
    // The unstamped indexes: a handed value keeps the type and space its producer gave it.
    daedalus_runner::put_handed(
        slot,
        daedalus_types::TypeIndex(0),
        daedalus_types::SpaceIndex(0),
        value,
    );
}

/// The bytes of a handed cell, for a parameter that is copied out of them.
fn handed_bytes(slot: &Slot) -> Option<&[u8]> {
    if slot.owner != daedalus_abi::OWNER_HANDED {
        return None;
    }
    slot.bytes()
}

/// Takes input `at` by value. A constant is copied, since it stays for the next tick.
#[inline]
pub fn take<T: 'static, C: ConstTable>(
    cell: &mut Slot,
    consts: &C,
    at: usize,
    kind: Kind<T>,
    wire: u32,
) -> Option<T> {
    if is_full(cell) && wired(wire, at) {
        return read_wire(cell, kind);
    }
    // A value another node of this plugin handed over as it is, a handle among them.
    #[cfg(feature = "alloc")]
    if is_full(cell) && daedalus_runner::is_inside(cell) {
        return daedalus_runner::inside_take::<T>(cell, kind.clone);
    }
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
        if let Some(bytes) = handed_bytes(slot) {
            return (kind.from_bytes?)(bytes);
        }
        return typed_view::<T>(slot).and_then(|value| kind.clone.map(|clone| clone(value)));
    }
    if cell.owner == daedalus_abi::OWNER_HANDED {
        // A by-value parameter that is not a handle copies the bytes out: the one copy a handed
        // value makes.
        return (kind.from_bytes?)(cell.bytes()?);
    }
    if cell.owner == INLINE {
        return inline_value(cell, kind);
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

/// Stores `value` in `slot`, output `at` of the step: in `buf` if it's small, otherwise behind a
/// reference that `held` keeps as `keep` says.
#[inline]
#[allow(clippy::too_many_arguments)] // what the expansion has at hand for one output
pub fn encode<T: Send + Sync + 'static, S: Keeper<T>>(
    value: T,
    kind: Kind<T>,
    keep: Keep,
    held: &mut S,
    buf: &mut InlineCell,
    slot: &mut Slot,
    cx: &mut daedalus_abi::StepCx,
    at: usize,
) {
    #[cfg(feature = "alloc")]
    if keep == Keep::Wire
        && let Some(wire) = kind.wire
    {
        return write_wire(&value, wire, held.wire_buffers(), buf, slot);
    }
    // A value written in its cell is the same in any build. Anything else a node of this plugin
    // reads goes in a box of this build's, a handle too: the host releases the box through the
    // plugin while the plugin is loaded, and never runs this build's code on a cell itself.
    #[cfg(feature = "alloc")]
    let value = if keep == Keep::Carried && kind.write.is_none() {
        // A wire form that fits in the cell is written there: less work than a box, and the
        // reader reads it as it reads any value in place.
        if let Some(wire) = kind.wire
            && (wire.len)(&value) <= buf.len()
        {
            return write_wire(&value, wire, None, buf, slot);
        }
        match daedalus_runner::hand_inside(value, held.inside_shelf(), cx, at, slot) {
            Ok(()) => return,
            Err(value) => value,
        }
    } else {
        value
    };
    #[cfg(not(feature = "alloc"))]
    let _ = (cx, at);
    if let Some(handle) = kind.handle {
        return (handle.put)(value, slot);
    }
    match kind.write {
        Some(write) => write(&value, buf, slot),
        None => held.publish(value, keep, slot),
    }
}

/// Writes `value`'s wire form, for an output placed in `cpu.wire`: in the cell's own bytes when it
/// fits, and in shared bytes otherwise. Shared bytes come from `buffers` when one of the same
/// length is no longer read by anyone, so a stream of values of one size allocates nothing once
/// warm. Otherwise they are allocated once, and kept.
#[cfg(feature = "alloc")]
fn write_wire<T>(
    value: &T,
    wire: Wire<T>,
    mut buffers: Option<&mut super::keep::WireBuffers>,
    buf: &mut InlineCell,
    slot: &mut Slot,
) {
    let len = (wire.len)(value);
    if let Some(room) = buf.get_mut(..len) {
        let written = (wire.write)(value, room);
        let _ = daedalus_runner::put_inline(slot, UNSTAMPED.0, UNSTAMPED.1, &buf[..written]);
        return;
    }
    let free = buffers.as_deref_mut().and_then(|buffers| {
        let at = buffers
            .iter()
            .position(|held| held.len() == len && Arc::strong_count(held) == 1)?;
        Some(buffers.swap_remove(at))
    });
    let mut bytes: Arc<[u8]> = free.unwrap_or_else(|| core::iter::repeat_n(0, len).collect());
    let Some(room) = Arc::get_mut(&mut bytes) else {
        return;
    };
    let written = (wire.write)(value, room);
    // A wire form is as long as its type said it would be. One that isn't is kept to its length.
    if written != len {
        bytes = Arc::from(&room[..written]);
    }
    if let Some(buffers) = buffers {
        buffers.push(bytes.clone());
        if buffers.len() > super::keep::POOL_LIMIT {
            buffers.swap_remove(0);
        }
    }
    daedalus_runner::put_shared(slot, UNSTAMPED.0, UNSTAMPED.1, bytes);
}

/// Takes the handle a cell holds. A value kept in a carrier is moved into a new handle.
#[cfg(feature = "alloc")]
pub(crate) fn take_handle<U: Send + Sync + 'static>(cell: &mut Slot) -> Option<Arc<U>> {
    if let Some(handle) = take_typed::<U>(cell) {
        return Some(handle);
    }
    take_carried::<U>(cell)?.map(Arc::new)
}

/// Another handle to what a cell holds, leaving the cell's own where it is.
#[cfg(feature = "alloc")]
pub(crate) fn share_handle<U: Send + Sync + 'static>(cell: &Slot) -> Option<Arc<U>> {
    daedalus_runner::share_typed(cell)?.downcast::<U>().ok()
}

/// Puts a handle in a cell as it is: the value isn't moved and no handle is made.
#[cfg(feature = "alloc")]
pub(crate) fn put_handle<U: Send + Sync + 'static>(value: Arc<U>, slot: &mut Slot) {
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
    /// The cells, how their type is held, and whether they are in `cpu.wire`.
    Cells(&'a [Slot], Kind<T>, bool),
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
    pub fn new(cells: &'a [Slot], kind: Kind<T>, wire: u32) -> FanIn<'a, T> {
        FanIn {
            from: Fan::Cells(
                cells,
                kind,
                cfg!(feature = "alloc") && wire & FAN_IN_WIRE != 0,
            ),
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
            Fan::Cells(cells, ..) => cells.len(),
            Fan::Values(values) => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The value on port `index`, if there is one.
    pub fn get(&self, index: usize) -> Option<FanItem<'a, T>> {
        match self.from {
            Fan::Cells(cells, kind, wire) => item(cells.get(index)?, kind, wire),
            Fan::Values(values) => values.get(index)?.as_ref().map(FanItem::Borrowed),
        }
    }

    /// Every value present, in port number order.
    pub fn iter(&self) -> impl Iterator<Item = FanItem<'a, T>> + use<'a, '_, T> {
        (0..self.len()).filter_map(|index| self.get(index))
    }
}

fn item<'a, T: 'static>(cell: &'a Slot, kind: Kind<T>, wire: bool) -> Option<FanItem<'a, T>> {
    if !is_full(cell) {
        return None;
    }
    if wire {
        return read_wire(cell, kind).map(FanItem::Copied);
    }
    if cell.owner == INLINE {
        return (kind.read?)(cell).map(FanItem::Copied);
    }
    typed_view::<T>(cell).map(FanItem::Borrowed)
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use super::*;
    use crate::steps::WireOf;

    #[test]
    fn a_wire_buffer_is_written_over_once_nobody_reads_it() {
        let wire = WireOf::<Vec<f32>>::WIRE.expect("a list has a wire form");
        let value = alloc::vec![1.5f32; 64];
        let (mut buffers, mut buf) = (Vec::new(), [0u8; daedalus_runner::INLINE_BYTES]);
        let mut first = Slot::EMPTY;
        write_wire(&value, wire, Some(&mut buffers), &mut buf, &mut first);
        assert_eq!(first.bytes().map(<[u8]>::len), Some(4 + 64 * 4));
        // Still read: the next value gets a buffer of its own.
        let mut second = Slot::EMPTY;
        write_wire(&value, wire, Some(&mut buffers), &mut buf, &mut second);
        assert_ne!(first.data, second.data);
        // Let go of: the next value is written over it.
        let at = first.data;
        daedalus_runner::drop_owned(first);
        let mut third = Slot::EMPTY;
        write_wire(&value, wire, Some(&mut buffers), &mut buf, &mut third);
        assert_eq!(third.data, at);
        assert_eq!(third.bytes(), second.bytes());
        // A value of another length gets a buffer of its own.
        let mut other = Slot::EMPTY;
        write_wire(
            &alloc::vec![2.0f32; 65],
            wire,
            Some(&mut buffers),
            &mut buf,
            &mut other,
        );
        assert_eq!(other.bytes().map(<[u8]>::len), Some(4 + 65 * 4));
        for slot in [second, third, other] {
            daedalus_runner::drop_owned(slot);
        }
    }
}
