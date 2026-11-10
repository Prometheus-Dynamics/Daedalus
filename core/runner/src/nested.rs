//! Running a step list inside a step, and the values a control node passes around.
//!
//! An input is a view: a copy of the cell that owns nothing, as for any step. An output is a `Full`
//! cell that owns its value.

#![cfg(feature = "alloc")]

use alloc::vec;
use alloc::vec::Vec;
use core::any::TypeId;

use daedalus_abi::{NO_SLOT, Slot, SlotState, Slots, Step, StepCx, StepEnd};
use daedalus_value::OwnerKind;

use crate::owned::{self, INLINE_BYTES, InlineCell};

fn is_full(slot: &Slot) -> bool {
    slot.state == SlotState::Full as u32
}

/// Releases what `slot` owns. Pass only a cell that owns its value, never a view of an input.
pub fn drop_owned(slot: Slot) {
    if is_full(&slot) {
        // SAFETY: the caller hands over a cell that owns its value, by value, so it's released
        // once.
        unsafe { owned::release(&slot) };
    }
}

/// The `T` a typed cell holds, without taking it.
pub fn typed_view<T: 'static>(slot: &Slot) -> Option<&T> {
    if !is_full(slot) || slot.owner != OwnerKind::Typed as u32 {
        return None;
    }
    // SAFETY: a full typed cell's `cap` was written by `typed_raw`, so it names this table.
    let ops = unsafe { owned::typed_ops(slot.cap) };
    // SAFETY: the table says the payload is a `T`, and the holder of `slot` keeps its reference
    // for as long as the borrow of `slot` lasts.
    ((ops.type_id)() == TypeId::of::<T>()).then(|| unsafe { &*slot.data.cast::<T>().cast_const() })
}

/// Another reference to what a typed cell holds, without taking it from the cell. For a step
/// that has to keep what it read alive after it returns, such as one whose work on a device is
/// still reading it.
pub fn share_typed(slot: &Slot) -> Option<alloc::sync::Arc<dyn core::any::Any + Send + Sync>> {
    if !is_full(slot) || slot.owner != OwnerKind::Typed as u32 {
        return None;
    }
    // SAFETY: a full typed cell's `cap` was written by `typed_raw`, so it names this table.
    let ops = unsafe { owned::typed_ops(slot.cap) };
    // SAFETY: the cell holds one reference to a payload this table is for. `retain` takes a
    // second one, which `rebuild` then turns into the `Arc` returned, so the cell's own is
    // untouched.
    Some(unsafe {
        (ops.retain)(slot.data);
        (ops.rebuild)(slot.data)
    })
}

/// The bool in a cell, typed or as one byte.
pub fn read_bool(slot: &Slot) -> Option<bool> {
    if !is_full(slot) {
        return None;
    }
    if slot.owner == OwnerKind::Typed as u32 {
        return typed_view::<bool>(slot).copied();
    }
    let bytes = slot.owner == OwnerKind::Inline as u32 || slot.owner == OwnerKind::Static as u32;
    // SAFETY: inline and static cells hold `len` readable bytes, and `len` is checked to be 1.
    (bytes && slot.len == 1).then(|| unsafe { *slot.data } != 0)
}

/// A list of values of any one type: what a `for_each` reads and writes. Elements stay owned here
/// and are lent to the body as views.
#[derive(Debug, Default)]
pub struct SlotList {
    cells: Vec<Slot>,
    inline: Vec<InlineCell>,
}

// SAFETY: a list owns its cells outright and only hands out shared views. Their payloads are
// `Send + Sync` values or plain bytes, which is what a typed or shared cell holds.
unsafe impl Send for SlotList {}
// SAFETY: as above; nothing mutates a list once it is shared.
unsafe impl Sync for SlotList {}

impl SlotList {
    /// Room for `capacity` elements. Pushing more is refused, so inline elements never move.
    pub fn with_capacity(capacity: usize) -> SlotList {
        SlotList {
            cells: Vec::with_capacity(capacity),
            inline: vec![[0; INLINE_BYTES]; capacity],
        }
    }

    /// Takes ownership of `slot`. If the list is full, the value is released and `false` returned.
    pub fn push(&mut self, slot: Slot) -> bool {
        let at = self.cells.len();
        let Some(buffer) = self.inline.get_mut(at) else {
            drop_owned(slot);
            return false;
        };
        self.cells.push(owned::place(slot, buffer));
        true
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// A view of one element. The list keeps it.
    pub fn get(&self, index: usize) -> Option<&Slot> {
        self.cells.get(index)
    }
}

impl Drop for SlotList {
    fn drop(&mut self) {
        self.cells.drain(..).for_each(drop_owned);
    }
}

/// One step of a [`Nested`] list and the cells its frame maps to.
#[derive(Debug)]
pub struct NestedStep {
    pub step: Step,
    pub reads: Vec<u32>,
    pub writes: Vec<u32>,
}

/// A step list with cells of its own.
#[derive(Debug)]
pub struct Nested {
    steps: Vec<NestedStep>,
    cells: Vec<Slot>,
    inline: Vec<InlineCell>,
    scratch: Vec<Slot>,
    /// Which cells hold views, in the order `run` takes them.
    views: Vec<u32>,
}

// SAFETY: steps are `Send`, and the cells are only touched by whoever holds this exclusively.
unsafe impl Send for Nested {}

impl Nested {
    pub fn new(steps: Vec<NestedStep>, cells: usize, views: Vec<u32>) -> Nested {
        let scratch = steps
            .iter()
            .map(|s| s.reads.len() + s.writes.len())
            .max()
            .unwrap_or(0);
        Nested {
            steps,
            cells: vec![Slot::EMPTY; cells],
            inline: vec![[0; INLINE_BYTES]; cells],
            scratch: vec![Slot::EMPTY; scratch],
            views,
        }
    }

    fn is_view(&self, cell: usize) -> bool {
        self.views.contains(&(cell as u32))
    }

    fn run_one(&mut self, at: usize, cx: &mut StepCx) -> StepEnd {
        let inputs = self.steps[at].reads.len();
        for (to, &read) in self.steps[at].reads.iter().enumerate() {
            self.scratch[to] = if read == NO_SLOT {
                Slot::EMPTY
            } else {
                self.cells[read as usize]
            };
        }
        let used = inputs + self.steps[at].writes.len();
        self.scratch[inputs..used].fill(Slot::EMPTY);
        let mut slots = Slots::of(&mut self.scratch[..used]);
        let step = &self.steps[at].step;
        // SAFETY: `slots` and `cx` are valid and exclusive for this call, and a `Step`'s `run` and
        // `data` belong together, as they do for every step the runner calls.
        let end = unsafe { (step.run)(step.data, &mut slots, cx) };
        if matches!(end, StepEnd::Done) {
            self.publish(at, inputs);
        } else {
            self.scratch[inputs..used]
                .iter()
                .copied()
                .for_each(drop_owned);
        }
        end
    }

    fn publish(&mut self, at: usize, inputs: usize) {
        for (from, &read) in self.steps[at].reads.iter().enumerate() {
            let consumed = self.scratch[from].state == SlotState::Moved as u32;
            if !consumed || read == NO_SLOT {
                continue;
            }
            if self.is_view(read as usize) {
                // Whoever lent this has to know it's gone.
                self.cells[read as usize].state = SlotState::Moved as u32;
            } else {
                // The reference went to one of this step's outputs, so it's only forgotten.
                self.cells[read as usize] = Slot::EMPTY;
            }
        }
        for from in 0..self.steps[at].writes.len() {
            let (write, value) = (self.steps[at].writes[from], self.scratch[inputs + from]);
            if write == NO_SLOT {
                drop_owned(value);
            } else if is_full(&value) {
                let cell = write as usize;
                drop_owned(self.cells[cell]);
                self.cells[cell] = owned::place(value, &mut self.inline[cell]);
            }
        }
    }

    /// Lends `inputs` to the cells named at construction and runs every step in order. Stops at
    /// the first fault. A step that was skipped doesn't stop the ones after it. Anything else
    /// comes back as the step ended, for the caller to report.
    pub fn run(&mut self, inputs: &[Slot], cx: &mut StepCx) -> StepEnd {
        for (&cell, input) in self.views.iter().zip(inputs) {
            self.cells[cell as usize] = *input;
        }
        for at in 0..self.steps.len() {
            match self.run_one(at, cx) {
                StepEnd::Done | StepEnd::Skipped => {}
                end => return end,
            }
        }
        StepEnd::Done
    }

    /// Runs the steps as one chain, each woken by the one before: the list stops at the first
    /// step that didn't finish and ends the way that step did, so a skip stops what it feeds and
    /// nothing downstream is woken for it.
    pub fn run_chain(&mut self, inputs: &[Slot], cx: &mut StepCx) -> StepEnd {
        for (&cell, input) in self.views.iter().zip(inputs) {
            self.cells[cell as usize] = *input;
        }
        for at in 0..self.steps.len() {
            match self.run_one(at, cx) {
                StepEnd::Done => {}
                end => return end,
            }
        }
        StepEnd::Done
    }

    /// Whether a step took the value lent in view `cell`.
    pub fn moved(&self, cell: u32) -> bool {
        self.cells
            .get(cell as usize)
            .is_some_and(|slot| slot.state == SlotState::Moved as u32)
    }

    /// Takes an output, which then belongs to the caller. A cell holding a view has none to give.
    pub fn take(&mut self, cell: u32) -> Option<Slot> {
        let at = cell as usize;
        if self.is_view(at) || !self.cells.get(at).is_some_and(is_full) {
            return None;
        }
        let slot = self.cells[at];
        self.cells[at] = Slot::EMPTY;
        Some(slot)
    }

    /// Clears every cell, releasing outputs nobody took.
    pub fn reset(&mut self) {
        for at in 0..self.cells.len() {
            let slot = core::mem::replace(&mut self.cells[at], Slot::EMPTY);
            if !self.is_view(at) {
                drop_owned(slot);
            }
        }
    }
}

impl Drop for Nested {
    fn drop(&mut self) {
        self.reset();
        self.steps
            .drain(..)
            .for_each(|nested| nested.step.discard());
    }
}
