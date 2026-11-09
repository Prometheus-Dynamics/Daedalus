//! What the runner knows about each step and each slot, and the slot queues.

use daedalus_abi::Slot;
use daedalus_plan::Span;

/// How a consumer takes values from a slot when its step has run.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotKind {
    /// The step pops one value per run.
    Fifo = 0,
    /// The step keeps the newest value and clears its unconsumed flag.
    Latest = 1,
    /// The step pops nothing (C4 holds its inputs).
    Held = 2,
}

/// One slot as the plan declares it: how many values it holds and how consumers take them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotSpec {
    pub cap: u32,
    pub kind: SlotKind,
}

/// One step's wiring, built from the plan. Its ranges index the flat table the runner is given.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StepMeta {
    /// Consumer step indexes, woken when this step is `Done`.
    pub wake: Span,
    /// Slot indexes this step reads.
    pub inputs: Span,
    /// Slot indexes this step writes.
    pub outputs: Span,
    /// Dirty on every tick.
    pub always: bool,
    /// May return `Pending`, so it needs a pending entry.
    pub may_pend: bool,
    /// What a fault of this step does to its tick.
    pub recovery: crate::policy::Recovery,
}

/// One slot's queue: a ring over its part of the shared queue storage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Queue {
    pub base: u32,
    pub cap: u32,
    pub head: u32,
    pub len: u32,
    /// Values pushed and not yet consumed.
    pub unconsumed: u32,
    /// Goes up on every push, so a step can tell whether an input changed.
    pub version: u32,
    pub kind: Option<SlotKind>,
}

impl Queue {
    /// The index in the shared storage of the oldest value.
    pub(crate) fn head_index(&self) -> usize {
        (self.base + self.head) as usize
    }

    /// The slot after the newest value, wrapping inside this queue's part.
    pub(crate) fn tail_index(&self) -> usize {
        (self.base + (self.head + self.len) % self.cap) as usize
    }

    pub(crate) fn is_full(&self) -> bool {
        self.len == self.cap
    }

    /// Drops the oldest value from the queue without releasing it, as a take does.
    pub(crate) fn pop_head(&mut self) {
        if self.len > 0 {
            self.head = (self.head + 1) % self.cap;
            self.len -= 1;
            self.unconsumed = self.unconsumed.min(self.len);
        }
    }

    /// Appends `value`, which must not be full. Stamps it with the next version.
    pub(crate) fn push(&mut self, storage: &mut [Slot], mut value: Slot) {
        self.version = self.version.wrapping_add(1);
        value.version = self.version;
        storage[self.tail_index()] = value;
        self.len += 1;
        self.unconsumed += 1;
    }

    /// Removes the oldest value, as a commit does, according to how this slot is read.
    pub(crate) fn commit(&mut self) {
        match self.kind {
            Some(SlotKind::Fifo) => {
                if self.len > 0 {
                    self.head = (self.head + 1) % self.cap;
                    self.len -= 1;
                }
                self.unconsumed = self.unconsumed.saturating_sub(1).min(self.len);
            }
            Some(SlotKind::Latest) => self.unconsumed = 0,
            Some(SlotKind::Held) | None => {}
        }
    }
}
