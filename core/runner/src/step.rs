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

/// What every value written to one slot is. A step doesn't know these numbers when it's compiled,
/// so the runner writes them as it publishes the value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotStamp {
    pub ty: daedalus_types::TypeIndex,
    pub space: daedalus_types::SpaceIndex,
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
    /// Slot indexes this step reads, in frame order. `NO_SLOT` marks a port with no edge.
    pub inputs: Span,
    /// Slot indexes this step writes, in frame order after the inputs.
    pub outputs: Span,
    /// Dirty on every tick.
    pub always: bool,
    /// May return `Pending`, so it needs a pending entry.
    pub may_pend: bool,
    /// What a fault of this step does to its tick.
    pub recovery: crate::policy::Recovery,
    /// How a missing required input is handled.
    pub fire: crate::join::Fire,
    /// Slot indexes the step cannot run without. Empty means the join check is skipped.
    pub required: Span,
    /// The node opted into output reuse. A runner with no reuse table ignores it.
    pub reuse: bool,
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
    /// What a push does at capacity. A loaded edge's policy; the default buffers up to `cap`.
    pub pressure: daedalus_value::Pressure,
    /// When the value in a coalescing window was stored.
    pub opened_ns: u64,
    /// A full queue holds its producer back rather than taking a value.
    pub blocking: bool,
    /// Rejected values count as backpressure events.
    pub counts_rejects: bool,
    /// How many values were rejected while `counts_rejects` was set.
    pub rejected: u32,
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

    /// The index of the newest value, removed from the queue without being released. The caller
    /// releases it. None when the queue is empty.
    pub(crate) fn take_tail(&mut self) -> Option<usize> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        self.unconsumed = self.unconsumed.min(self.len);
        Some((self.base + (self.head + self.len) % self.cap) as usize)
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
    pub(crate) fn push(&mut self, storage: &mut [Slot], value: Slot) {
        let at = self.tail_index();
        storage[at] = self.stamp(value);
    }

    /// Counts one more value, which the caller stores at `tail_index`. It keeps its version.
    pub(crate) fn count_replay(&mut self) {
        self.len += 1;
        self.unconsumed += 1;
    }

    /// Counts one more value, which the caller stores at `tail_index`, and stamps its version.
    pub(crate) fn stamp(&mut self, mut value: Slot) -> Slot {
        self.version = self.version.wrapping_add(1);
        value.version = self.version;
        self.len += 1;
        self.unconsumed += 1;
        value
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
