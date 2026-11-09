//! Walks a solved step list. The runner has no planner, registry, host or backend, and it never
//! allocates: the caller hands it every buffer when it loads a plan.
//!
//! A tick runs the steps whose inputs changed, in step order. An idle tick reads one word per 32
//! steps and calls nothing. Values move as raw [`daedalus_abi::Slot`] descriptions, so the runner
//! never copies or reference-counts what they point to.

#![no_std]

mod bits;
mod error;
mod fence;
mod step;
mod tick;

use daedalus_abi::{Fence, Slot, Step, StepCx};
use daedalus_plan::Span;

pub use error::{LoadError, MaskError, RunError};
pub use step::{Queue, SlotKind, SlotSpec, StepMeta};
pub use tick::TickReport;

use bits::words_for;

/// What a step is waiting on, and the step it belongs to.
#[derive(Clone, Copy, Debug)]
pub struct Pending {
    pub step: u32,
    pub fence: Fence,
}

impl Pending {
    pub const EMPTY: Pending = Pending {
        step: 0,
        fence: Fence::NONE,
    };
}

/// The solved graph's steps and wiring, as the planner or a loader produced them.
pub struct Plan<'a> {
    /// Steps in topological order: a consumer always has a higher index than its producer.
    pub steps: &'a mut [Step],
    /// One record per step.
    pub meta: &'a [StepMeta],
    /// The flat table every span in `meta` indexes.
    pub table: &'a [u32],
    /// One record per slot.
    pub slots: &'a [SlotSpec],
}

/// Every buffer the runner needs, supplied by the caller. Lengths are checked at load.
pub struct Memory<'a> {
    /// Raw cells the bodies see: one per slot, each the head of that slot's queue.
    pub cells: &'a mut [Slot],
    /// One queue state per slot.
    pub queues: &'a mut [Queue],
    /// The storage every queue is a ring over. Its length is the sum of the slot capacities.
    pub storage: &'a mut [Slot],
    /// One bit per step, in `words_for(steps)` words.
    pub dirty: &'a mut [u32],
    /// Bits to carry into the next tick.
    pub carry: &'a mut [u32],
    /// Steps that run every tick. Filled at load.
    pub always: &'a mut [u32],
    /// One entry per step with `may_pend`.
    pub pending: &'a mut [Pending],
    /// The activity mask, in `words_for(steps)` words. Only used once a host sets a mask.
    pub activity: &'a mut [u32],
}

/// A loaded graph ready to tick.
pub struct Runner<'a> {
    steps: &'a mut [Step],
    meta: &'a [StepMeta],
    table: &'a [u32],
    slots: usize,
    cells: &'a mut [Slot],
    queues: &'a mut [Queue],
    storage: &'a mut [Slot],
    dirty: &'a mut [u32],
    carry: &'a mut [u32],
    always: &'a mut [u32],
    pending: &'a mut [Pending],
    pending_len: usize,
    activity: &'a mut [u32],
    masked: bool,
    cx: StepCx,
    tick: u32,
}

impl<'a> Runner<'a> {
    /// Checks the plan and sets up the queues. Loading may be slow; ticking never is.
    pub fn load(plan: Plan<'a>, memory: Memory<'a>) -> Result<Runner<'a>, LoadError> {
        let Plan {
            steps,
            meta,
            table,
            slots,
        } = plan;
        let Memory {
            cells,
            queues,
            storage,
            dirty,
            carry,
            always,
            pending,
            activity,
        } = memory;
        let count = steps.len();
        let words = words_for(count);
        let nslots = slots.len();
        if meta.len() != count
            || cells.len() < nslots
            || queues.len() < nslots
            || dirty.len() < words
            || carry.len() < words
            || always.len() < words
            || activity.len() < words
        {
            return Err(LoadError::Length);
        }
        let mut total: u32 = 0;
        for (index, spec) in slots.iter().enumerate() {
            if spec.cap == 0 {
                return Err(LoadError::Capacity);
            }
            queues[index] = Queue {
                base: total,
                cap: spec.cap,
                kind: Some(spec.kind),
                ..Queue::default()
            };
            total = total.checked_add(spec.cap).ok_or(LoadError::Capacity)?;
        }
        if storage.len() < total as usize {
            return Err(LoadError::Length);
        }
        dirty[..words].fill(0);
        carry[..words].fill(0);
        always[..words].fill(0);
        let mut may_pend = 0;
        for (index, record) in meta.iter().enumerate() {
            for &slot in span(table, record.inputs)?
                .iter()
                .chain(span(table, record.outputs)?)
            {
                if slot as usize >= nslots {
                    return Err(LoadError::SlotRange);
                }
            }
            for &to in span(table, record.wake)? {
                if to as usize >= count {
                    return Err(LoadError::WakeRange);
                }
                if to as usize <= index {
                    return Err(LoadError::BackEdge {
                        from: index as u32,
                        to,
                    });
                }
            }
            if record.always {
                bits::set(always, index);
            }
            may_pend += usize::from(record.may_pend);
        }
        if pending.len() < may_pend {
            return Err(LoadError::Length);
        }
        Ok(Runner {
            steps,
            meta,
            table,
            slots: nslots,
            cells,
            queues,
            storage,
            dirty,
            carry,
            always,
            pending,
            pending_len: 0,
            activity,
            masked: false,
            cx: StepCx::new(),
            tick: 0,
        })
    }

    /// Stores a raw value at the back of `slot`'s queue. It does not wake consumers: a host feed
    /// calls [`Runner::wake`] for each consumer, as a body write does.
    pub fn push(&mut self, slot: u32, value: Slot) -> Result<(), RunError> {
        let index = slot as usize;
        if index >= self.slots {
            return Err(RunError::NoSlot { slot });
        }
        if self.queues[index].is_full() {
            return Err(RunError::Overflow { slot });
        }
        self.queues[index].push(self.storage, value);
        Ok(())
    }

    /// Marks `step` dirty, so it runs on the next tick. Steps past the end are ignored.
    pub fn wake(&mut self, step: u32) {
        if (step as usize) < self.steps.len() {
            bits::set(self.dirty, step as usize);
        }
    }

    /// The queue state of `slot`, for hosts and tests.
    pub fn queue(&self, slot: u32) -> Option<&Queue> {
        self.queues
            .get(slot as usize)
            .filter(|_| (slot as usize) < self.slots)
    }

    /// Sets which steps may run. `None` clears the mask, so every step runs. A step whose bit is
    /// clear is skipped and wakes nothing, and its inputs stay queued.
    pub fn set_activity(&mut self, mask: Option<&[u32]>) -> Result<(), MaskError> {
        let expected = words_for(self.steps.len());
        match mask {
            None => self.masked = false,
            Some(words) if words.len() == expected => {
                self.activity[..expected].copy_from_slice(words);
                self.masked = true;
            }
            Some(_) => {
                return Err(MaskError {
                    expected: expected as u32,
                });
            }
        }
        Ok(())
    }

    /// How many fences are registered and not yet finished.
    pub fn pending(&self) -> usize {
        self.pending_len
    }
}

/// A span the load already checked, so it always fits.
fn spanned(table: &[u32], range: Span) -> &[u32] {
    let start = range.start as usize;
    let end = start.saturating_add(range.len as usize);
    table.get(start..end).unwrap_or(&[])
}

fn span(table: &[u32], range: Span) -> Result<&[u32], LoadError> {
    let start = range.start as usize;
    let end = start
        .checked_add(range.len as usize)
        .ok_or(LoadError::Span)?;
    table.get(start..end).ok_or(LoadError::Span)
}
