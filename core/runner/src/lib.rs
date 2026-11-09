//! Walks a solved step list. The runner has no planner, registry, host or backend, and it never
//! allocates: the caller hands it every buffer when it loads a plan.
//!
//! A tick runs the steps whose inputs changed, in step order. An idle tick reads one word per 32
//! steps and calls nothing. Values move as raw [`daedalus_abi::Slot`] descriptions, so the runner
//! never copies or reference-counts what they point to.

#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

#[cfg(feature = "alloc")]
mod access;
mod arena;
#[cfg(feature = "std")]
mod async_step;
mod backpressure;
mod bits;
mod cancel;
mod error;
#[cfg(all(feature = "std", target_os = "linux"))]
pub mod event_fd;
mod exec;
mod failure;
#[cfg(feature = "alloc")]
mod fanin;
mod fault_log;
mod fence;
mod fixed_queue;
#[cfg(feature = "alloc")]
mod forward;
mod inline;
mod join;
#[cfg(all(feature = "threads", target_arch = "wasm32", target_os = "unknown"))]
compile_error!(
    "threads aren't available on wasm32-unknown-unknown: leave the `threads` feature off"
);

#[cfg(feature = "threads")]
mod lanes;
#[cfg(feature = "alloc")]
mod nested;
mod owned;
#[cfg(feature = "std")]
pub mod owned_run;
mod policy;
mod probe;
mod progress;
mod reuse;
mod step;
mod telemetry;
mod tick;
mod values;

use daedalus_abi::{Fence, NO_SLOT, Progress, Slot, Step, StepCx};
use daedalus_plan::Span;
use daedalus_value::OwnerKind;

#[cfg(feature = "alloc")]
pub use access::{Held, UNSTAMPED, take_typed};
pub use arena::{
    ArenaBody, ArenaFull, Fresh, StepArena, align_up, bytes_of, copy_inline, fill_inline,
    steps_end, value_of,
};
#[cfg(feature = "std")]
pub use async_step::FutureFence;
pub use backpressure::BackpressureStrategy;
pub use cancel::{CANCEL_REASON, CancelWord, skip_cancelled};
pub use error::{LoadError, MaskError, RunError};
pub use failure::{FailurePolicy, MAX_CAP_NS, PolicyError, StepHealth};
#[cfg(feature = "alloc")]
pub use fanin::fanin_order;
pub use fault_log::StepFault;
pub use fixed_queue::{FixedQueue, Overflow, QueueFull, Ring};
#[cfg(feature = "alloc")]
pub use forward::Forward;
pub use inline::{read_inline, write_inline};
pub use join::Fire;
#[cfg(feature = "threads")]
pub use lanes::{LaneError, Lanes, Pool};
#[cfg(feature = "alloc")]
pub use nested::{Nested, NestedStep, SlotList, drop_owned, read_bool, typed_view};
pub use owned::{INLINE_BYTES, InlineCell, Untyped, place};
#[cfg(feature = "alloc")]
pub use owned::{TypedOps, typed_ops, typed_raw};
pub use policy::Recovery;
pub use probe::{ClockFn, HISTOGRAM_BUCKETS, StepHistogram, TickProbe, TickSample};
pub use reuse::{
    InputVersion, ReuseEntry, ReuseMarker, ReuseMemory, ReuseReason, ReuseRefused, check_reuse,
};
pub use step::{Queue, SlotKind, SlotSpec, SlotStamp, StepMeta};
pub use telemetry::Counters;
pub use tick::TickReport;
pub use values::{put, put_inline, put_static};
#[cfg(feature = "alloc")]
pub use values::{put_shared, put_typed};

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
    /// Where a body's frame is built: at least as many cells as the largest step has ports.
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
    /// One failure record per step. Its length is at least the step count.
    pub health: &'a mut [StepHealth],
    /// The activity mask, in `words_for(steps)` words. Only used once a host sets a mask.
    pub activity: &'a mut [u32],
    /// One inline buffer per queue entry, so an inline value's bytes sit beside its entry. Its
    /// length is at least `storage.len()`.
    pub inline: &'a mut [InlineCell],
}

/// A loaded graph ready to tick.
pub struct Runner<'a> {
    steps: &'a mut [Step],
    meta: &'a [StepMeta],
    table: &'a [u32],
    slots: usize,
    /// The largest step's port count.
    #[cfg(feature = "threads")]
    widest: usize,
    stamps: &'a [SlotStamp],
    cells: &'a mut [Slot],
    queues: &'a mut [Queue],
    storage: &'a mut [Slot],
    dirty: &'a mut [u32],
    carry: &'a mut [u32],
    always: &'a mut [u32],
    pending: &'a mut [Pending],
    pending_len: usize,
    health: &'a mut [StepHealth],
    failure: FailurePolicy,
    counters: Counters,
    activity: &'a mut [u32],
    inline: &'a mut [InlineCell],
    masked: bool,
    cx: StepCx,
    tick: u32,
    /// One `(done, total)` pair per step, once a host attaches them.
    progress: &'a [Progress],
    fault_log: fault_log::FaultLog,
    any_blocking: bool,
    /// The reuse table, for a plan with a reusable step.
    reuse: Option<reuse::Table<'a>>,
    /// Timing, when a host attached a probe.
    probe: Option<&'a probe::TickProbe<'a>>,
}

// SAFETY: a `Runner` holds exclusive borrows of its buffers and calls each step from whichever
// thread owns it. The step contract makes each body safe to run on another thread: `OwnedStep`
// requires `Send`, `StepWord` is a function, and a hand-built `Step` promises the same.
unsafe impl Send for Runner<'_> {}

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
            health,
            activity,
            inline,
        } = memory;
        let count = steps.len();
        let words = words_for(count);
        let nslots = slots.len();
        if meta.len() != count
            || queues.len() < nslots
            || dirty.len() < words
            || carry.len() < words
            || always.len() < words
            || activity.len() < words
            || health.len() < count
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
        if storage.len() < total as usize || inline.len() < storage.len() {
            return Err(LoadError::Length);
        }
        dirty[..words].fill(0);
        carry[..words].fill(0);
        health[..count].fill(StepHealth::default());
        always[..words].fill(0);
        let mut may_pend = 0;
        let mut widest = 0;
        for (index, record) in meta.iter().enumerate() {
            widest = widest.max((record.inputs.len + record.outputs.len) as usize);
            for &slot in span(table, record.inputs)?
                .iter()
                .chain(span(table, record.outputs)?)
                .filter(|&&slot| slot != NO_SLOT)
            {
                if slot as usize >= nslots {
                    return Err(LoadError::SlotRange);
                }
            }
            for &slot in span(table, record.required)? {
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
        if pending.len() < may_pend || cells.len() < widest {
            return Err(LoadError::Length);
        }
        Ok(Runner {
            steps,
            meta,
            table,
            slots: nslots,
            #[cfg(feature = "threads")]
            widest,
            stamps: &[],
            cells,
            queues,
            storage,
            dirty,
            carry,
            always,
            pending,
            pending_len: 0,
            health,
            failure: FailurePolicy::DEFAULT,
            counters: Counters::default(),
            activity,
            inline,
            masked: false,
            cx: StepCx::new(),
            tick: 0,
            progress: &[],
            fault_log: fault_log::FaultLog::NONE,
            any_blocking: false,
            reuse: None,
            probe: None,
        })
    }

    /// Stores a raw value at the back of `slot`'s queue. It does not wake consumers: a host feed
    /// calls [`Runner::wake`] for each consumer, as a body write does.
    ///
    /// Only values that own nothing can be pushed raw: static bytes, a slot reference, or a host
    /// handle. Owning values go in through `feed_value` or `feed_typed`.
    pub fn push(&mut self, slot: u32, value: Slot) -> Result<(), RunError> {
        let index = slot as usize;
        if index >= self.slots {
            return Err(RunError::NoSlot { slot });
        }
        let owner = value.owner;
        if owner != OwnerKind::Static as u32
            && owner != OwnerKind::Slot as u32
            && owner != OwnerKind::Host as u32
        {
            return Err(RunError::NotRaw { slot });
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

    /// The wiring of `step`, for hosts and reports.
    pub fn step_meta(&self, step: u32) -> Option<StepMeta> {
        self.meta.get(step as usize).copied()
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

impl Drop for Runner<'_> {
    /// Releases every value still queued, once each. Cells hold only views and outputs that were
    /// already moved into queues, so they own nothing here.
    fn drop(&mut self) {
        self.clear_reuse();
        for index in 0..self.slots {
            let queue = self.queues[index];
            for offset in 0..queue.len {
                let at = queue.base + (queue.head + offset) % queue.cap;
                // SAFETY: each queued entry owns its reference, and the queue forgets it here.
                unsafe { owned::release(&self.storage[at as usize]) };
            }
            self.queues[index].head = 0;
            self.queues[index].len = 0;
            self.queues[index].unconsumed = 0;
        }
    }
}
