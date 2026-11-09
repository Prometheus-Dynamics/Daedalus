//! A runner that owns its buffers, for a host that builds one graph at a time. The runner's
//! borrows are made `'static` here, where `unsafe` is allowed, and [`RunBuffers`] frees them only
//! after the runner is gone.

#![cfg(feature = "std")]

use std::boxed::Box;
use std::vec;
use std::vec::Vec;

use daedalus_abi::{Slot, Step};

use crate::bits::words_for;
use crate::cancel::CancelWord;
use crate::error::LoadError;
use crate::owned::InlineCell;
use crate::reuse::{InputVersion, ReuseEntry, ReuseMemory};
use crate::step::{Queue, SlotSpec, StepMeta};
use crate::{Memory, Pending, Plan, Runner, StepHealth};

/// One boxed slice, held raw so a `Runner<'static>` can borrow it while this owns it.
struct Raw<T> {
    ptr: *mut [T],
}

impl<T> Raw<T> {
    fn new(values: Vec<T>) -> Raw<T> {
        Raw {
            ptr: Box::into_raw(values.into_boxed_slice()),
        }
    }

    /// The whole slice, for as long as `self` is alive.
    ///
    /// # Safety
    ///
    /// Called once, and the borrow must end before `self` is dropped.
    // SAFETY: the caller upholds the `# Safety` contract documented above.
    unsafe fn borrow(&self) -> &'static mut [T] {
        // SAFETY: `ptr` came from `Box::into_raw`, and the caller promises one live borrow.
        unsafe { &mut *self.ptr }
    }
}

impl<T> Raw<T> {
    /// A shared view that lasts as long as `self`: the crate's own use keeps it inside the runner
    /// or inside `self`'s lifetime, and nothing borrows this slice mutably.
    fn shared(&self) -> &'static [T] {
        // SAFETY: `ptr` is live until `self` drops, and only shared views of this slice exist.
        unsafe { &*self.ptr }
    }
}

impl<T> Drop for Raw<T> {
    fn drop(&mut self) {
        // SAFETY: `ptr` came from `Box::into_raw`, and every borrow ended before this drop.
        drop(unsafe { Box::from_raw(self.ptr) });
    }
}

/// The buffers behind a runner from [`build`]. Drop this after the runner.
pub struct RunBuffers {
    _steps: Raw<Step>,
    _meta: Raw<StepMeta>,
    _table: Raw<u32>,
    _slots: Raw<SlotSpec>,
    _cells: Raw<Slot>,
    _queues: Raw<Queue>,
    _storage: Raw<Slot>,
    _dirty: Raw<u32>,
    _carry: Raw<u32>,
    _always: Raw<u32>,
    _pending: Raw<Pending>,
    _health: Raw<StepHealth>,
    _activity: Raw<u32>,
    _inline: Raw<InlineCell>,
    _reuse_entries: Raw<ReuseEntry>,
    _reuse_stamps: Raw<u64>,
    _reuse_stored: Raw<Slot>,
    _reuse_inline: Raw<InlineCell>,
    _reuse_inputs: Raw<InputVersion>,
    cancel: Raw<CancelWord>,
}

impl RunBuffers {
    /// The cancel word of this runner.
    pub fn cancel(&self) -> &CancelWord {
        &self.cancel.shared()[0]
    }
}

// SAFETY: `RunBuffers` owns its allocations, and the runner borrowing them is `Send`.
unsafe impl Send for RunBuffers {}

/// Builds a runner over buffers it owns. The caller keeps the buffers and drops them after the
/// runner.
pub fn build(
    steps: Vec<Step>,
    meta: Vec<StepMeta>,
    table: Vec<u32>,
    slots: Vec<SlotSpec>,
    host_inputs: &[u32],
) -> Result<(Runner<'static>, RunBuffers), LoadError> {
    let count = steps.len();
    let words = words_for(count);
    let reusable = || meta.iter().filter(|record| record.reuse);
    let stamps: usize = reusable().map(|record| record.inputs.len as usize).sum();
    let stored: usize = reusable().map(|record| record.outputs.len as usize).sum();
    let storage: u32 = slots.iter().map(|spec| spec.cap).sum();
    let nslots = slots.len();
    let may_pend = meta.iter().filter(|record| record.may_pend).count();
    let buffers = RunBuffers {
        _steps: Raw::new(steps),
        _meta: Raw::new(meta),
        _table: Raw::new(table),
        _slots: Raw::new(slots),
        _cells: Raw::new(vec![Slot::EMPTY; nslots]),
        _queues: Raw::new(vec![Queue::default(); nslots]),
        _storage: Raw::new(vec![Slot::EMPTY; storage as usize]),
        _dirty: Raw::new(vec![0; words]),
        _carry: Raw::new(vec![0; words]),
        _always: Raw::new(vec![0; words]),
        _pending: Raw::new(vec![Pending::EMPTY; may_pend]),
        _health: Raw::new(vec![StepHealth::default(); count]),
        _activity: Raw::new(vec![0; words]),
        _inline: Raw::new(vec![[0; crate::owned::INLINE_BYTES]; storage as usize]),
        _reuse_entries: Raw::new(vec![ReuseEntry::EMPTY; count]),
        _reuse_stamps: Raw::new(vec![0; stamps]),
        _reuse_stored: Raw::new(vec![Slot::EMPTY; stored]),
        _reuse_inline: Raw::new(vec![[0; crate::owned::INLINE_BYTES]; stored]),
        _reuse_inputs: Raw::new(vec![InputVersion::default(); nslots]),
        cancel: Raw::new(vec![CancelWord::new()]),
    };
    // SAFETY: each buffer is borrowed exactly once here, and `RunBuffers` keeps them alive until
    // the runner is gone, as the function's documentation requires of the caller.
    let plan = unsafe {
        Plan {
            steps: buffers._steps.borrow(),
            meta: buffers._meta.borrow(),
            table: buffers._table.borrow(),
            slots: buffers._slots.borrow(),
        }
    };
    // SAFETY: as above.
    let memory = unsafe {
        Memory {
            cells: buffers._cells.borrow(),
            queues: buffers._queues.borrow(),
            storage: buffers._storage.borrow(),
            dirty: buffers._dirty.borrow(),
            carry: buffers._carry.borrow(),
            always: buffers._always.borrow(),
            pending: buffers._pending.borrow(),
            health: buffers._health.borrow(),
            activity: buffers._activity.borrow(),
            inline: buffers._inline.borrow(),
        }
    };
    let mut runner = Runner::load(plan, memory)?;
    if stamps + stored > 0 {
        // SAFETY: as above; these five are borrowed only here.
        let table = unsafe {
            ReuseMemory {
                entries: buffers._reuse_entries.borrow(),
                stamps: buffers._reuse_stamps.borrow(),
                stored: buffers._reuse_stored.borrow(),
                inline: buffers._reuse_inline.borrow(),
                inputs: buffers._reuse_inputs.borrow(),
            }
        };
        runner.set_reuse(table, host_inputs)?;
    }
    let word: &'static CancelWord = &buffers.cancel.shared()[0];
    runner.set_cancel(word);
    Ok((runner, buffers))
}
