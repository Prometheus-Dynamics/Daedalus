//! A runner that owns its buffers, for a host that builds one graph at a time. The runner's
//! borrows are made `'static` here, where `unsafe` is allowed, and [`RunBuffers`] frees them only
//! after the runner is gone.

#![cfg(feature = "std")]

use std::boxed::Box;
use std::sync::Arc;
use std::vec;
use std::vec::Vec;

use daedalus_abi::{Progress, Slot, Step};

use crate::bits::words_for;
use crate::cancel::CancelWord;
use crate::error::LoadError;
use crate::fault_log::StepFault;
use crate::owned::InlineCell;
use crate::probe::{ProbeClock, TickProbe};
use crate::reuse::{InputVersion, ReuseEntry, ReuseMemory};
use crate::step::{Queue, SlotSpec, SlotStamp, StepMeta};
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
    _stamps: Raw<SlotStamp>,
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
    cancel: Arc<CancelWord>,
    _faults: Raw<StepFault>,
    _progress: Raw<Progress>,
    probe: Option<Raw<TickProbe<'static>>>,
}

impl RunBuffers {
    /// The cancel word of this runner.
    pub fn cancel(&self) -> &CancelWord {
        &self.cancel
    }

    /// The cancel word as a handle another thread can keep: a host that serves several callers
    /// cancels a tick that one of them is running. It may outlive the runner.
    pub fn cancel_handle(&self) -> CancelHandle {
        CancelHandle(Arc::clone(&self.cancel))
    }

    /// The tick probe, when the runner was built with one.
    pub fn probe(&self) -> Option<&TickProbe<'static>> {
        self.probe.as_ref().map(|probe| &probe.shared()[0])
    }
}

impl Drop for RunBuffers {
    /// Releases the steps. They were moved in at [`build`], so nothing else will.
    fn drop(&mut self) {
        // SAFETY: the runner that borrowed the steps is dropped before its buffers, as the type
        // requires, so this is the only view of them.
        let steps = unsafe { &mut *self._steps.ptr };
        for step in steps {
            if let Some(release) = step.release.take() {
                // SAFETY: `release` belongs to this step's `data`, and taking it out of the step
                // means it is called once.
                unsafe { release(step.data) };
            }
        }
    }
}

/// A runner's cancel word, shared. Cancelling through it is what [`RunBuffers::cancel`] does.
#[derive(Clone)]
pub struct CancelHandle(Arc<CancelWord>);

impl CancelHandle {
    /// Asks the run to stop at the next step boundary.
    pub fn cancel(&self) {
        self.0.cancel();
    }

    /// Clears the request.
    pub fn clear(&self) {
        self.0.clear();
    }

    pub fn is_set(&self) -> bool {
        self.0.is_set()
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
    slot_stamps: Vec<SlotStamp>,
    host_inputs: &[u32],
    clock: Option<ProbeClock>,
) -> Result<(Runner<'static>, RunBuffers), LoadError> {
    let count = steps.len();
    let words = words_for(count);
    let reusable = || meta.iter().filter(|record| record.reuse);
    let stamps: usize = reusable().map(|record| record.inputs.len as usize).sum();
    let stored: usize = reusable().map(|record| record.outputs.len as usize).sum();
    let storage: u32 = slots.iter().map(|spec| spec.cap).sum();
    let nslots = slots.len();
    let widest = meta
        .iter()
        .map(|record| (record.inputs.len + record.outputs.len) as usize)
        .max()
        .unwrap_or(0);
    let may_pend = meta.iter().filter(|record| record.may_pend).count();
    let buffers = RunBuffers {
        _steps: Raw::new(steps),
        _meta: Raw::new(meta),
        _table: Raw::new(table),
        _slots: Raw::new(slots),
        _stamps: Raw::new(slot_stamps),
        _cells: Raw::new(vec![Slot::EMPTY; widest]),
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
        cancel: Arc::new(CancelWord::new()),
        _faults: Raw::new(vec![StepFault::EMPTY; count]),
        _progress: Raw::new((0..count).map(|_| Progress::new()).collect()),
        probe: clock.map(|clock| Raw::new(vec![TickProbe::sampled_by(clock, 1)])),
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
    // SAFETY: as above; this table is borrowed only here.
    runner.set_stamps(unsafe { buffers._stamps.borrow() })?;
    // SAFETY: as above; the progress cells are borrowed only here, one per step.
    runner.set_progress(unsafe { buffers._progress.borrow() })?;
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
    // SAFETY: the word lives in an `Arc` that `buffers` holds until it is dropped, which is
    // after the runner, so the runner's borrow never outlives it.
    let word: &'static CancelWord = unsafe { &*Arc::as_ptr(&buffers.cancel) };
    runner.set_cancel(word);
    // SAFETY: as above; this table is borrowed only here.
    runner.set_fault_log(unsafe { buffers._faults.borrow() })?;
    if let Some(probe) = &buffers.probe {
        runner.set_probe(&probe.shared()[0]);
    }
    Ok((runner, buffers))
}
