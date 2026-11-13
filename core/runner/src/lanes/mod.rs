//! Parallel lanes, behind the `threads` feature: a tick that fans out to a [`Pool`] made once at
//! host start. Nothing is spawned or allocated per tick, and a parallel tick gives the same
//! outputs, counters and faults as the serial tick on the same inputs.
//!
//! A step can start once every step that may touch its slots or wake it has finished, so a slot is
//! never written and read at once. Each worker has its own cells and context, so bodies never
//! share those. A short lock per slot covers the queue metadata readers update in place.

#![cfg(feature = "std")]

mod pool;
mod ready;

use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicU8, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::vec;
use std::vec::Vec;

use daedalus_abi::{NO_SLOT, Progress, Slot, Step, StepCx, Word};

pub use pool::Pool;
use ready::Ready;

use crate::bits::{self, words_for};
use crate::error::RunError;
use crate::exec::{Env, RunFn, SlotParts, step_once};
use crate::failure::{FailurePolicy, StepHealth};
use crate::fault_log::FaultLog;
use crate::fence::insert_pending;
use crate::reuse::ReuseParts;
use crate::step::{Queue, SlotKind, SlotStamp, StepMeta};
use crate::telemetry::Counters;
use crate::tick::TickReport;
use crate::{Pending, Runner, spanned};

/// Why a [`Lanes`] could not be built.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LaneError {
    /// There must be one priority per step, or none at all.
    Priorities { steps: u32, given: u32 },
}

/// What a tick writes into the cancel word when one of its steps stops the run. It differs from
/// the host's own cancel, so the tick clears only what it raised.
const STOPPED: u32 = 2;

/// Whether the error `new` at its step replaces `old`: a fault outranks the `Cancelled` it caused
/// in other steps, and among errors of one kind the lowest step wins.
fn outranks(new: &(u32, RunError), old: &(u32, RunError)) -> bool {
    let kind = |error: &RunError| u8::from(matches!(error, RunError::Cancelled { .. }));
    (kind(&new.1), new.0) < (kind(&old.1), old.0)
}

const CLEAN: u8 = 0;
const DIRTY: u8 = 1;
const CARRY: u8 = 2;

/// One worker's own state. Only that worker touches it during a tick.
struct Worker {
    cells: Vec<Slot>,
    cx: StepCx,
    report: TickReport,
    counters: Counters,
    readied: Vec<u32>,
    error: Option<(u32, RunError)>,
}

/// Everything a parallel tick needs that was sized once, for one runner.
pub struct Lanes {
    pool: Arc<Pool>,
    /// Rank to step, and step to rank: higher priority first, index order among equals.
    by_rank: Vec<u32>,
    rank: Vec<u32>,
    /// `after[start[i]..start[i + 1]]` are the steps that wait for step `i`.
    start: Vec<u32>,
    after: Vec<u32>,
    marks: Vec<AtomicU8>,
    unfinished: Vec<AtomicU32>,
    reached: Vec<bool>,
    locks: Vec<Mutex<()>>,
    pending_lock: Mutex<()>,
    ready: Ready,
    workers: Vec<UnsafeCell<Worker>>,
    /// The cancel word the workers read when the host gave the runner none.
    stop: Word,
}

// SAFETY: each `Worker` is touched only by the thread whose number it has, during one fan-out, and
// `tick_parallel` takes `&mut Lanes`, so fan-outs on one `Lanes` never overlap. The `Slot`s and
// `StepCx` inside hold only views of the runner's buffers. `tick_parallel` guards those.
unsafe impl Sync for Lanes {}
// SAFETY: as above; nothing in a `Worker` is tied to the thread that built it.
unsafe impl Send for Lanes {}

impl Lanes {
    /// Sizes the lanes for `runner`. `priorities` has one entry per step (from
    /// `NODE_PRIORITY_META_KEY`, higher first), or is empty when no step has one.
    pub fn new(
        runner: &Runner<'_>,
        pool: Arc<Pool>,
        priorities: &[i32],
    ) -> Result<Lanes, LaneError> {
        let count = runner.steps.len();
        if !priorities.is_empty() && priorities.len() != count {
            return Err(LaneError::Priorities {
                steps: count as u32,
                given: priorities.len() as u32,
            });
        }
        let mut by_rank: Vec<u32> = (0..count as u32).collect();
        by_rank.sort_by_key(|&step| {
            let priority = priorities.get(step as usize).copied().unwrap_or(0);
            (core::cmp::Reverse(priority), step)
        });
        let mut rank = vec![0; count];
        for (at, &step) in by_rank.iter().enumerate() {
            rank[step as usize] = at as u32;
        }
        let (start, after) = dependencies(runner);
        let workers = (0..pool.workers())
            .map(|_| {
                UnsafeCell::new(Worker {
                    cells: vec![Slot::EMPTY; runner.widest],
                    cx: StepCx::new(),
                    report: TickReport::default(),
                    counters: Counters::default(),
                    readied: Vec::with_capacity(count),
                    error: None,
                })
            })
            .collect();
        Ok(Lanes {
            pool,
            by_rank,
            rank,
            start,
            after,
            marks: (0..count).map(|_| AtomicU8::new(CLEAN)).collect(),
            unfinished: (0..count).map(|_| AtomicU32::new(0)).collect(),
            reached: vec![false; count],
            locks: (0..runner.slots).map(|_| Mutex::new(())).collect(),
            pending_lock: Mutex::new(()),
            ready: Ready::new(count),
            workers,
            stop: Word::new(0),
        })
    }

    fn waiters(&self, step: usize) -> &[u32] {
        &self.after[self.start[step] as usize..self.start[step + 1] as usize]
    }
}

/// Which steps wait for which: a wake edge, or one step following another on a slot. A value's
/// producer comes before its consumers, and consumers that pop run in step order, so the order
/// of pops is the serial one. Consumers that only read a slot don't wait for each other.
fn dependencies(runner: &Runner<'_>) -> (Vec<u32>, Vec<u32>) {
    let count = runner.steps.len();
    let mut edges: Vec<Vec<u32>> = vec![Vec::new(); count];
    let mut last: Vec<Option<u32>> = vec![None; runner.slots];
    let mut readers: Vec<Vec<u32>> = vec![Vec::new(); runner.slots];
    for (index, meta) in runner.meta.iter().enumerate() {
        let step = index as u32;
        edges[index].extend_from_slice(spanned(runner.table, meta.wake));
        for &slot in spanned(runner.table, meta.inputs) {
            if slot == NO_SLOT {
                continue;
            }
            let at = slot as usize;
            if let Some(from) = last[at].filter(|&from| from != step) {
                edges[from as usize].push(step);
            }
            if runner.queues[at].kind == Some(SlotKind::Fifo) {
                for reader in readers[at].drain(..).filter(|&reader| reader != step) {
                    edges[reader as usize].push(step);
                }
                last[at] = Some(step);
            } else {
                readers[at].push(step);
            }
        }
        for &slot in spanned(runner.table, meta.outputs) {
            if slot == NO_SLOT {
                continue;
            }
            let at = slot as usize;
            if let Some(from) = last[at].filter(|&from| from != step) {
                edges[from as usize].push(step);
            }
            for reader in readers[at].drain(..).filter(|&reader| reader != step) {
                edges[reader as usize].push(step);
            }
            last[at] = Some(step);
        }
    }
    let mut start = Vec::with_capacity(count + 1);
    let mut after = Vec::new();
    for list in &mut edges {
        list.sort_unstable();
        list.dedup();
        start.push(after.len() as u32);
        after.extend_from_slice(list);
    }
    start.push(after.len() as u32);
    (start, after)
}

/// The runner's buffers for the length of one fan-out.
struct View<'t> {
    lanes: &'t Lanes,
    /// The cancel word every worker reads this tick.
    stop: &'t Word,
    steps: *const Step,
    meta: &'t [StepMeta],
    table: &'t [u32],
    health: *mut StepHealth,
    queues: *mut Queue,
    parts: SlotParts,
    activity: &'t [u32],
    masked: bool,
    failure: FailurePolicy,
    stamps: &'t [SlotStamp],
    pending: *mut Pending,
    pending_cap: usize,
    pending_len: *mut usize,
    progress: &'t [Progress],
    fault_log: FaultLog,
    tick: u32,
    any_blocking: bool,
    slots: usize,
    reuse: Option<ReuseParts>,
    probe: Option<&'t crate::probe::TickProbe<'t>>,
}

// SAFETY: during a fan-out each step's health and body are touched by the one worker running it.
// The dependency order makes writers of a slot exclusive, the per-slot lock covers queue metadata,
// and the pending list has its own lock. Bodies are `Send` by the `Step` contract.
unsafe impl Sync for View<'_> {}

/// One worker's view of a tick.
struct Lane<'t> {
    view: &'t View<'t>,
    worker: &'t mut Worker,
}

fn locked(lock: &Mutex<()>) -> MutexGuard<'_, ()> {
    lock.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<'t> Env<'t> for Lane<'t> {
    fn meta(&self, index: usize) -> StepMeta {
        self.view.meta[index]
    }
    fn table(&self) -> &'t [u32] {
        self.view.table
    }
    fn step_fn(&self, index: usize) -> (RunFn, *mut ()) {
        // SAFETY: `index` is below the step count, and steps are not written during a tick.
        let step = unsafe { &*self.view.steps.add(index) };
        (step.run, step.data)
    }
    fn health(&mut self, index: usize) -> &mut StepHealth {
        // SAFETY: `index` is the step this worker is running, and no other worker touches it.
        unsafe { &mut *self.view.health.add(index) }
    }
    fn any_blocking(&self) -> bool {
        self.view.any_blocking
    }
    fn slot_count(&self) -> usize {
        self.view.slots
    }
    fn fault_log(&self) -> (FaultLog, u32) {
        (self.view.fault_log, self.view.tick)
    }
    fn progress(&self, index: usize) -> *const Progress {
        crate::progress::cell(self.view.progress, index)
    }
    fn probe(&self) -> Option<&'t crate::probe::TickProbe<'t>> {
        self.view.probe
    }
    fn reuse(&mut self) -> Option<ReuseParts> {
        self.view.reuse
    }
    fn parts(&mut self) -> (&mut [Slot], &mut StepCx) {
        (&mut self.worker.cells, &mut self.worker.cx)
    }
    fn failure(&self) -> FailurePolicy {
        self.view.failure
    }
    fn stamp(&self, slot: usize) -> Option<SlotStamp> {
        self.view.stamps.get(slot).copied()
    }
    fn counters(&mut self) -> &mut Counters {
        &mut self.worker.counters
    }
    fn active(&self, index: usize) -> bool {
        !self.view.masked || bits::get(self.view.activity, index)
    }
    fn with_queue<R>(&mut self, slot: usize, f: impl FnOnce(&mut Queue, SlotParts) -> R) -> R {
        let _guard = locked(&self.view.lanes.locks[slot]);
        // SAFETY: the per-slot lock is held, and `slot` was range-checked at load.
        f(unsafe { &mut *self.view.queues.add(slot) }, self.view.parts)
    }
    fn wake(&mut self, step: u32) {
        if let Some(mark) = self.view.lanes.marks.get(step as usize) {
            mark.store(DIRTY, Ordering::Release);
        }
    }
    fn carry(&mut self, step: usize) {
        self.view.lanes.marks[step].store(CARRY, Ordering::Release);
    }
    fn register_pending(&mut self, entry: Pending) -> bool {
        let _guard = locked(&self.view.lanes.pending_lock);
        // SAFETY: the pending lock is held, and `pending_cap` is the list's length.
        let list =
            unsafe { core::slice::from_raw_parts_mut(self.view.pending, self.view.pending_cap) };
        // SAFETY: as above.
        insert_pending(list, unsafe { &mut *self.view.pending_len }, entry)
    }
    fn is_pending(&mut self, step: usize) -> bool {
        let _guard = locked(&self.view.lanes.pending_lock);
        // SAFETY: the pending lock is held, and the first `pending_len` entries of the list are
        // the live ones.
        let list =
            unsafe { core::slice::from_raw_parts(self.view.pending, *self.view.pending_len) };
        crate::fence::holds(list, step)
    }
}

/// One worker's part of a tick: take a ready step, run it, release the steps waiting on it.
fn work(view: &View<'_>, number: usize) {
    let lanes = view.lanes;
    // SAFETY: each worker number is used by one thread per fan-out, and `workers` has one per
    // number.
    let worker = unsafe { &mut *lanes.workers[number].get() };
    let mut lane = Lane { view, worker };
    let mut next = lanes.ready.next(false, &[], false);
    while let Some(rank) = next {
        let index = lanes.by_rank[rank as usize] as usize;
        let mut stop = false;
        let dirty = lanes.marks[index].load(Ordering::Acquire) == DIRTY;
        if dirty && lane.worker.cx.cancelled() {
            // The step stays marked, so it goes back to dirty and runs on a later tick.
            let found = (index as u32, RunError::Cancelled { step: index as u32 });
            if lane
                .worker
                .error
                .as_ref()
                .is_none_or(|old| outranks(&found, old))
            {
                lane.worker.error = Some(found);
            }
            next = lanes.ready.next(true, &[], true);
            continue;
        }
        if lanes.marks[index].swap(CLEAN, Ordering::Acquire) == DIRTY {
            let mut report = lane.worker.report;
            if let Err(error) = step_once(&mut lane, index, &mut report) {
                stop = true;
                // The steps running beside this one are told to stop. A cancel the host raised
                // itself is left as it is.
                if !matches!(error, RunError::Cancelled { .. }) {
                    view.stop.replace_if(0, STOPPED);
                }
                let found = (index as u32, error);
                if lane
                    .worker
                    .error
                    .as_ref()
                    .is_none_or(|old| outranks(&found, old))
                {
                    lane.worker.error = Some(found);
                }
            }
            lane.worker.report = report;
        }
        lane.worker.readied.clear();
        for &waiter in lanes.waiters(index) {
            if lanes.unfinished[waiter as usize].fetch_sub(1, Ordering::AcqRel) == 1 {
                lane.worker.readied.push(lanes.rank[waiter as usize]);
            }
        }
        next = lanes.ready.next(true, &lane.worker.readied, stop);
    }
}

impl Runner<'_> {
    /// Runs one tick on the pool. The result is the serial tick's, except for a `fail_fast` stop:
    /// the cancel word is raised for the rest of the tick, so a step already running that checks
    /// [`StepCx::cancelled`] stops early, and one that never checks runs to its end. The tick
    /// clears the word again if it raised it. Body panics are caught by the step wrappers and count
    /// as that step's `Fault`. Every `Step` must be safe to run on another thread.
    pub fn tick_parallel(&mut self, lanes: &mut Lanes) -> Result<TickReport, RunError> {
        self.tick = self.tick.wrapping_add(1);
        let mut report = TickReport {
            tick: self.tick,
            ..TickReport::default()
        };
        let probe = self.probe;
        let start = probe.and_then(|probe| probe.begin_tick(false));
        let result = self.run_parallel(lanes, &mut report);
        if let Some(probe) = probe {
            probe.finish_tick(start, report.step_calls);
        }
        self.counters.absorb(&report);
        result.map(|()| report)
    }

    fn run_parallel(&mut self, lanes: &mut Lanes, report: &mut TickReport) -> Result<(), RunError> {
        let words = words_for(self.steps.len());
        let mut any = 0;
        for word in 0..words {
            self.dirty[word] |= self.always[word];
            any |= self.dirty[word];
        }
        report.word_reads = words as u32;
        if any == 0 && self.pending_len == 0 {
            report.idle = true;
            return Ok(());
        }
        if self.pending_len > 0 {
            self.poll_fences(report);
        }
        let remaining = self.seed_lanes(lanes);
        if remaining > 0 {
            let now = self.cx.now_ns;
            // SAFETY: the runner's cancel word is null or a `Word` that outlives the runner.
            let stop: &Word = unsafe { self.cx.cancel.as_ref() }.unwrap_or(&lanes.stop);
            let cancel: *const Word = stop;
            for worker in &mut lanes.workers {
                let cx = &mut worker.get_mut().cx;
                cx.now_ns = now;
                cx.cancel = cancel;
            }
            let view = View {
                lanes: &*lanes,
                stop,
                steps: self.steps.as_ptr(),
                meta: self.meta,
                table: self.table,
                health: self.health.as_mut_ptr(),
                queues: self.queues.as_mut_ptr(),
                parts: SlotParts {
                    storage: self.storage.as_mut_ptr(),
                    inline: self.inline.as_mut_ptr(),
                },
                activity: self.activity,
                masked: self.masked,
                failure: self.failure,
                stamps: self.stamps,
                pending: self.pending.as_mut_ptr(),
                pending_cap: self.pending.len(),
                pending_len: &mut self.pending_len,
                progress: self.progress,
                fault_log: self.fault_log,
                tick: self.tick,
                any_blocking: self.any_blocking,
                slots: self.slots,
                reuse: self.reuse.as_mut().map(|table| table.parts()),
                probe: self.probe,
            };
            lanes.pool.run(&|number| work(&view, number));
            stop.replace_if(STOPPED, 0);
        }
        let error = self.collect_workers(lanes, report);
        match error {
            Some(error) => Err(error),
            None => {
                for word in 0..words {
                    self.dirty[word] |= self.carry[word];
                    self.carry[word] = 0;
                }
                Ok(())
            }
        }
    }

    /// Moves the dirty bits into the lanes, finds the steps that may run this tick, counts what
    /// each waits for, and queues the ones that wait for nothing. Returns how many may run.
    fn seed_lanes(&mut self, lanes: &mut Lanes) -> u32 {
        let count = self.steps.len();
        lanes.reached.fill(false);
        for index in 0..count {
            lanes.unfinished[index].store(0, Ordering::Relaxed);
            let dirty = bits::get(self.dirty, index);
            lanes.marks[index].store(if dirty { DIRTY } else { CLEAN }, Ordering::Relaxed);
            lanes.reached[index] = dirty;
        }
        self.dirty[..words_for(count)].fill(0);
        for index in 0..count {
            if lanes.reached[index] {
                for at in lanes.start[index]..lanes.start[index + 1] {
                    let waiter = lanes.after[at as usize];
                    lanes.reached[waiter as usize] = true;
                }
            }
        }
        let mut remaining = 0;
        for index in 0..count {
            if lanes.reached[index] {
                remaining += 1;
                for &waiter in lanes.waiters(index) {
                    lanes.unfinished[waiter as usize].fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        lanes.ready.begin(remaining);
        for index in 0..count {
            if lanes.reached[index] && lanes.unfinished[index].load(Ordering::Relaxed) == 0 {
                lanes.ready.seed(lanes.rank[index]);
            }
        }
        remaining
    }

    /// Folds the workers' results into the tick, restores unfinished dirty bits and carries, and
    /// returns the error of the lowest failing step.
    fn collect_workers(&mut self, lanes: &mut Lanes, report: &mut TickReport) -> Option<RunError> {
        let mut error: Option<(u32, RunError)> = None;
        for worker in &mut lanes.workers {
            let worker = worker.get_mut();
            let tick = core::mem::take(&mut worker.report);
            report.step_calls += tick.step_calls;
            report.faults += tick.faults;
            report.skipped += tick.skipped;
            report.retries += tick.retries;
            report.word_reads += tick.word_reads;
            let counters = core::mem::take(&mut worker.counters);
            self.counters.backoff_skips += counters.backoff_skips;
            self.counters.deferrals += counters.deferrals;
            self.counters.pending += counters.pending;
            self.counters.reuse_checks += counters.reuse_checks;
            self.counters.reuse_replays += counters.reuse_replays;
            self.counters.dropped += counters.dropped;
            if let Some(found) = worker.error.take()
                && error.as_ref().is_none_or(|old| outranks(&found, old))
            {
                error = Some(found);
            }
        }
        for index in 0..self.steps.len() {
            match lanes.marks[index].swap(CLEAN, Ordering::Relaxed) {
                DIRTY => bits::set(self.dirty, index),
                CARRY => bits::set(self.carry, index),
                _ => {}
            }
        }
        let error = error.map(|(_, failure)| failure);
        if matches!(error, Some(RunError::Cancelled { .. })) {
            self.count_cancelled();
        }
        error
    }
}
