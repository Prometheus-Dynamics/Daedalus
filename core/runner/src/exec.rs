//! One step's run, written once for the serial tick and the parallel lanes. Each supplies an
//! [`Env`], so both run the same logic and cannot drift apart.

use daedalus_abi::{NO_NODE, NO_SLOT, Progress, Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_value::Decision;

use crate::backpressure::output_blocked;
use crate::error::RunError;
use crate::failure::{FailurePolicy, StepHealth};
use crate::fault_log::FaultLog;
use crate::fence::insert_pending;
use crate::join::join_ready;
use crate::owned::{self, InlineCell};
use crate::policy::Recovery;
use crate::reuse::{ReuseParts, forget, replay, stamp_versions, store};
use crate::step::{Queue, SlotStamp, StepMeta};
use crate::telemetry::Counters;
use crate::tick::TickReport;
use crate::values::LENT;
use crate::{Pending, Runner, spanned};

/// A step's entry point.
pub(crate) type RunFn = unsafe extern "C" fn(*mut (), *mut Slots, *mut StepCx) -> StepEnd;

/// What a publish did with a value. A drop under the slot's policy is not a failure.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stored {
    Kept,
    Dropped,
    Full,
}

/// The queue storage and inline buffers of a runner. The caller of each method holds exclusive
/// access to the slot's own range of these buffers.
#[derive(Clone, Copy)]
pub(crate) struct SlotParts {
    pub storage: *mut Slot,
    pub inline: *mut InlineCell,
}

impl SlotParts {
    /// The head entry of `queue`, or an empty cell.
    ///
    /// # Safety
    /// The caller has exclusive access to this queue's storage range, or shared access while no
    /// one writes it.
    unsafe fn head(self, queue: &Queue) -> Slot {
        if queue.len > 0 {
            // SAFETY: `head_index` is inside the queue's range, which the load sized in `storage`.
            unsafe { *self.storage.add(queue.head_index()) }
        } else {
            Slot::EMPTY
        }
    }

    /// Pushes a replayed value, which keeps its version. False when the queue is full.
    ///
    /// # Safety
    /// The caller has exclusive access to this queue's storage and inline range.
    pub(crate) unsafe fn replay(self, queue: &mut Queue, cell: Slot) -> bool {
        if queue.is_full() {
            return false;
        }
        let at = queue.tail_index();
        // SAFETY: `at` is inside this queue's inline range, which the caller holds exclusively.
        let raw = owned::place(cell, unsafe { &mut *self.inline.add(at) });
        queue.count_replay();
        // SAFETY: `at` is inside this queue's storage range, which the caller holds exclusively.
        unsafe { self.storage.add(at).write(raw) };
        true
    }

    /// Stores `cell` at the back of `queue`, moving its reference in. False when the queue is full.
    /// With `hide`, the value is stored where the next one goes and not counted: no reader sees
    /// it until [`Queue::show_hidden`]. A step that is still working publishes this way.
    ///
    /// # Safety
    /// The caller has exclusive access to this queue's storage and inline range.
    unsafe fn publish(self, queue: &mut Queue, cell: Slot, now_ns: u64, hide: bool) -> Stored {
        if queue.hidden {
            // One hidden value a queue: its producer runs once at a time.
            return Stored::Full;
        }
        let decision = queue
            .pressure
            .decide(queue.len, queue.cap, queue.opened_ns, now_ns);
        match decision {
            Decision::Store => {
                if queue.len == 0 && !hide {
                    queue.opened_ns = now_ns;
                }
            }
            Decision::Refuse => return Stored::Full,
            Decision::Discard(_) => {
                // SAFETY: the caller's cell owns its reference, and it is given up here.
                unsafe { owned::release(&cell) };
                return Stored::Dropped;
            }
            Decision::Evict(_) => {
                // SAFETY: the queue owns its head entry, and the pop below forgets it.
                unsafe { owned::release(&*self.storage.add(queue.head_index())) };
                queue.pop_head();
                queue.opened_ns = now_ns;
                self.place(queue, cell, hide);
                return Stored::Dropped;
            }
        }
        self.place(queue, cell, hide);
        Stored::Kept
    }

    /// Writes `cell` at the back of `queue`, which has room: counted and stamped, or hidden.
    fn place(self, queue: &mut Queue, cell: Slot, hide: bool) {
        let at = queue.tail_index();
        // SAFETY: `at` is inside this queue's inline range, which the caller holds exclusively.
        let raw = owned::place(cell, unsafe { &mut *self.inline.add(at) });
        let raw = if hide {
            queue.hidden = true;
            raw
        } else {
            queue.stamp(raw)
        };
        // SAFETY: `at` is inside this queue's storage range, which the caller holds exclusively.
        unsafe { self.storage.add(at).write(raw) };
    }

    /// Releases and removes every queued value.
    ///
    /// # Safety
    /// The caller has exclusive access to this queue's storage range.
    pub(crate) unsafe fn drop_all(self, queue: &mut Queue) {
        while queue.len > 0 {
            // SAFETY: the queue owns its head entry, and the pop below forgets it, so the
            // reference is released exactly once.
            unsafe { owned::release(&*self.storage.add(queue.head_index())) };
            queue.pop_head();
        }
        if let Some(at) = queue.take_hidden() {
            // SAFETY: the queue owned its hidden entry, and `take_hidden` forgot it.
            unsafe { owned::release(&*self.storage.add(at)) };
        }
        queue.unconsumed = 0;
    }

    /// Removes the consumed head. Unless the step moved it out, it is released here, once. A
    /// queue that keeps its value removes nothing: its cells are lent, so a step that took from
    /// one got a reference of its own, never the queue's.
    ///
    /// # Safety
    /// The caller has exclusive access to this queue's storage range.
    pub(crate) unsafe fn commit(self, queue: &mut Queue, moved: bool) {
        debug_assert!(
            !(moved && queue.keeps()),
            "a cell of a slot that keeps its value was marked moved"
        );
        if !queue.keeps() && queue.len > 0 && !moved {
            // SAFETY: the queue owns its head entry and forgets it on the commit below.
            unsafe { owned::release(&*self.storage.add(queue.head_index())) };
        }
        queue.commit();
    }
}

/// What a step's run needs from whoever is running it.
pub(crate) trait Env<'t> {
    fn meta(&self, index: usize) -> StepMeta;
    fn table(&self) -> &'t [u32];
    fn step_fn(&self, index: usize) -> (RunFn, *mut ());
    fn health(&mut self, index: usize) -> &mut StepHealth;
    /// `index`'s progress cells, or null when nobody attached any.
    fn progress(&self, index: usize) -> *const Progress;
    /// The frame a body sees, and the context for the call.
    fn parts(&mut self) -> (&mut [Slot], &mut StepCx);
    /// The attached probe, if any.
    fn probe(&self) -> Option<&'t crate::probe::TickProbe<'t>>;
    /// The reuse table, if the plan has one.
    fn reuse(&mut self) -> Option<ReuseParts>;
    fn failure(&self) -> FailurePolicy;
    /// Whether any slot is a blocking edge, so the check costs nothing when none is.
    fn any_blocking(&self) -> bool;
    /// How many slots the plan has.
    fn slot_count(&self) -> usize;
    /// Where a failure goes, and the tick it happened in.
    fn fault_log(&self) -> (FaultLog, u32);
    /// What an output written to `slot` is stamped with, if the plan says.
    fn stamp(&self, slot: usize) -> Option<SlotStamp>;
    fn counters(&mut self) -> &mut Counters;
    /// False when the activity mask holds this step back.
    fn active(&self, index: usize) -> bool;
    /// Runs `f` with the queue of `slot`, holding whatever protects it.
    fn with_queue<R>(&mut self, slot: usize, f: impl FnOnce(&mut Queue, SlotParts) -> R) -> R;
    fn wake(&mut self, step: u32);
    /// Keeps `step` dirty for the next tick.
    fn carry(&mut self, step: usize);
    fn register_pending(&mut self, entry: Pending) -> bool;
    /// Whether `step` started work whose fence hasn't resolved.
    fn is_pending(&mut self, step: usize) -> bool;
}

/// Calls step `index` once, or runs its recovery, and applies what it returned.
pub(crate) fn step_once<'t, E: Env<'t>>(
    env: &mut E,
    index: usize,
    report: &mut TickReport,
) -> Result<(), RunError> {
    if !env.active(index) {
        env.counters().deferrals += 1;
        return Ok(());
    }
    let meta = env.meta(index);
    let health = *env.health(index);
    if health.disabled() {
        drop_inputs(env, meta);
        return Ok(());
    }
    if health.backing_off() {
        let now = env.parts().1.now_ns;
        if now < health.until_ns() {
            // Still in backoff: stay dirty, so the step is tried again on a later tick.
            env.counters().backoff_skips += 1;
            env.carry(index);
            return Ok(());
        }
        env.health(index).backing_off = false;
    }
    if meta.may_pend && env.is_pending(index) {
        // Its last run is still going. One run at a time: stay dirty, and run again once the
        // fence has resolved. Without this a second run would publish over outputs nobody has
        // been woken for yet, and take a second place in the pending list.
        env.carry(index);
        return Ok(());
    }
    if env.any_blocking() && output_blocked(env, meta) {
        env.counters().backpressure_deferrals += 1;
        env.carry(index);
        return Ok(());
    }
    if meta.required.len != 0 && !join_ready(env, meta) {
        return Ok(());
    }
    if meta.reuse && replay(env, index, meta)? {
        return Ok(());
    }
    let (run, data) = env.step_fn(index);
    let mut attempt = 0;
    let end = loop {
        load_cells(env, meta, attempt != 0);
        let flags = env.health(index).call_flags();
        let probe = env.probe();
        let progress = env.progress(index);
        let (cells, cx) = env.parts();
        cx.flags = flags;
        cx.progress = progress;
        cx.step = index as u32;
        // A body that fails without saying why mustn't inherit the last one's reason.
        cx.reason_len = 0;
        cx.at = NO_NODE;
        cx.code = 0;
        let mut slots = Slots::of(&mut cells[..port_count(meta)]);
        report.step_calls += 1;
        // SAFETY: `run` and `data` come from one `Step`, so the body was built to take that data.
        // `slots` and `cx` are exclusive to this call, and they stay valid until it returns.
        let end = unsafe { run(data, &mut slots, cx) };
        if let Some(probe) = probe {
            probe.add_step(index);
        }
        let health = env.health(index);
        health.calls = health.calls.saturating_add(1);
        // The restart flag is spent by the call it was set for.
        health.restart = false;
        let retry = match meta.recovery {
            Recovery::Retry { max } => attempt < max,
            _ => false,
        };
        if matches!(end, StepEnd::Fault) && retry {
            discard_outputs(env, meta);
            attempt += 1;
            report.retries += 1;
            continue;
        }
        break end;
    };
    match end {
        StepEnd::Done => {
            env.health(index).on_success();
            if meta.reuse {
                store(env, index, meta);
            }
            commit_inputs(env, meta);
            if let Err(error) = publish(env, meta, false) {
                if meta.reuse {
                    forget(env, index, meta);
                }
                return Err(error);
            }
            if meta.reuse {
                stamp_versions(env, index, meta);
            }
            for &to in spanned(env.table(), meta.wake) {
                env.wake(to);
            }
        }
        StepEnd::Skipped => {
            commit_inputs(env, meta);
            discard_outputs(env, meta);
            report.skipped += 1;
        }
        StepEnd::Pending(fence) => {
            // The outputs are stored now, hidden: no reader sees them until the fence is Ready.
            commit_inputs(env, meta);
            let kept = publish(env, meta, true)?;
            let step = index as u32;
            if !env.register_pending(Pending { step, fence, kept }) {
                return Err(RunError::PendingFull { step });
            }
            env.counters().pending += 1;
        }
        StepEnd::Fault => {
            // Containment: the tick is dropped. Its inputs are consumed, its outputs are
            // released, and no consumer is woken.
            report.faults += 1;
            if meta.reuse {
                forget(env, index, meta);
            }
            discard_outputs(env, meta);
            commit_inputs(env, meta);
            let now = env.parts().1.now_ns;
            let policy = env.failure();
            env.health(index).on_failure(policy, now, meta.recovery);
            let (log, tick) = env.fault_log();
            let health = *env.health(index);
            log.record(index, tick, env.parts().1, &health);
            if meta.recovery != Recovery::SkipTick && policy.fail_fast() {
                return Err(RunError::Fault { step: index as u32 });
            }
        }
    }
    Ok(())
}

/// How many cells a step's frame has.
pub(crate) fn port_count(meta: StepMeta) -> usize {
    (meta.inputs.len + meta.outputs.len) as usize
}

/// The frame position of output `at`.
pub(crate) fn output_at(meta: StepMeta, at: usize) -> usize {
    meta.inputs.len as usize + at
}

/// Fills the frame's inputs from their queues and clears its outputs, before a call.
///
/// A queue that pops gives its head to the step that reads it: the body may take the reference,
/// and the cell says so by being `Moved`. A queue that keeps its value, for several readers,
/// can't give its own reference away, so the cell is marked [`LENT`]: taking from it gives the
/// body another reference and leaves the cell as it is. A body that only looks pays nothing.
///
/// With `retry`, this is another call after a fault. An input the failed call took out of a queue
/// that pops is gone, so its cell stays `Moved`: the queue's entry is no longer the queue's to
/// hand out.
pub(crate) fn load_cells<'t, E: Env<'t>>(env: &mut E, meta: StepMeta, retry: bool) {
    for (at, &slot) in spanned(env.table(), meta.inputs).iter().enumerate() {
        if slot == NO_SLOT {
            env.parts().0[at] = Slot::EMPTY;
            continue;
        }
        if retry && env.parts().0[at].state == SlotState::Moved as u32 {
            continue;
        }
        let head = env.with_queue(slot as usize, |queue, parts| {
            // SAFETY: `with_queue` holds what protects this queue, and only this step's slot
            // range is read.
            let mut head = unsafe { parts.head(queue) };
            if queue.keeps() {
                head.reserved |= LENT;
            }
            head
        });
        env.parts().0[at] = head;
    }
    for at in 0..meta.outputs.len as usize {
        env.parts().0[output_at(meta, at)] = Slot::EMPTY;
    }
}

/// Drops every value queued on a disabled step's inputs, releasing each once.
fn drop_inputs<'t, E: Env<'t>>(env: &mut E, meta: StepMeta) {
    for &slot in spanned(env.table(), meta.inputs) {
        if slot == NO_SLOT {
            continue;
        }
        // SAFETY: `with_queue` holds what protects this queue.
        env.with_queue(slot as usize, |queue, parts| unsafe {
            parts.drop_all(queue)
        });
    }
}

/// Removes what the step consumed. A moved entry's reference went with the body, so it is only
/// forgotten.
pub(crate) fn commit_inputs<'t, E: Env<'t>>(env: &mut E, meta: StepMeta) {
    for (at, &slot) in spanned(env.table(), meta.inputs).iter().enumerate() {
        if slot == NO_SLOT {
            continue;
        }
        let slot = slot as usize;
        let moved = env.parts().0[at].state == SlotState::Moved as u32;
        // SAFETY: `with_queue` holds what protects this queue.
        env.with_queue(slot, |queue, parts| unsafe { parts.commit(queue, moved) });
    }
}

/// Stores every output the body wrote, moving each one's reference into its queue. A cell the
/// body left empty stores nothing. On overflow, the rest of the outputs are released.
/// Publishes a step's outputs, hidden when `hide` is set. Returns a mask with bit `at` set for
/// each output that was stored, so a pending step's values can be shown when its fence is
/// ready, or taken back if it fails.
fn publish<'t, E: Env<'t>>(env: &mut E, meta: StepMeta, hide: bool) -> Result<u32, RunError> {
    let mut overflow = None;
    let mut kept = 0u32;
    for (at, &slot) in spanned(env.table(), meta.outputs).iter().enumerate() {
        let mut cell = core::mem::replace(&mut env.parts().0[output_at(meta, at)], Slot::EMPTY);
        if cell.state != SlotState::Full as u32 {
            continue;
        }
        if slot == NO_SLOT {
            // SAFETY: the cell is Full and this step wrote it, and nothing reads this output, so
            // the reference is released once here.
            unsafe { owned::release(&cell) };
            continue;
        }
        let slot = slot as usize;
        // A body that passed a lent input on as it was wrote the mark too. A stored value is
        // the queue's own.
        cell.reserved &= !LENT;
        if let Some(stamp) = env.stamp(slot) {
            cell.type_index = stamp.ty.0;
            cell.space = u32::from(stamp.space.0);
        }
        let now = env.parts().1.now_ns;
        let stored = match overflow {
            // SAFETY: `with_queue` holds what protects this queue, and this step is its producer.
            None => env.with_queue(slot, |queue, parts| unsafe {
                parts.publish(queue, cell, now, hide)
            }),
            Some(_) => Stored::Full,
        };
        if stored == Stored::Dropped {
            env.counters().dropped += 1;
            let counted = env.with_queue(slot, |queue, _| {
                if queue.counts_rejects {
                    queue.rejected = queue.rejected.wrapping_add(1);
                }
                queue.counts_rejects
            });
            if counted {
                env.counters().backpressure_events += 1;
            }
        }
        if stored == Stored::Kept && at < 32 {
            kept |= 1 << at;
        }
        if stored == Stored::Full {
            // SAFETY: the cell is Full and this step wrote it, so the reference is still the
            // cell's, and it is released once here.
            unsafe { owned::release(&cell) };
            overflow.get_or_insert(slot as u32);
        }
    }
    match overflow {
        Some(slot) => Err(RunError::Overflow { slot }),
        None => Ok(kept),
    }
}

/// Releases the outputs a skipped or failed step wrote but did not publish.
fn discard_outputs<'t, E: Env<'t>>(env: &mut E, meta: StepMeta) {
    for at in 0..meta.outputs.len as usize {
        let cell = core::mem::replace(&mut env.parts().0[output_at(meta, at)], Slot::EMPTY);
        if cell.state == SlotState::Full as u32 {
            // SAFETY: the cell is Full and this step wrote it, so it owns the reference.
            unsafe { owned::release(&cell) };
        }
    }
}

impl<'a> Env<'a> for Runner<'a> {
    #[inline]
    fn meta(&self, index: usize) -> StepMeta {
        self.meta[index]
    }
    #[inline]
    fn table(&self) -> &'a [u32] {
        self.table
    }
    #[inline]
    fn step_fn(&self, index: usize) -> (RunFn, *mut ()) {
        (self.steps[index].run, self.steps[index].data)
    }
    #[inline]
    fn health(&mut self, index: usize) -> &mut StepHealth {
        &mut self.health[index]
    }
    #[inline]
    fn any_blocking(&self) -> bool {
        self.any_blocking
    }
    #[inline]
    fn slot_count(&self) -> usize {
        self.slots
    }
    #[inline]
    fn fault_log(&self) -> (FaultLog, u32) {
        (self.fault_log, self.tick)
    }
    #[inline]
    fn progress(&self, index: usize) -> *const Progress {
        crate::progress::cell(self.progress, index)
    }
    #[inline]
    fn parts(&mut self) -> (&mut [Slot], &mut StepCx) {
        (&mut *self.cells, &mut self.cx)
    }
    #[inline]
    fn probe(&self) -> Option<&'a crate::probe::TickProbe<'a>> {
        self.probe
    }
    #[inline]
    fn reuse(&mut self) -> Option<ReuseParts> {
        self.reuse.as_mut().map(|table| table.parts())
    }
    #[inline]
    fn failure(&self) -> FailurePolicy {
        self.failure
    }
    #[inline]
    fn stamp(&self, slot: usize) -> Option<SlotStamp> {
        self.stamps.get(slot).copied()
    }
    #[inline]
    fn counters(&mut self) -> &mut Counters {
        &mut self.counters
    }
    #[inline]
    fn active(&self, index: usize) -> bool {
        !self.masked || crate::bits::get(self.activity, index)
    }
    #[inline]
    fn with_queue<R>(&mut self, slot: usize, f: impl FnOnce(&mut Queue, SlotParts) -> R) -> R {
        let parts = SlotParts {
            storage: self.storage.as_mut_ptr(),
            inline: self.inline.as_mut_ptr(),
        };
        f(&mut self.queues[slot], parts)
    }
    #[inline]
    fn wake(&mut self, step: u32) {
        Runner::wake(self, step);
    }
    #[inline]
    fn carry(&mut self, step: usize) {
        crate::bits::set(self.carry, step);
    }
    #[inline]
    fn register_pending(&mut self, entry: Pending) -> bool {
        insert_pending(self.pending, &mut self.pending_len, entry)
    }
    #[inline]
    fn is_pending(&mut self, step: usize) -> bool {
        crate::fence::holds(&self.pending[..self.pending_len], step)
    }
}
