//! The tick loop. An idle tick is one OR per word and one zero test.

use daedalus_abi::{Slot, SlotState, Slots, StepEnd};

use crate::bits::{self, words_for};
use crate::error::RunError;
use crate::owned;
use crate::policy::Recovery;
use crate::step::{SlotKind, StepMeta};
use crate::{Pending, Runner, spanned};

/// What one tick did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TickReport {
    /// The tick number, counting from 1.
    pub tick: u32,
    /// True when nothing was dirty and no fence was waiting: the idle path.
    pub idle: bool,
    /// How many step bodies were called.
    pub step_calls: u32,
    /// How many of those returned `Fault`.
    pub faults: u32,
    /// Dirty words read, counting each re-read after a step.
    pub word_reads: u32,
    /// How many calls returned `Skipped`: an input the body dropped, counted apart from faults.
    pub skipped: u32,
    /// How many extra calls a `Retry` recovery made in this tick.
    pub retries: u32,
}

impl Runner<'_> {
    /// Runs one tick.
    pub fn tick(&mut self) -> Result<TickReport, RunError> {
        self.tick = self.tick.wrapping_add(1);
        let mut report = TickReport {
            tick: self.tick,
            ..TickReport::default()
        };
        let words = words_for(self.steps.len());
        let mut any = 0;
        for word in 0..words {
            self.dirty[word] |= self.always[word];
            any |= self.dirty[word];
        }
        report.word_reads = words as u32;
        if any == 0 && self.pending_len == 0 {
            report.idle = true;
            return Ok(report);
        }
        if self.pending_len > 0 {
            self.poll_fences(&mut report);
        }
        for word in 0..words {
            loop {
                let bits = self.dirty[word];
                if bits == 0 {
                    break;
                }
                let bit = bits.trailing_zeros();
                self.dirty[word] = bits & !(1 << bit);
                report.word_reads += 1;
                self.step_once(word * 32 + bit as usize, &mut report)?;
            }
        }
        for word in 0..words {
            self.dirty[word] |= self.carry[word];
            self.carry[word] = 0;
        }
        Ok(report)
    }

    /// Calls step `index` once, or runs its recovery, and applies what it returned.
    fn step_once(&mut self, index: usize, report: &mut TickReport) -> Result<(), RunError> {
        if self.masked && !bits::get(self.activity, index) {
            return Ok(());
        }
        let meta: StepMeta = self.meta[index];
        let health = self.health[index];
        if health.disabled() {
            self.drop_inputs(meta);
            return Ok(());
        }
        if health.backing_off() {
            if self.cx.now_ns < health.until_ns() {
                // Still in backoff: stay dirty, so the step is tried again on a later tick.
                bits::set(self.carry, index);
                return Ok(());
            }
            self.health[index].backing_off = false;
        }
        let mut attempt = 0;
        let end = loop {
            self.load_cells(meta);
            let (run, data) = (self.steps[index].run, self.steps[index].data);
            self.cx.flags = self.call_flags(index);
            let mut slots = Slots::of(&mut self.cells[..self.slots]);
            report.step_calls += 1;
            // SAFETY: `run` and `data` come from one `Step`, so the body was built to take that data.
            // `slots` and `cx` are exclusive to this call, and they stay valid until it returns.
            let end = unsafe { run(data, &mut slots, &mut self.cx) };
            // The restart flag is spent by the call it was set for.
            self.health[index].restart = false;
            let retry = match meta.recovery {
                Recovery::Retry { max } => attempt < max,
                _ => false,
            };
            if matches!(end, StepEnd::Fault) && retry {
                self.discard_outputs(meta);
                attempt += 1;
                report.retries += 1;
                continue;
            }
            break end;
        };
        match end {
            StepEnd::Done => {
                self.record_success(index);
                self.commit_inputs(meta);
                self.publish(meta)?;
                for &to in spanned(self.table, meta.wake) {
                    self.wake(to);
                }
            }
            StepEnd::Skipped => {
                self.commit_inputs(meta);
                self.discard_outputs(meta);
                report.skipped += 1;
            }
            StepEnd::Pending(fence) => {
                // The outputs are stored now. Consumers wait for the fence to be Ready.
                self.commit_inputs(meta);
                self.publish(meta)?;
                let entry = Pending {
                    step: index as u32,
                    fence,
                };
                if !self.register_pending(entry) {
                    return Err(RunError::PendingFull { step: index as u32 });
                }
            }
            StepEnd::Fault => {
                // Containment: the tick is dropped. Its inputs are consumed, its outputs are
                // released, and no consumer is woken.
                report.faults += 1;
                self.discard_outputs(meta);
                self.commit_inputs(meta);
                self.record_failure(index, meta.recovery);
                if meta.recovery != Recovery::SkipTick && self.failure.fail_fast() {
                    return Err(RunError::Fault { step: index as u32 });
                }
            }
        }
        Ok(())
    }

    /// Sets the input cells from their queues, and clears the output cells, before a call.
    fn load_cells(&mut self, meta: StepMeta) {
        for &slot in spanned(self.table, meta.inputs) {
            let slot = slot as usize;
            let queue = self.queues[slot];
            self.cells[slot] = if queue.len > 0 {
                self.storage[queue.head_index()]
            } else {
                Slot::EMPTY
            };
        }
        for &slot in spanned(self.table, meta.outputs) {
            self.cells[slot as usize] = Slot::EMPTY;
        }
    }

    /// Drops every value queued on a disabled step's inputs, releasing each once.
    fn drop_inputs(&mut self, meta: StepMeta) {
        for &slot in spanned(self.table, meta.inputs) {
            let slot = slot as usize;
            while self.queues[slot].len > 0 {
                let at = self.queues[slot].head_index();
                // SAFETY: the queue owns its head entry, and the pop below forgets it, so the
                // reference is released exactly once.
                unsafe { owned::release(&self.storage[at]) };
                self.queues[slot].pop_head();
            }
            self.queues[slot].unconsumed = 0;
        }
    }

    /// Removes what the step consumed. A consumed entry that the step didn't move out is released
    /// here, once. A moved entry's reference went to an output, so it is only forgotten.
    fn commit_inputs(&mut self, meta: StepMeta) {
        for &slot in spanned(self.table, meta.inputs) {
            let slot = slot as usize;
            let queue = self.queues[slot];
            let moved = self.cells[slot].state == SlotState::Moved as u32;
            if queue.kind == Some(SlotKind::Fifo) && queue.len > 0 && !moved {
                // SAFETY: the queue owns its head entry, and the queue forgets it on the commit
                // below, so the reference is released exactly once.
                unsafe { owned::release(&self.storage[queue.head_index()]) };
            }
            self.queues[slot].commit();
        }
    }

    /// Stores every output the body wrote, moving each one's reference into its queue. A cell the
    /// body left empty stores nothing. On overflow, the rest of the outputs are released.
    fn publish(&mut self, meta: StepMeta) -> Result<(), RunError> {
        let mut overflow = None;
        for &slot in spanned(self.table, meta.outputs) {
            let slot = slot as usize;
            let cell = self.cells[slot];
            if cell.state != SlotState::Full as u32 {
                continue;
            }
            self.cells[slot] = Slot::EMPTY;
            if overflow.is_some() || self.queues[slot].is_full() {
                // SAFETY: the cell is Full and this step wrote it, so the reference is still the
                // cell's, and it is released once here.
                unsafe { owned::release(&cell) };
                overflow.get_or_insert(slot as u32);
                continue;
            }
            let at = self.queues[slot].tail_index();
            let raw = owned::place(cell, &mut self.inline[at]);
            self.queues[slot].push(&mut *self.storage, raw);
        }
        match overflow {
            Some(slot) => Err(RunError::Overflow { slot }),
            None => Ok(()),
        }
    }

    /// Releases the outputs a skipped or failed step wrote but did not publish.
    fn discard_outputs(&mut self, meta: StepMeta) {
        for &slot in spanned(self.table, meta.outputs) {
            let slot = slot as usize;
            let cell = self.cells[slot];
            if cell.state == SlotState::Full as u32 {
                // SAFETY: the cell is Full and this step wrote it, so it owns the reference.
                unsafe { owned::release(&cell) };
            }
            self.cells[slot] = Slot::EMPTY;
        }
    }
}
