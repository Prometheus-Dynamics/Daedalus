//! The tick loop. An idle tick is one OR per word and one zero test.

use daedalus_abi::{Slot, SlotState, Slots, StepEnd};

use crate::bits::{self, words_for};
use crate::error::RunError;
use crate::step::StepMeta;
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

    /// Calls step `index` once and applies what it returned.
    fn step_once(&mut self, index: usize, report: &mut TickReport) -> Result<(), RunError> {
        if self.masked && !bits::get(self.activity, index) {
            return Ok(());
        }
        let meta: StepMeta = self.meta[index];
        let table = self.table;
        for &slot in spanned(table, meta.inputs) {
            let slot = slot as usize;
            let queue = self.queues[slot];
            self.cells[slot] = if queue.len > 0 {
                self.storage[queue.head_index()]
            } else {
                Slot::EMPTY
            };
        }
        for &slot in spanned(table, meta.outputs) {
            self.cells[slot as usize] = Slot::EMPTY;
        }
        let (run, data) = (self.steps[index].run, self.steps[index].data);
        let mut slots = Slots::of(&mut self.cells[..self.slots]);
        report.step_calls += 1;
        // SAFETY: `run` and `data` come from one `Step`, so the body was built to take that data.
        // `slots` and `cx` are exclusive to this call, and they stay valid until it returns.
        let end = unsafe { run(data, &mut slots, &mut self.cx) };
        match end {
            StepEnd::Done => {
                self.commit_inputs(meta);
                self.publish(meta)?;
                for &to in spanned(table, meta.wake) {
                    self.wake(to);
                }
            }
            StepEnd::Skipped => self.commit_inputs(meta),
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
            StepEnd::Fault => report.faults += 1,
        }
        Ok(())
    }

    fn commit_inputs(&mut self, meta: StepMeta) {
        for &slot in spanned(self.table, meta.inputs) {
            self.queues[slot as usize].commit();
        }
    }

    /// Stores every output the body wrote. A cell the body left empty stores nothing.
    fn publish(&mut self, meta: StepMeta) -> Result<(), RunError> {
        for &slot in spanned(self.table, meta.outputs) {
            let slot = slot as usize;
            if self.cells[slot].state != SlotState::Full as u32 {
                continue;
            }
            let queue = &mut self.queues[slot];
            if queue.is_full() {
                return Err(RunError::Overflow { slot: slot as u32 });
            }
            queue.push(&mut *self.storage, self.cells[slot]);
        }
        Ok(())
    }
}
