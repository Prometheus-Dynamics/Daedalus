//! The tick loop. An idle tick is one OR per word and one zero test.

use crate::Runner;
use crate::bits::words_for;
use crate::error::RunError;
use crate::exec::step_once;

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
        let probe = self.probe;
        let start = probe.and_then(|probe| probe.begin_tick(true));
        let result = self.run_tick(&mut report);
        if let Some(probe) = probe {
            probe.finish_tick(start, report.step_calls);
        }
        self.counters.absorb(&report);
        result.map(|()| report)
    }

    /// The tick's work, with its report filled in as it goes.
    fn run_tick(&mut self, report: &mut TickReport) -> Result<(), RunError> {
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
        for word in 0..words {
            loop {
                let bits = self.dirty[word];
                if bits == 0 {
                    break;
                }
                let bit = bits.trailing_zeros();
                if self.cx.cancelled() {
                    return Err(self.stop_cancelled(word * 32 + bit as usize));
                }
                self.dirty[word] = bits & !(1 << bit);
                report.word_reads += 1;
                step_once(self, word * 32 + bit as usize, report)?;
            }
        }
        for word in 0..words {
            self.dirty[word] |= self.carry[word];
            self.carry[word] = 0;
        }
        Ok(())
    }
}
