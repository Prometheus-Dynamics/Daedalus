//! Per-tick slots: a value lives for the tick it was written in. When a tick ends, what a
//! per-tick slot still holds is released, unless one of its readers was held back in that tick
//! (waiting on its fence, backing off, behind a blocked output, or masked): then it stays until
//! the end of the first tick that reader was free to run. A graph with no such slot pays one test
//! at the end of a tick that did work, and nothing on an idle one.

use daedalus_abi::NO_SLOT;

use crate::bits::words_for;
use crate::exec::Env;
use crate::{RunError, Runner, owned, spanned};

impl Runner<'_> {
    /// Makes `slot` per-tick, or an ordinary slot again. A loader calls this with the edge's
    /// policy.
    pub fn set_per_tick(&mut self, slot: u32, per_tick: bool) -> Result<(), RunError> {
        let index = self.queue_index(slot)?;
        self.queues[index].per_tick = per_tick;
        self.any_per_tick = self.queues.iter().any(|queue| queue.per_tick);
        Ok(())
    }

    /// Ends a tick that did work: empties the per-tick slots, and makes the carried steps dirty
    /// for the next tick.
    pub(crate) fn end_tick(&mut self) {
        if self.any_per_tick {
            self.end_per_tick();
        }
        for word in 0..words_for(self.steps.len()) {
            self.dirty[word] |= self.carry[word];
            self.carry[word] = 0;
        }
    }

    fn end_per_tick(&mut self) {
        // A reader held back this tick keeps what waits for it.
        for step in 0..self.steps.len() {
            if !crate::bits::get(self.carry, step) && self.active(step) {
                continue;
            }
            for &slot in spanned(self.table, self.meta[step].inputs) {
                if slot != NO_SLOT && self.queues[slot as usize].per_tick {
                    self.queues[slot as usize].spared = true;
                }
            }
        }
        for queue in self.queues.iter_mut().filter(|queue| queue.per_tick) {
            if core::mem::take(&mut queue.spared) {
                continue;
            }
            // A hidden value belongs to the tick it is shown in, so it stays.
            while queue.len > 0 {
                // SAFETY: the queue owns its head entry, and the pop below forgets it, so the
                // reference is released exactly once.
                unsafe { owned::release(&self.storage[queue.head_index()]) };
                queue.pop_head();
            }
        }
    }
}
