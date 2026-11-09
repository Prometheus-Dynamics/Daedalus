//! Cooperative cancellation. A host owns one word; the tick reads it before each step and stops at
//! that boundary. Steps not yet run stay dirty for the next tick, so no value is lost.

use daedalus_abi::{StepCx, StepEnd, Word};

use crate::bits::words_for;
use crate::{RunError, Runner};

/// The reason a body gives when it stops early because of a cancel.
pub const CANCEL_REASON: &str = "cancelled";

/// The host's cancel word. A `Runner` borrows it, and it outlives the runner.
pub struct CancelWord(Word);

impl CancelWord {
    pub const fn new() -> CancelWord {
        CancelWord(Word::new(0))
    }

    /// Asks the run to stop at the next step boundary.
    ///
    /// Cancellation is cooperative: a body that never checks [`StepCx::cancelled`] runs to its
    /// end, and only then does the tick stop.
    pub fn cancel(&self) {
        self.0.set(1);
    }

    /// Clears the request, so the next tick runs what the cancelled one left dirty.
    pub fn clear(&self) {
        self.0.set(0);
    }

    /// Whether a cancel is being asked for.
    pub fn is_set(&self) -> bool {
        self.0.get() != 0
    }
}

/// Ends a body that saw a cancel: returns `Skipped` with the reason [`CANCEL_REASON`]. Not a fault,
/// so it starts no backoff.
pub fn skip_cancelled(cx: &mut StepCx) -> StepEnd {
    cx.fault(None, CANCEL_REASON);
    StepEnd::Skipped
}

impl Default for CancelWord {
    fn default() -> CancelWord {
        CancelWord::new()
    }
}

impl<'a> Runner<'a> {
    /// Attaches the host's cancel word. Every tick from now on reads it before each step.
    pub fn set_cancel(&mut self, word: &'a CancelWord) {
        self.cx.cancel = &word.0;
    }

    /// Counts and reports a cancel that stopped the tick before step `index`. The step's dirty bit
    /// is still set, so it runs on the next tick.
    pub(crate) fn stop_cancelled(&mut self, index: usize) -> RunError {
        self.count_cancelled();
        RunError::Cancelled { step: index as u32 }
    }

    /// Adds the steps a cancel left dirty to the counters.
    pub(crate) fn count_cancelled(&mut self) {
        let words = words_for(self.steps.len());
        let left: u32 = self.dirty[..words]
            .iter()
            .map(|word| word.count_ones())
            .sum();
        self.counters.cancelled += u64::from(left);
    }
}
