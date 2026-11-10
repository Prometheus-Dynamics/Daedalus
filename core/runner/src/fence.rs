//! Pending steps: a step that started work returns `Pending`, and the runner polls its fence on
//! each tick until the work is done.

use daedalus_abi::FencePoll;

use crate::step::StepMeta;
use crate::tick::TickReport;
use crate::{Pending, Runner};

/// Adds `entry` to the sorted list, or returns false when it is full. The list stays sorted by step
/// index, so the polls of one tick always run in the same order whatever order the steps went
/// pending in.
pub(crate) fn insert_pending(list: &mut [Pending], len: &mut usize, entry: Pending) -> bool {
    if *len == list.len() {
        return false;
    }
    let mut at = *len;
    while at > 0 && list[at - 1].step > entry.step {
        list[at] = list[at - 1];
        at -= 1;
    }
    list[at] = entry;
    *len += 1;
    true
}

/// Whether the list has an entry for `step`.
pub(crate) fn holds(list: &[Pending], step: usize) -> bool {
    list.iter().any(|entry| entry.step as usize == step)
}

impl Runner<'_> {
    /// Polls every registered fence once, in step index order. Ready steps wake their consumers,
    /// and a failed step is counted as a fault. Nothing is woken for a fence that is still
    /// pending, and an entry that finishes is removed while the rest keep their order.
    pub(crate) fn poll_fences(&mut self, report: &mut TickReport) {
        let mut kept = 0;
        for at in 0..self.pending_len {
            let entry = self.pending[at];
            // SAFETY: the step that returned `entry.fence` promised the work stays valid until the
            // fence reports Ready or Failed, and this entry is removed as soon as it does.
            let poll = unsafe { entry.fence.poll() };
            match poll {
                FencePoll::Pending => {
                    self.pending[kept] = entry;
                    kept += 1;
                }
                FencePoll::Ready => {
                    let index = entry.step as usize;
                    let meta: StepMeta = self.meta[index];
                    for &to in crate::spanned(self.table, meta.wake) {
                        self.wake(to);
                    }
                }
                FencePoll::Failed => report.faults += 1,
            }
        }
        self.pending_len = kept;
    }
}
