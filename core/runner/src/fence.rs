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
                FencePoll::Failed => {
                    report.faults += 1;
                    self.take_back(entry);
                }
            }
        }
        self.pending_len = kept;
    }

    /// Removes what a failed step published, so no reader ever sees its work. The step's newest
    /// value in each output it stored is the one it put there: nothing else writes that slot while
    /// the step is pending.
    fn take_back(&mut self, entry: Pending) {
        let meta = self.meta[entry.step as usize];
        for (at, &slot) in crate::spanned(self.table, meta.outputs).iter().enumerate() {
            if slot == daedalus_abi::NO_SLOT || at >= 32 || entry.kept & (1 << at) == 0 {
                continue;
            }
            let Some(queue) = self.queues.get_mut(slot as usize) else {
                continue;
            };
            if let Some(index) = queue.take_tail() {
                // SAFETY: the queue owned this entry, and `take_tail` removed it from the queue, so
                // its reference is released exactly once here.
                unsafe { crate::owned::release(&self.storage[index]) };
            }
        }
    }
}
