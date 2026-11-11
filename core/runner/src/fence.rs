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
                    self.show(entry);
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

    /// The queues of the outputs `entry`'s step stored, by index.
    fn stored(&self, entry: Pending) -> impl Iterator<Item = usize> + use<> {
        let meta = self.meta[entry.step as usize];
        let outputs = crate::spanned(self.table, meta.outputs);
        let slots = self.slots;
        let mut found = [usize::MAX; 32];
        for (at, &slot) in outputs.iter().enumerate().take(32) {
            if slot != daedalus_abi::NO_SLOT && entry.kept & (1 << at) != 0 {
                found[at] = slot as usize;
            }
        }
        found.into_iter().filter(move |slot| *slot < slots)
    }

    /// Shows readers what a finished step stored while it was working.
    fn show(&mut self, entry: Pending) {
        let now = self.cx.now_ns;
        for slot in self.stored(entry) {
            self.queues[slot].show_hidden(self.storage, now);
        }
    }

    /// Releases what a failed step stored. No reader ever saw it.
    fn take_back(&mut self, entry: Pending) {
        for slot in self.stored(entry) {
            if let Some(index) = self.queues[slot].take_hidden() {
                // SAFETY: the queue owned this entry, and `take_hidden` forgot it, so its
                // reference is released exactly once here.
                unsafe { crate::owned::release(&self.storage[index]) };
            }
        }
    }
}
