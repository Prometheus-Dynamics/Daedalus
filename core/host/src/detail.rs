//! The longer account of a failure: what a step gave [`StepCx::detail`] before it faulted. A
//! fault's reason is 120 bytes in a fixed place. The detail is the host's own memory, bounded
//! per step, and only a step that has something to say pays for it.
//!
//! [`StepCx::detail`]: daedalus_abi::StepCx::detail

#![cfg(feature = "std")]

use std::string::String;
use std::sync::Arc;
use std::vec::Vec;

use daedalus_runner::DetailStore;

use crate::load::LoadedHost;

/// The detail of one step's most recent failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FaultDetail<'a> {
    /// What the step wrote, in the order it wrote it.
    pub text: &'a str,
    /// The step wrote more than [`LoadOptions::fault_detail_bytes`], and `text` is only its
    /// start.
    ///
    /// [`LoadOptions::fault_detail_bytes`]: crate::LoadOptions::fault_detail_bytes
    pub cut: bool,
    /// The tick the step failed in.
    pub tick: u32,
}

struct Kept {
    text: String,
    cut: bool,
    tick: u32,
}

/// The details of a loaded graph: where its steps write, and what each step's last failure left.
#[derive(Default)]
pub(crate) struct Details {
    store: Option<Arc<DetailStore>>,
    kept: Vec<Option<Kept>>,
}

impl LoadedHost {
    /// Gives the running graph somewhere to write its details, sized for its steps. Runs when a
    /// graph loads and when one is loaded over it: a new graph has new steps.
    pub(crate) fn attach_detail(&mut self) {
        let steps = self.covers.len();
        let cap = self.options.fault_detail_bytes;
        self.detail.kept.clear();
        self.detail.kept.resize_with(steps, || None);
        self.detail.store = (cap > 0).then(|| DetailStore::new(steps, cap));
        self.host.runner.keep_details(self.detail.store.clone());
    }

    /// Whether any step has written a detail since this was last asked: after a tick, whether
    /// any did in that tick.
    pub(crate) fn details_written(&self) -> bool {
        self.detail
            .store
            .as_ref()
            .is_some_and(|store| store.written())
    }

    /// Settles the detail of `step`, which failed in `tick`: what it wrote is kept as that
    /// failure's, in place of the last one's. Says whether there is one.
    pub(crate) fn keep_detail(&mut self, step: usize, tick: u32, written: bool) -> bool {
        let Some(kept) = self.detail.kept.get_mut(step) else {
            return false;
        };
        let taken = match &self.detail.store {
            Some(store) if written => store.take(step),
            _ => None,
        };
        *kept = taken.map(|(text, cut)| Kept { text, cut, tick });
        kept.is_some()
    }

    /// Drops what the steps that didn't fail wrote.
    pub(crate) fn drop_details(&self) {
        if let Some(store) = &self.detail.store {
            store.clear();
        }
    }

    /// The detail of the most recent failure of `step`: what its body gave `detail` before it
    /// faulted. `None` when the step hasn't failed, or wrote nothing the last time it did. It is
    /// kept until the step fails again. [`FaultReport::detail`] says whether a report has one.
    ///
    /// [`FaultReport::detail`]: crate::FaultReport::detail
    pub fn fault_detail(&mut self, step: u32) -> Option<FaultDetail<'_>> {
        self.collect_faults();
        let kept = self.detail.kept.get(step as usize)?.as_ref()?;
        Some(FaultDetail {
            text: &kept.text,
            cut: kept.cut,
            tick: kept.tick,
        })
    }
}
