//! Node progress: one `(done, total)` pair of cells per step, sized at load. A body writes them
//! with relaxed atomics through `cx.progress`, and writing never allocates. The host reads them.

use core::ptr;

use daedalus_abi::Progress;

use crate::step::SlotStamp;
use crate::{LoadError, Runner};

/// The cells of step `index`, or null when no cells were attached.
#[inline]
pub(crate) fn cell(cells: &[Progress], index: usize) -> *const Progress {
    cells.get(index).map_or(ptr::null(), |cell| cell)
}

/// The cells of step `index`, or none.
#[inline]
pub(crate) fn metric_cells<'a>(
    cells: &'a [daedalus_abi::MetricCell],
    at: &[u32],
    index: usize,
) -> &'a [daedalus_abi::MetricCell] {
    match (at.get(index), at.get(index + 1)) {
        (Some(&start), Some(&end)) => cells.get(start as usize..end as usize).unwrap_or(&[]),
        _ => &[],
    }
}

impl<'a> Runner<'a> {
    /// Attaches what each slot's values are: one per slot. Without them an output keeps the
    /// numbers its step wrote.
    pub fn set_stamps(&mut self, stamps: &'a [SlotStamp]) -> Result<(), LoadError> {
        if stamps.len() < self.slots {
            return Err(LoadError::Length);
        }
        self.stamps = stamps;
        Ok(())
    }

    /// Attaches the metric cells: step `i` has `at[i]..at[i + 1]` of `cells`, one for each metric
    /// of each node it runs, in the order it runs them. Without them a body's metrics go nowhere.
    pub fn set_metrics(
        &mut self,
        cells: &'a [daedalus_abi::MetricCell],
        at: &'a [u32],
    ) -> Result<(), LoadError> {
        let fits = at.len() == self.steps.len() + 1
            && at.windows(2).all(|pair| pair[0] <= pair[1])
            && at.last().is_some_and(|end| *end as usize <= cells.len());
        if !fits {
            return Err(LoadError::Length);
        }
        self.metrics = cells;
        self.metric_at = at;
        Ok(())
    }

    /// The metric cells of `step`. Empty when it has none, or none were attached.
    pub fn metric_cells(&self, step: u32) -> &'a [daedalus_abi::MetricCell] {
        metric_cells(self.metrics, self.metric_at, step as usize)
    }

    /// Attaches the progress cells: one per step. Without them `cx.progress` writes nothing.
    pub fn set_progress(&mut self, cells: &'a [Progress]) -> Result<(), LoadError> {
        if cells.len() < self.steps.len() {
            return Err(LoadError::Length);
        }
        self.progress = cells;
        Ok(())
    }

    /// The `(done, total)` the body of `step` last reported, or `None` when no cells are attached
    /// or past the end.
    pub fn progress(&self, step: u32) -> Option<(u32, u32)> {
        let cell = self.progress.get(step as usize)?;
        Some((cell.done.get(), cell.total.get()))
    }
}
