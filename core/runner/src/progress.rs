//! Node progress: one `(done, total)` pair of cells per step, sized at load. A body writes them
//! with relaxed atomics through `cx.progress`, and writing never allocates. The host reads them.

use core::ptr;

use daedalus_abi::Progress;

use crate::{LoadError, Runner};

/// The cells of step `index`, or null when no cells were attached.
#[inline]
pub(crate) fn cell(cells: &[Progress], index: usize) -> *const Progress {
    cells.get(index).map_or(ptr::null(), |cell| cell)
}

impl<'a> Runner<'a> {
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
