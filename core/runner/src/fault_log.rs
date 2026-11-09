//! The last failure of each step, kept in a table the caller sized at load. Writing one copies a
//! bounded reason and never allocates, and a step's entry belongs to the worker running that step.

use core::ptr;

use daedalus_abi::{NO_NODE, REASON_CAPACITY, StepCx};

use crate::Runner;
use crate::error::LoadError;
use crate::failure::StepHealth;

/// One step's most recent failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StepFault {
    /// Counts this step's failures. A new number means a new failure.
    pub seq: u32,
    pub tick: u32,
    at: u32,
    /// Failures in a row, including this one.
    pub consecutive: u32,
    /// When a backoff ends, on the host's clock. Zero when none is running.
    pub until_ns: u64,
    reason_len: u32,
    reason: [u8; REASON_CAPACITY],
}

impl StepFault {
    pub const EMPTY: StepFault = StepFault {
        seq: 0,
        tick: 0,
        at: NO_NODE,
        consecutive: 0,
        until_ns: 0,
        reason_len: 0,
        reason: [0; REASON_CAPACITY],
    };

    /// The node the body named, if it named one.
    pub fn at(&self) -> Option<u32> {
        (self.at != NO_NODE).then_some(self.at)
    }

    /// The reason the body gave, at most [`REASON_CAPACITY`] bytes.
    pub fn reason(&self) -> &str {
        let len = (self.reason_len as usize).min(REASON_CAPACITY);
        core::str::from_utf8(&self.reason[..len]).unwrap_or("")
    }
}

/// The table, as a pointer so lanes can share it.
#[derive(Clone, Copy)]
pub(crate) struct FaultLog {
    cells: *mut StepFault,
    len: usize,
}

impl FaultLog {
    pub(crate) const NONE: FaultLog = FaultLog {
        cells: ptr::null_mut(),
        len: 0,
    };

    /// Records what the body just reported on `cx`.
    pub(crate) fn record(self, index: usize, tick: u32, cx: &StepCx, health: &StepHealth) {
        if index >= self.len {
            return;
        }
        let text = cx.reason().as_bytes();
        // SAFETY: `index` is in bounds, the table outlives the runner by the borrow taken in
        // `set_fault_log`, and only the one worker running this step touches its entry.
        let cell = unsafe { &mut *self.cells.add(index) };
        cell.seq = cell.seq.wrapping_add(1);
        cell.tick = tick;
        cell.at = cx.at;
        cell.consecutive = health.failures();
        cell.until_ns = if health.backing_off() {
            health.until_ns()
        } else {
            0
        };
        cell.reason_len = text.len() as u32;
        cell.reason[..text.len()].copy_from_slice(text);
    }
}

impl<'a> Runner<'a> {
    /// Attaches the fault table: one entry per step. Without it failures are only counted.
    pub fn set_fault_log(&mut self, cells: &'a mut [StepFault]) -> Result<(), LoadError> {
        if cells.len() < self.steps.len() {
            return Err(LoadError::Length);
        }
        self.fault_log = FaultLog {
            len: cells.len(),
            cells: cells.as_mut_ptr(),
        };
        Ok(())
    }

    /// The last failure of `step`, when the table is attached and it has failed.
    pub fn step_fault(&self, step: u32) -> Option<StepFault> {
        if step as usize >= self.fault_log.len {
            return None;
        }
        // SAFETY: in bounds, and `&self` means no tick is writing the table.
        let cell = unsafe { *self.fault_log.cells.add(step as usize) };
        (cell.seq != 0).then_some(cell)
    }
}
