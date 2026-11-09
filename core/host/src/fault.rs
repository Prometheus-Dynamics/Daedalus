//! What a failure looks like from the host: the step's nodes, the one at fault when known, and a
//! fixed ring of the most recent reports. A report is plain data, so recording one never allocates.

#![cfg(feature = "std")]

use std::vec::Vec;

use daedalus_abi::REASON_CAPACITY;
use daedalus_runner::StepFault;

use crate::load::LoadedHost;

/// One failure of one step. A merged region is reported as a whole.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FaultReport {
    pub step: u32,
    /// Every node the step runs, from the plan.
    pub covers: &'static [&'static str],
    /// The node the body said failed, if it said.
    pub at: Option<&'static str>,
    /// Failures in a row.
    pub consecutive: u32,
    /// When the backoff ends. Zero when none is running.
    pub until_ns: u64,
    pub tick: u32,
    reason_len: u32,
    reason: [u8; REASON_CAPACITY],
}

impl FaultReport {
    fn new(
        step: u32,
        covers: &'static [&'static str],
        at: Option<&'static str>,
        fault: &StepFault,
    ) -> FaultReport {
        let text = fault.reason().as_bytes();
        let mut reason = [0; REASON_CAPACITY];
        reason[..text.len()].copy_from_slice(text);
        FaultReport {
            step,
            covers,
            at,
            consecutive: fault.consecutive,
            until_ns: fault.until_ns,
            tick: fault.tick,
            reason_len: text.len() as u32,
            reason,
        }
    }

    /// The reason the body gave, cut to 120 bytes.
    pub fn reason(&self) -> &str {
        core::str::from_utf8(&self.reason[..self.reason_len as usize]).unwrap_or("")
    }

    /// The node at fault: the one the body named, or the only one the step runs.
    pub fn node(&self) -> Option<&'static str> {
        match (self.at, self.covers) {
            (Some(node), _) => Some(node),
            (None, [only]) => Some(only),
            _ => None,
        }
    }
}

/// The last `capacity` reports, oldest first.
#[derive(Debug)]
pub(crate) struct FaultRing {
    items: Vec<FaultReport>,
    capacity: usize,
    next: usize,
}

impl FaultRing {
    pub(crate) fn new(capacity: usize) -> FaultRing {
        FaultRing {
            items: Vec::with_capacity(capacity),
            capacity,
            next: 0,
        }
    }

    fn push(&mut self, report: FaultReport) {
        if self.capacity == 0 {
            return;
        }
        if self.items.len() < self.capacity {
            self.items.push(report);
        } else {
            self.items[self.next] = report;
            self.next = (self.next + 1) % self.capacity;
        }
    }

    fn iter(&self) -> impl Iterator<Item = &FaultReport> {
        let (newer, older) = self.items.split_at(self.next);
        older.iter().chain(newer)
    }
}

impl LoadedHost {
    /// Moves new failures from the runner's table into the ring. Runs after every tick.
    pub(crate) fn collect_faults(&mut self) {
        for (step, seen) in self.fault_seen.iter_mut().enumerate() {
            let Some(fault) = self.host.runner.step_fault(step as u32) else {
                continue;
            };
            if fault.seq == *seen {
                continue;
            }
            *seen = fault.seq;
            let at = fault
                .at()
                .and_then(|node| self.node_names.get(node as usize).copied());
            self.faults
                .push(FaultReport::new(step as u32, self.covers[step], at, &fault));
        }
    }

    /// The most recent failures, oldest first. The ring holds [`LoadOptions::fault_reports`].
    ///
    /// [`LoadOptions::fault_reports`]: crate::LoadOptions::fault_reports
    pub fn fault_reports(&mut self) -> impl Iterator<Item = &FaultReport> {
        self.collect_faults();
        self.faults.iter()
    }
}
