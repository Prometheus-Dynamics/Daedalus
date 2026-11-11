//! What a domain's ticks did and what sharing saved.

#![cfg(feature = "std")]

use std::string::String;
use std::vec::Vec;

use crate::TickOverheadReport;

use super::Domain;

/// One domain tick.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DomainTick {
    /// Graphs that ticked.
    pub ran: u32,
    /// Graphs with nothing to do.
    pub idle: u32,
    /// Graphs whose tick returned an error.
    pub failed: u32,
    /// Values a linked input wouldn't take.
    pub rejected: u32,
    pub duration_ns: u64,
}

impl DomainTick {
    pub fn is_ok(&self) -> bool {
        self.failed == 0 && self.rejected == 0
    }
}

/// One graph's totals.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DomainGraphStats {
    pub name: String,
    pub nodes: usize,
    pub runs: u64,
    pub failures: u64,
    pub run_ns: u64,
    pub last_run_ns: u64,
    pub max_run_ns: u64,
    /// Graphs its outputs are linked to, at the last run.
    pub consumers: usize,
    /// Runs that separate graphs would have needed and this one saved.
    pub avoided_runs: u64,
    pub avoided_node_runs: u64,
    pub saved_ns: u64,
    /// The probe's overhead over its window, when the probe is on.
    pub overhead: Option<TickOverheadReport>,
}

impl DomainGraphStats {
    pub(super) fn named(name: &str, nodes: usize) -> DomainGraphStats {
        DomainGraphStats {
            name: String::from(name),
            nodes,
            ..DomainGraphStats::default()
        }
    }

    pub(super) fn record(&mut self, nanos: u64, consumers: usize, ok: bool) {
        self.runs += 1;
        self.failures += u64::from(!ok);
        self.run_ns += nanos;
        self.last_run_ns = nanos;
        self.max_run_ns = self.max_run_ns.max(nanos);
        self.consumers = consumers;
        let avoided = consumers.saturating_sub(1) as u64;
        self.avoided_runs += avoided;
        self.avoided_node_runs += avoided * self.nodes as u64;
        self.saved_ns += avoided * nanos;
    }
}

/// The domain's totals, and each graph's.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DomainStats {
    pub ticks: u64,
    pub tick_ns: u64,
    /// Time inside graph ticks.
    pub step_ns: u64,
    pub forward_rejected: u64,
    pub avoided_runs: u64,
    pub graphs: Vec<DomainGraphStats>,
}

impl DomainStats {
    pub(super) fn record(&mut self, total: u64, steps: u64) {
        self.ticks += 1;
        self.tick_ns += total;
        self.step_ns += steps;
    }

    /// Domain time outside graph ticks: ordering and forwarding.
    pub fn overhead_ns(&self) -> u64 {
        self.tick_ns.saturating_sub(self.step_ns)
    }
}

impl Domain {
    /// The totals so far.
    pub fn stats(&self) -> DomainStats {
        let mut stats = self.stats.clone();
        stats.graphs = self
            .order
            .iter()
            .map(|&at| {
                let graph = &self.graphs[at];
                let mut row = graph.stats.clone();
                row.overhead = graph.host.tick_overhead();
                row
            })
            .collect();
        stats.avoided_runs = stats.graphs.iter().map(|row| row.avoided_runs).sum();
        stats
    }
}
