//! Domain telemetry: per-graph run counts and times, what sharing saved, and per-graph frame
//! overhead with domain totals.

use crate::prelude::*;
use core::fmt;
use core::time::Duration;

use daedalus_runtime::FrameOverheadReport;
use daedalus_runtime::executor::NodeHandler;

use super::ExecutionDomain;

/// Summary of one [`ExecutionDomain::tick`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DomainTick {
    /// Graphs that ticked.
    pub ran: u32,
    /// Graphs without pending input.
    pub idle: u32,
    /// Graphs whose tick failed (see [`ExecutionDomain::last_error`]).
    pub failed: u32,
    /// Graphs skipped because a graph they are linked from failed or was skipped.
    pub skipped: u32,
    /// Wall time of the domain tick.
    pub duration: Duration,
}

impl DomainTick {
    /// Every graph with input ran without error.
    pub fn is_ok(&self) -> bool {
        self.failed == 0 && self.skipped == 0
    }
}

/// Counters of one graph of a domain.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DomainGraphStats {
    pub name: String,
    /// Nodes in its runtime plan besides the host bridge.
    pub nodes: usize,
    pub runs: u64,
    pub failures: u64,
    pub skipped: u64,
    /// Time in its ticks.
    pub run_time: Duration,
    pub last_run: Duration,
    pub max_run: Duration,
    /// Distinct graphs its outputs are linked to.
    pub consumers: usize,
    /// Runs it saved: with `consumers` downstream graphs each computing it themselves it would
    /// have run `consumers` times per frame; one shared run saves `consumers - 1`.
    pub avoided_runs: u64,
    /// `avoided_runs` times its node count.
    pub avoided_node_runs: u64,
    /// Time those avoided runs would have taken (each at the shared run's measured time).
    pub saved_time: Duration,
    /// Values a linked downstream input refused (type mismatch).
    pub forward_rejected: u64,
}

impl DomainGraphStats {
    pub(super) fn record_run(&mut self, elapsed: Duration) {
        self.runs += 1;
        self.run_time += elapsed;
        self.last_run = elapsed;
        self.max_run = self.max_run.max(elapsed);
        let avoided = self.consumers.saturating_sub(1) as u64;
        self.avoided_runs += avoided;
        self.avoided_node_runs += avoided * self.nodes as u64;
        self.saved_time += elapsed * avoided as u32;
    }

    /// Mean time per run.
    pub fn mean_run(&self) -> Duration {
        if self.runs == 0 {
            Duration::ZERO
        } else {
            self.run_time / self.runs as u32
        }
    }
}

/// Counters of a domain ([`ExecutionDomain::stats`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DomainStats {
    pub ticks: u64,
    /// Wall time of all domain ticks.
    pub wall_time: Duration,
    /// Time inside graph ticks (the rest of `wall_time` is forwarding and bookkeeping).
    pub graph_time: Duration,
    /// Domain totals of the graphs' [`DomainGraphStats::avoided_runs`],
    /// [`DomainGraphStats::avoided_node_runs`] and [`DomainGraphStats::saved_time`].
    pub avoided_runs: u64,
    pub avoided_node_runs: u64,
    pub saved_time: Duration,
    /// Per graph, in tick order.
    pub graphs: Vec<DomainGraphStats>,
}

impl DomainStats {
    /// `wall_time - graph_time`: taking, forwarding and the domain's own bookkeeping.
    pub fn forward_time(&self) -> Duration {
        self.wall_time.saturating_sub(self.graph_time)
    }

    pub fn graph(&self, name: &str) -> Option<&DomainGraphStats> {
        self.graphs.iter().find(|graph| graph.name == name)
    }
}

impl fmt::Display for DomainStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "domain: {} ticks, wall {:?}, graphs {:?}, forwarding {:?}; sharing avoided {} graph runs ({} node runs, ~{:?})",
            self.ticks,
            self.wall_time,
            self.graph_time,
            self.forward_time(),
            self.avoided_runs,
            self.avoided_node_runs,
            self.saved_time
        )?;
        for graph in &self.graphs {
            writeln!(
                f,
                "  {}: {} runs ({} failed, {} skipped), mean {:?}, max {:?}, {} consumers, avoided {} runs (~{:?})",
                graph.name,
                graph.runs,
                graph.failures,
                graph.skipped,
                graph.mean_run(),
                graph.max_run,
                graph.consumers,
                graph.avoided_runs,
                graph.saved_time
            )?;
        }
        Ok(())
    }
}

/// Frame-path overhead of each graph of a domain plus domain totals
/// ([`ExecutionDomain::frame_overhead`]).
#[derive(Clone, Debug)]
pub struct DomainOverhead {
    /// Per graph with recording on, in tick order.
    pub graphs: Vec<(String, FrameOverheadReport)>,
    /// Sum over the graphs of the mean `tick` row (ns): the graphs' share of a domain tick.
    pub tick_mean_ns: f64,
    /// Sum over the graphs of the mean `graph_overhead` row (ns): runtime cost around handlers.
    pub graph_overhead_mean_ns: f64,
    /// The domain's counters, sharing savings included.
    pub stats: DomainStats,
}

impl DomainOverhead {
    pub fn graph(&self, name: &str) -> Option<&FrameOverheadReport> {
        self.graphs
            .iter()
            .find(|(graph, _)| graph == name)
            .map(|(_, report)| report)
    }
}

impl fmt::Display for DomainOverhead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (name, report) in &self.graphs {
            writeln!(f, "== {name}")?;
            write!(f, "{report}")?;
        }
        writeln!(
            f,
            "== domain totals: tick mean {:.0} ns, graph_overhead mean {:.0} ns",
            self.tick_mean_ns, self.graph_overhead_mean_ns
        )?;
        write!(f, "{}", self.stats)
    }
}

impl<H: NodeHandler + Send + Sync + 'static> ExecutionDomain<H> {
    /// Counters per graph (tick order) and domain totals.
    pub fn stats(&self) -> DomainStats {
        let graphs: Vec<DomainGraphStats> = self
            .order
            .iter()
            .map(|&index| self.members[index].stats.clone())
            .collect();
        let sum = |f: fn(&DomainGraphStats) -> u64| graphs.iter().map(f).sum();
        DomainStats {
            ticks: self.ticks,
            wall_time: self.wall_time,
            graph_time: graphs.iter().map(|graph| graph.run_time).sum(),
            avoided_runs: sum(|graph| graph.avoided_runs),
            avoided_node_runs: sum(|graph| graph.avoided_node_runs),
            saved_time: graphs.iter().map(|graph| graph.saved_time).sum(),
            graphs,
        }
    }

    /// Reset the counters (e.g. after warm-up); graph errors are kept.
    pub fn reset_stats(&mut self) {
        self.ticks = 0;
        self.wall_time = Duration::ZERO;
        for member in &mut self.members {
            member.stats = DomainGraphStats {
                name: core::mem::take(&mut member.stats.name),
                nodes: member.stats.nodes,
                consumers: member.stats.consumers,
                ..DomainGraphStats::default()
            };
        }
    }

    /// Record frame-path overhead on every graph, present and added later
    /// (`HostGraph::enable_frame_overhead`).
    pub fn enable_frame_overhead(&mut self, window: usize) {
        self.overhead_window = Some(window);
        for member in &mut self.members {
            member.graph.enable_frame_overhead(window);
        }
    }

    /// Start every graph's overhead window over and reset the counters.
    pub fn reset_frame_overhead(&mut self) {
        for member in &mut self.members {
            member.graph.reset_frame_overhead();
        }
        self.reset_stats();
    }

    /// Each recording graph's frame overhead plus domain totals; `None` when no graph records.
    pub fn frame_overhead(&self) -> Option<DomainOverhead> {
        let graphs: Vec<(String, FrameOverheadReport)> = self
            .order
            .iter()
            .filter_map(|&index| {
                let member = &self.members[index];
                Some((member.name.clone(), member.graph.frame_overhead()?))
            })
            .collect();
        if graphs.is_empty() {
            return None;
        }
        let mean = |row: &str| -> f64 {
            graphs
                .iter()
                .filter_map(|(_, report)| report.stage(row))
                .map(|stat| stat.mean)
                .sum()
        };
        Some(DomainOverhead {
            tick_mean_ns: mean("tick"),
            graph_overhead_mean_ns: mean("graph_overhead"),
            stats: self.stats(),
            graphs,
        })
    }
}
