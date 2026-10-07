//! Rolling window of [`FrameTickSample`]s and its percentile report.

use crate::prelude::*;
use core::fmt::{self, Write as _};

use super::summary::format_compact_duration;
use super::{EdgeAdapterClass, EdgeTickSample, FrameProbe, FrameTickSample, Histogram};

type Field = fn(&FrameTickSample) -> u64;

/// Report rows: name, nested under `tick`, and the sample field.
const STAGES: [(&str, bool, Field); 13] = [
    ("push", false, |s| s.push_ns),
    ("tick", false, |s| s.tick_ns),
    ("inject", true, |s| s.inject_ns),
    ("inputs", true, |s| s.inputs_ns),
    ("adapters_zero_copy", true, |s| s.adapters_zero_copy_ns),
    ("adapters_copying", true, |s| s.adapters_copying_ns),
    ("handlers", true, |s| s.handlers_ns),
    ("node_io", true, |s| s.node_io_ns),
    ("drain", true, |s| s.drain_ns),
    ("dispatch", true, |s| s.dispatch_ns),
    ("take", false, |s| s.take_ns),
    ("graph_overhead", false, |s| s.graph_overhead_ns),
    ("queue_wait", false, |s| s.queue_wait_ns),
];

const COUNTERS: [(&str, Field); 13] = [
    ("nodes", |s| s.nodes),
    ("zero_copy_adapts", |s| s.zero_copy_adapts),
    ("copies", |s| s.copies),
    ("copied_bytes", |s| s.copied_bytes),
    ("shared_clones", |s| s.shared_clones),
    ("gpu_uploads", |s| s.gpu_uploads),
    ("gpu_downloads", |s| s.gpu_downloads),
    ("runtime_allocs", |s| s.runtime_allocs),
    ("node_allocs", |s| s.node_allocs),
    ("host_allocs", |s| s.host_allocs),
    ("runtime_alloc_bytes", |s| s.runtime_alloc_bytes),
    ("node_alloc_bytes", |s| s.node_alloc_bytes),
    ("fused_handoffs", |s| s.fused_handoffs),
];

/// The last `capacity` ticks of a host-driven graph, preallocated so recording never allocates.
#[derive(Clone, Debug)]
pub struct FrameOverheadWindow {
    capacity: usize,
    len: usize,
    next: usize,
    recorded: u64,
    ticks: Vec<FrameTickSample>,
    /// `capacity` rows of one sample per edge.
    edges: Vec<EdgeTickSample>,
    classes: Vec<EdgeAdapterClass>,
}

impl FrameOverheadWindow {
    /// A window of `capacity` ticks (at least one) for the edges of `probe`.
    pub fn new(capacity: usize, probe: &FrameProbe) -> Self {
        let capacity = capacity.max(1);
        let edge_count = probe.edge_count();
        Self {
            capacity,
            len: 0,
            next: 0,
            recorded: 0,
            ticks: vec![FrameTickSample::default(); capacity],
            edges: vec![EdgeTickSample::default(); capacity * edge_count],
            classes: (0..edge_count).map(|idx| probe.edge_class(idx)).collect(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Ticks currently in the window.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Ticks recorded since creation or [`Self::clear`], including those that left the window.
    pub fn recorded(&self) -> u64 {
        self.recorded
    }

    pub fn clear(&mut self) {
        self.len = 0;
        self.next = 0;
        self.recorded = 0;
    }

    /// Record one tick: `fill` writes the cleared sample and per-edge samples of the next slot,
    /// replacing the oldest tick once the window is full.
    pub fn record(&mut self, fill: impl FnOnce(&mut FrameTickSample, &mut [EdgeTickSample])) {
        let slot = self.next;
        let edge_count = self.classes.len();
        let sample = &mut self.ticks[slot];
        *sample = FrameTickSample::default();
        fill(
            sample,
            &mut self.edges[slot * edge_count..(slot + 1) * edge_count],
        );
        self.next = (slot + 1) % self.capacity;
        self.len = (self.len + 1).min(self.capacity);
        self.recorded += 1;
    }

    fn latest_slot(&self) -> Option<usize> {
        (self.len > 0).then(|| (self.next + self.capacity - 1) % self.capacity)
    }

    /// The most recent tick.
    pub fn latest(&self) -> Option<&FrameTickSample> {
        self.latest_slot().map(|slot| &self.ticks[slot])
    }

    pub fn latest_mut(&mut self) -> Option<&mut FrameTickSample> {
        self.latest_slot().map(|slot| &mut self.ticks[slot])
    }

    /// Slots oldest first.
    fn slots(&self) -> impl Iterator<Item = usize> + '_ {
        let start = (self.next + self.capacity - self.len) % self.capacity;
        (0..self.len).map(move |offset| (start + offset) % self.capacity)
    }

    /// Ticks oldest first.
    pub fn samples(&self) -> impl Iterator<Item = &FrameTickSample> + '_ {
        self.slots().map(|slot| &self.ticks[slot])
    }

    /// Percentiles over the window. `edge_labels[i]` names edge `i` (missing labels print the
    /// index).
    pub fn report(&self, edge_labels: &[String]) -> FrameOverheadReport {
        let column = |field: Field| -> Vec<u64> {
            self.slots().map(|slot| field(&self.ticks[slot])).collect()
        };
        let stages = STAGES
            .iter()
            .map(|(name, nested, field)| FrameStat::of(name, *nested, column(*field)))
            .collect();
        let counters = COUNTERS
            .iter()
            .map(|(name, field)| FrameStat::of(name, false, column(*field)))
            .collect();
        let edge_count = self.classes.len();
        let edges = (0..edge_count)
            .filter_map(|edge| {
                let rows: Vec<EdgeTickSample> = self
                    .slots()
                    .map(|slot| self.edges[slot * edge_count + edge])
                    .collect();
                if rows
                    .iter()
                    .all(|row| row.adapts == 0 && row.wait_ns == 0 && row.fused == 0)
                {
                    return None;
                }
                let adapts: u64 = rows.iter().map(|row| row.adapts).sum();
                let fused: u64 = rows.iter().map(|row| row.fused).sum();
                Some(EdgeOverheadStats {
                    edge,
                    label: edge_labels
                        .get(edge)
                        .cloned()
                        .unwrap_or_else(|| format!("edge {edge}")),
                    adapter: self.classes[edge],
                    queue_wait: FrameStat::of(
                        "queue_wait",
                        false,
                        rows.iter().map(|row| row.wait_ns).collect(),
                    ),
                    adapter_time: FrameStat::of(
                        "adapter",
                        false,
                        rows.iter().map(|row| row.adapter_ns).collect(),
                    ),
                    adapts_per_tick: adapts as f64 / rows.len().max(1) as f64,
                    fused_per_tick: fused as f64 / rows.len().max(1) as f64,
                })
            })
            .collect();
        FrameOverheadReport {
            window: self.capacity,
            ticks: self.len,
            recorded: self.recorded,
            alloc_probe: alloc_probe_installed(),
            stages,
            counters,
            edges,
        }
    }
}

fn alloc_probe_installed() -> bool {
    #[cfg(feature = "alloc-probe")]
    {
        crate::alloc_probe::is_installed()
    }
    #[cfg(not(feature = "alloc-probe"))]
    {
        false
    }
}

/// Distribution of one per-tick value over the window (ns for stages, counts for counters).
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FrameStat {
    pub name: String,
    /// Printed under `tick` (a part of the tick's wall time).
    #[serde(default)]
    pub nested: bool,
    pub p50: u64,
    pub p99: u64,
    pub max: u64,
    pub mean: f64,
    /// Log2 buckets of the raw values (ns or counts).
    pub histogram: Histogram,
}

impl FrameStat {
    fn of(name: &str, nested: bool, mut values: Vec<u64>) -> Self {
        let mut histogram = Histogram::default();
        for &value in &values {
            histogram.record_value(value);
        }
        values.sort_unstable();
        // Nearest rank.
        let rank = |q: usize| {
            values
                .get((values.len() * q).div_ceil(100).saturating_sub(1))
                .copied()
                .unwrap_or(0)
        };
        Self {
            name: name.to_string(),
            nested,
            p50: rank(50),
            p99: rank(99),
            max: values.last().copied().unwrap_or(0),
            mean: values.iter().sum::<u64>() as f64 / values.len().max(1) as f64,
            histogram,
        }
    }
}

/// Per-edge queue and adapter time over the window.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EdgeOverheadStats {
    pub edge: usize,
    /// `from_node.port -> to_node.port`.
    pub label: String,
    pub adapter: EdgeAdapterClass,
    /// Per tick, summed over the payloads the edge delivered.
    pub queue_wait: FrameStat,
    /// Per tick adapter path time.
    pub adapter_time: FrameStat,
    pub adapts_per_tick: f64,
    /// Payloads handed straight to the consumer per tick: the edge is fused, so it never queues.
    #[serde(default)]
    pub fused_per_tick: f64,
}

/// Frame-path overhead of a host-driven graph over its recent ticks
/// (`HostGraph::frame_overhead`); see `docs/runtime-diagnostics.md`.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FrameOverheadReport {
    /// Window capacity in ticks.
    pub window: usize,
    /// Ticks in the window.
    pub ticks: usize,
    /// Ticks recorded since overhead recording started.
    pub recorded: u64,
    /// Whether allocation counters are live (`alloc-probe` allocator installed).
    pub alloc_probe: bool,
    /// Time rows in ns (see [`FrameTickSample`] for each stage).
    pub stages: Vec<FrameStat>,
    /// Per-tick counters.
    pub counters: Vec<FrameStat>,
    /// Edges that queued or adapted payloads in the window.
    pub edges: Vec<EdgeOverheadStats>,
}

impl FrameOverheadReport {
    /// A time row by name (`tick`, `graph_overhead`, `handlers`, ...).
    pub fn stage(&self, name: &str) -> Option<&FrameStat> {
        self.stages.iter().find(|stat| stat.name == name)
    }

    /// A counter row by name (`copies`, `runtime_allocs`, ...).
    pub fn counter(&self, name: &str) -> Option<&FrameStat> {
        self.counters.iter().find(|stat| stat.name == name)
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Aligned text table: stages (ns), per-tick counters, then edges.
    pub fn to_table(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "frame overhead: {} ticks in window (capacity {}, {} recorded), alloc probe {}",
            self.ticks,
            self.window,
            self.recorded,
            if self.alloc_probe { "on" } else { "off" }
        );
        let ns = |value: u64| format_compact_duration(core::time::Duration::from_nanos(value));
        let _ = writeln!(
            out,
            "{:<22} {:>10} {:>10} {:>10} {:>10}",
            "stage", "p50", "p99", "max", "mean"
        );
        for stat in &self.stages {
            let name = if stat.nested {
                format!("  {}", stat.name)
            } else {
                stat.name.clone()
            };
            let _ = writeln!(
                out,
                "{name:<22} {:>10} {:>10} {:>10} {:>10}",
                ns(stat.p50),
                ns(stat.p99),
                ns(stat.max),
                ns(stat.mean as u64)
            );
        }
        let _ = writeln!(
            out,
            "{:<22} {:>10} {:>10} {:>10} {:>10}",
            "per tick", "p50", "p99", "max", "mean"
        );
        for stat in self
            .counters
            .iter()
            .filter(|stat| self.alloc_probe || !stat.name.contains("alloc"))
        {
            let _ = writeln!(
                out,
                "{:<22} {:>10} {:>10} {:>10} {:>10.2}",
                stat.name, stat.p50, stat.p99, stat.max, stat.mean
            );
        }
        if !self.edges.is_empty() {
            let _ = writeln!(
                out,
                "{:<40} {:>9} {:>10} {:>10} {:>10} {:>10} {:>7} {:>6}",
                "edge",
                "adapter",
                "wait p50",
                "wait p99",
                "adapt p50",
                "adapt p99",
                "adapts",
                "fused"
            );
            for edge in &self.edges {
                let label = format!("{} {}", edge.edge, edge.label);
                let _ = writeln!(
                    out,
                    "{label:<40} {:>9} {:>10} {:>10} {:>10} {:>10} {:>7.2} {:>6.2}",
                    edge.adapter.as_str(),
                    ns(edge.queue_wait.p50),
                    ns(edge.queue_wait.p99),
                    ns(edge.adapter_time.p50),
                    ns(edge.adapter_time.p99),
                    edge.adapts_per_tick,
                    edge.fused_per_tick
                );
            }
        }
        out
    }
}

impl fmt::Display for FrameOverheadReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_table())
    }
}

#[cfg(test)]
#[path = "frame_tests.rs"]
mod tests;
