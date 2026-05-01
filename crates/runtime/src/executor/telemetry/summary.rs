use std::fmt::Write as _;
use std::time::Duration;

use super::ExecutionTelemetry;

impl ExecutionTelemetry {
    pub fn compact_snapshot(&self) -> String {
        let node_time = self
            .node_metrics
            .values()
            .fold(Duration::ZERO, |total, metrics| {
                total.saturating_add(metrics.total_duration)
            });
        let handler_time = self
            .node_metrics
            .values()
            .fold(Duration::ZERO, |total, metrics| {
                total.saturating_add(metrics.handler_duration)
            });
        let edge_wait = self
            .edge_metrics
            .values()
            .fold(Duration::ZERO, |total, metrics| {
                total.saturating_add(metrics.total_wait)
            });
        let adapter_time = self
            .edge_metrics
            .values()
            .fold(Duration::ZERO, |total, metrics| {
                total.saturating_add(metrics.adapter_duration)
            });
        let transport_time = self
            .edge_metrics
            .values()
            .fold(Duration::ZERO, |total, metrics| {
                total.saturating_add(metrics.transport_apply_duration)
            });
        let copied_bytes: u64 = self
            .edge_metrics
            .values()
            .map(|metrics| metrics.copied_bytes)
            .sum();
        let payload_clones: u64 = self
            .edge_metrics
            .values()
            .map(|metrics| metrics.payload_clone_count)
            .sum();
        let drops: u64 = self
            .edge_metrics
            .values()
            .map(|metrics| metrics.drops)
            .sum();
        let node_overhead = node_time
            .saturating_sub(handler_time)
            .saturating_sub(transport_time)
            .saturating_sub(adapter_time);
        let graph_overhead = self
            .graph_duration
            .saturating_sub(handler_time)
            .saturating_sub(transport_time)
            .saturating_sub(adapter_time);
        let mut out = String::new();
        let _ = writeln!(out, "metrics:");
        let _ = writeln!(
            out,
            "  level={:?} graph={} nodes={} node_time={} handler={} node_overhead={} graph_overhead={}",
            self.metrics_level,
            format_compact_duration(self.graph_duration),
            self.nodes_executed,
            format_compact_duration(node_time),
            format_compact_duration(handler_time),
            format_compact_duration(node_overhead),
            format_compact_duration(graph_overhead)
        );
        let _ = writeln!(
            out,
            "  edge_wait={} transport={} adapters={} backpressure={} drops={}",
            format_compact_duration(edge_wait),
            format_compact_duration(transport_time),
            format_compact_duration(adapter_time),
            self.backpressure_events,
            drops
        );
        let _ = write!(
            out,
            "  payload_clones={} copied_bytes={} errors={} warnings={}",
            payload_clones,
            copied_bytes,
            self.errors.len(),
            self.warnings.len()
        );
        if !self.demand.is_empty() {
            let _ = write!(
                out,
                "\n  demand selected={:?} active_nodes={} skipped_nodes={} active_edges={} skipped_edges={} avoided_clones={} avoided_adapters={} planned_inactive_adapters={:?} avoided_bytes={}",
                self.demand.selected_sinks,
                self.demand.active_nodes,
                self.demand.skipped_nodes,
                self.demand.active_edges,
                self.demand.skipped_edges,
                self.demand.avoided_clones,
                self.demand.avoided_adapter_calls,
                self.demand.planned_inactive_adapter_edges,
                self.demand.avoided_transport_bytes
            );
        }
        out
    }
}

fn format_compact_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos < 1_000 {
        format!("{nanos}ns")
    } else if nanos < 1_000_000 {
        format!("{:.2}us", nanos as f64 / 1_000.0)
    } else {
        format!("{:.2}ms", nanos as f64 / 1_000_000.0)
    }
}
