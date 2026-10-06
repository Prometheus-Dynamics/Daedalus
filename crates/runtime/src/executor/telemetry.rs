use crate::prelude::*;
use alloc::collections::BTreeMap;
use core::time::Duration;
use daedalus_core::platform::{Clock, Instant};

use daedalus_planner::GroupMetadata;

mod basics;
mod frame;
mod frame_report;
mod lifecycle;
mod metrics;
mod node_map;
mod record_edge;
mod record_node;
mod report;
mod resources;
mod summary;

pub use basics::{Histogram, MetricsLevel, ProfileLevel, Profiler};
pub use frame::{EdgeAdapterClass, EdgeTickSample, FrameProbe, FrameTickSample};
pub(crate) use frame::{ProbeCount, ProbeTime};
pub use frame_report::{EdgeOverheadStats, FrameOverheadReport, FrameOverheadWindow, FrameStat};
pub use lifecycle::{
    DataLifecycleEvent, DataLifecycleRecord, DataLifecycleStage, NodeFailure, TraceEvent,
};
pub use metrics::{
    CustomMetricValue, EdgeMetrics, EdgePressureMetrics, EdgePressureReason, FfiAdapterTelemetry,
    FfiBackendTelemetry, FfiPackageTelemetry, FfiPayloadTelemetry, FfiTelemetryReport,
    FfiWorkerTelemetry, NodeMetrics, TransportMetrics,
};
pub use node_map::NodeMetricsMap;
pub use report::{AdapterPathReport, OwnershipReport, TelemetryReport, TelemetryReportFilter};
pub use resources::{
    InternalTransferMetrics, NodeAllocationSpikeExplanation, NodePerfMetrics, NodeResourceMetrics,
    ResourceMetrics,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct InFlightNodeTransportMetrics {
    input_bytes: u64,
    output_bytes: u64,
}

/// Aggregated timing + diagnostics for a run.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExecutionTelemetry {
    pub nodes_executed: usize,
    pub cpu_segments: usize,
    pub gpu_segments: usize,
    pub gpu_fallbacks: usize,
    pub backpressure_events: usize,
    pub warnings: smallvec::SmallVec<[String; 8]>,
    #[serde(default, skip_serializing_if = "smallvec::SmallVec::is_empty")]
    pub errors: smallvec::SmallVec<[NodeFailure; 4]>,
    pub graph_duration: Duration,
    #[serde(default)]
    pub unattributed_runtime_duration: Duration,
    #[serde(default)]
    pub metrics_level: MetricsLevel,
    /// Per-node-instance metrics keyed by the planned node index (`NodeRef.0`).
    pub node_metrics: NodeMetricsMap,
    /// Per-group aggregate metrics keyed by group id (e.g. embedded graphs).
    pub group_metrics: BTreeMap<String, NodeMetrics>,
    /// Per-edge queue wait metrics keyed by the planned edge index.
    pub edge_metrics: BTreeMap<usize, EdgeMetrics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<Vec<TraceEvent>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub data_lifecycle: Vec<DataLifecycleEvent>,
    #[serde(
        default,
        skip_serializing_if = "crate::plan::DemandTelemetry::is_empty"
    )]
    pub demand: crate::plan::DemandTelemetry,
    #[serde(default, skip_serializing_if = "FfiTelemetryReport::is_empty")]
    pub ffi: FfiTelemetryReport,
    /// The executor's clock, which lifecycle timestamps and edge timings read.
    #[serde(skip)]
    clock: Clock,
    #[serde(skip)]
    lifecycle_origin: Option<Instant>,
    #[serde(skip)]
    in_flight_node_transport_metrics: BTreeMap<usize, InFlightNodeTransportMetrics>,
}

impl ExecutionTelemetry {
    pub fn with_level(level: MetricsLevel) -> Self {
        if !cfg!(feature = "metrics") {
            return Self {
                metrics_level: MetricsLevel::Off,
                ..Default::default()
            };
        }
        Self {
            metrics_level: level,
            lifecycle_origin: (level.is_profile() || level.is_trace()).then(Instant::now),
            ..Default::default()
        }
    }

    /// Read timestamps from `clock` (the executor's clock).
    pub(crate) fn with_clock(mut self, clock: &Clock) -> Self {
        if self.lifecycle_origin.is_some() {
            self.lifecycle_origin = Some(clock.now());
        }
        self.clock = clock.clone();
        self
    }

    /// The clock timings are read from.
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    pub fn is_lifecycle_enabled(&self) -> bool {
        cfg!(feature = "metrics")
            && (self.metrics_level.is_profile() || self.metrics_level.is_trace())
    }

    pub fn report(&self) -> TelemetryReport {
        let ownership = self
            .edge_metrics
            .iter()
            .map(|(idx, edge)| {
                (
                    *idx,
                    OwnershipReport {
                        unique_handoffs: edge.unique_handoffs,
                        shared_handoffs: edge.shared_handoffs,
                        payload_clones: edge.payload_clone_count,
                        copied_bytes: edge.copied_bytes,
                    },
                )
            })
            .collect();
        let hardware_counters = self
            .node_metrics
            .iter()
            .filter_map(|(idx, node)| node.perf.clone().map(|perf| (idx, perf)))
            .collect();
        let adapter_paths = self
            .data_lifecycle
            .iter()
            .filter(|event| !event.adapter_steps.is_empty())
            .map(|event| AdapterPathReport {
                edge: event.edge_idx,
                node: event.node_idx,
                port: event.port.clone(),
                correlation_id: event.correlation_id,
                steps: event.adapter_steps.clone(),
                detail: event.detail.clone(),
            })
            .collect();
        let skipped_nodes = self
            .node_metrics
            .iter()
            .filter_map(|(idx, metrics)| (metrics.calls == 0).then_some(idx))
            .collect();
        let fallbacks = (0..self.gpu_fallbacks)
            .map(|idx| format!("gpu_fallback_{idx}"))
            .collect();
        TelemetryReport {
            metrics_level: self.metrics_level,
            graph_duration: self.graph_duration,
            unattributed_runtime_duration: self.unattributed_runtime_duration,
            nodes_executed: self.nodes_executed,
            gpu_segments: self.gpu_segments,
            gpu_fallbacks: self.gpu_fallbacks,
            backpressure_events: self.backpressure_events,
            node_timing: self.node_metrics.to_btree_map(),
            edge_timing: self.edge_metrics.clone(),
            transport: self.edge_metrics.clone(),
            ownership,
            adapter_paths,
            capability_sources: Vec::new(),
            lifecycle: self.data_lifecycle.clone(),
            warnings: self.warnings.iter().cloned().collect(),
            errors: self.errors.iter().cloned().collect(),
            fallbacks,
            skipped_nodes,
            hardware_counters,
            ffi: self.ffi.clone(),
        }
    }

    pub fn reset_for_reuse(&mut self, level: MetricsLevel) {
        self.nodes_executed = 0;
        self.cpu_segments = 0;
        self.gpu_segments = 0;
        self.gpu_fallbacks = 0;
        self.backpressure_events = 0;
        self.warnings.clear();
        self.errors.clear();
        self.graph_duration = Duration::default();
        self.unattributed_runtime_duration = Duration::default();
        self.metrics_level = if cfg!(feature = "metrics") {
            level
        } else {
            MetricsLevel::Off
        };
        self.lifecycle_origin = (self.metrics_level.is_profile() || self.metrics_level.is_trace())
            .then(|| self.clock.now());
        self.node_metrics.clear();
        self.group_metrics.clear();
        self.edge_metrics.clear();
        if let Some(trace) = self.trace.as_mut() {
            trace.clear();
        }
        self.data_lifecycle.clear();
        self.demand = crate::plan::DemandTelemetry::default();
        self.ffi = FfiTelemetryReport::default();
        self.in_flight_node_transport_metrics.clear();
    }

    pub fn merge(&mut self, other: ExecutionTelemetry) {
        self.nodes_executed += other.nodes_executed;
        self.cpu_segments += other.cpu_segments;
        self.gpu_segments += other.gpu_segments;
        self.gpu_fallbacks += other.gpu_fallbacks;
        self.backpressure_events += other.backpressure_events;
        self.warnings.extend(other.warnings);
        self.errors.extend(other.errors);
        self.graph_duration = self.graph_duration.max(other.graph_duration);
        self.unattributed_runtime_duration = self
            .unattributed_runtime_duration
            .saturating_add(other.unattributed_runtime_duration);
        self.metrics_level = self.metrics_level.max(other.metrics_level);
        for (node, metrics) in other.node_metrics {
            self.node_metrics.entry(node).merge(metrics);
        }
        for (group, metrics) in other.group_metrics {
            self.group_metrics.entry(group).or_default().merge(metrics);
        }
        for (edge, metrics) in other.edge_metrics {
            self.edge_metrics.entry(edge).or_default().merge(metrics);
        }
        if let Some(other_trace) = other.trace {
            let trace = self.trace.get_or_insert_with(Vec::new);
            trace.extend(other_trace);
        }
        self.data_lifecycle.extend(other.data_lifecycle);
        self.ffi.merge(other.ffi);
    }

    pub fn record_ffi(&mut self, ffi: FfiTelemetryReport) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_basic() {
            return;
        }
        self.ffi.merge(ffi);
    }

    pub fn record_trace_event(&mut self, node_idx: usize, start: Duration, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_profile() && !self.metrics_level.is_trace() {
            return;
        }
        let trace = self.trace.get_or_insert_with(Vec::new);
        trace.push(TraceEvent {
            node_idx,
            start_ns: start.as_nanos() as u64,
            duration_ns: duration.as_nanos() as u64,
        });
    }

    pub fn recompute_unattributed_runtime_duration(&mut self) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            self.unattributed_runtime_duration = Duration::default();
            return;
        }
        let node_duration = self
            .node_metrics
            .values()
            .fold(Duration::default(), |total, metrics| {
                total.saturating_add(metrics.total_duration)
            });
        let edge_transport_apply_duration = self
            .edge_metrics
            .values()
            .fold(Duration::default(), |total, metrics| {
                total.saturating_add(metrics.transport_apply_duration)
            });
        let accounted = node_duration.saturating_add(edge_transport_apply_duration);
        self.unattributed_runtime_duration = self.graph_duration.saturating_sub(accounted);
    }

    pub fn record_data_lifecycle(&mut self, record: DataLifecycleRecord) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_profile() && !self.metrics_level.is_trace() {
            return;
        }
        let clock = &self.clock;
        let origin = *self.lifecycle_origin.get_or_insert_with(|| clock.now());
        let at_ns = clock.elapsed(origin).as_nanos() as u64;
        self.data_lifecycle.push(DataLifecycleEvent {
            correlation_id: record.correlation_id,
            stage: record.stage,
            at_ns,
            node_idx: record.node_idx,
            edge_idx: record.edge_idx,
            port: record.port,
            payload: record.payload,
            adapter_steps: record.adapter_steps,
            detail: record.detail,
        });
    }

    pub fn aggregate_groups(&mut self, nodes: &[crate::plan::RuntimeNode]) {
        for (idx, metrics) in self.node_metrics.iter() {
            let Some(node) = nodes.get(idx) else {
                continue;
            };
            let group = GroupMetadata::from_node_metadata(&node.metadata);
            let Some(group) = group.preferred_id() else {
                continue;
            };
            self.group_metrics
                .entry(group.to_string())
                .or_default()
                .merge(metrics.clone());
        }
    }
}

#[cfg(test)]
#[path = "telemetry_tests.rs"]
mod tests;
