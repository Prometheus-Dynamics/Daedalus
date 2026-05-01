use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use daedalus_planner::GroupMetadata;

use crate::perf::PerfSample;

mod basics;
mod lifecycle;
mod metrics;
mod report;
mod resources;
mod summary;

pub use basics::{Histogram, MetricsLevel, ProfileLevel, Profiler};
pub use lifecycle::{
    DataLifecycleEvent, DataLifecycleRecord, DataLifecycleStage, NodeFailure, TraceEvent,
};
pub use metrics::{
    CustomMetricValue, EdgeMetrics, EdgePressureMetrics, EdgePressureReason, FfiAdapterTelemetry,
    FfiBackendTelemetry, FfiPackageTelemetry, FfiPayloadTelemetry, FfiTelemetryReport,
    FfiWorkerTelemetry, NodeMetrics, TransportMetrics,
};
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
    pub node_metrics: BTreeMap<usize, NodeMetrics>,
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
            .filter_map(|(idx, node)| node.perf.clone().map(|perf| (*idx, perf)))
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
            .filter_map(|(idx, metrics)| (metrics.calls == 0).then_some(*idx))
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
            node_timing: self.node_metrics.clone(),
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
        self.lifecycle_origin =
            (self.metrics_level.is_profile() || self.metrics_level.is_trace()).then(Instant::now);
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
            self.node_metrics.entry(node).or_default().merge(metrics);
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

    pub fn record_node_duration(&mut self, node_idx: usize, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_basic() {
            return;
        }
        self.in_flight_node_transport_metrics.remove(&node_idx);
        let entry = self.node_metrics.entry(node_idx).or_default();
        entry.record(duration);
        if self.metrics_level.is_detailed() {
            entry
                .duration_histogram
                .get_or_insert_with(Histogram::default)
                .record_duration(duration);
        }
    }

    pub fn record_node_handler_duration(&mut self, node_idx: usize, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx).or_default();
        entry.record_handler(duration);
    }

    pub fn record_node_cpu_duration(&mut self, node_idx: usize, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx).or_default();
        entry.cpu_duration += duration;
    }

    pub fn record_node_perf(&mut self, node_idx: usize, sample: PerfSample) {
        if !cfg!(feature = "metrics") {
            return;
        }
        let entry = self.node_metrics.entry(node_idx).or_default();
        entry.record_perf(sample);
    }

    pub fn record_node_custom_metric(
        &mut self,
        node_idx: usize,
        name: impl Into<String>,
        value: CustomMetricValue,
    ) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_basic() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx).or_default();
        entry.record_custom(name, value);
    }

    pub fn record_node_custom_metrics(
        &mut self,
        node_idx: usize,
        metrics: std::collections::BTreeMap<String, CustomMetricValue>,
    ) {
        for (name, value) in metrics {
            self.record_node_custom_metric(node_idx, name, value);
        }
    }

    pub fn start_node_call(&mut self, node_idx: usize) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        self.in_flight_node_transport_metrics
            .insert(node_idx, InFlightNodeTransportMetrics::default());
    }

    pub fn record_node_transport_in(&mut self, node_idx: usize, bytes: Option<u64>) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx).or_default();
        let payload = entry
            .transport
            .get_or_insert_with(TransportMetrics::default);
        payload.in_count = payload.in_count.saturating_add(1);
        if let Some(bytes) = bytes {
            payload.in_bytes = payload.in_bytes.saturating_add(bytes);
            let in_flight = self
                .in_flight_node_transport_metrics
                .entry(node_idx)
                .or_default();
            in_flight.input_bytes = in_flight.input_bytes.saturating_add(bytes);
            payload.peak_input_bytes = payload.peak_input_bytes.max(in_flight.input_bytes);
            payload.peak_working_set_bytes = payload
                .peak_working_set_bytes
                .max(in_flight.input_bytes.saturating_add(in_flight.output_bytes));
        }
    }

    pub fn record_node_transport_out(&mut self, node_idx: usize, bytes: Option<u64>) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx).or_default();
        let payload = entry
            .transport
            .get_or_insert_with(TransportMetrics::default);
        payload.out_count = payload.out_count.saturating_add(1);
        if let Some(bytes) = bytes {
            payload.out_bytes = payload.out_bytes.saturating_add(bytes);
            let in_flight = self
                .in_flight_node_transport_metrics
                .entry(node_idx)
                .or_default();
            in_flight.output_bytes = in_flight.output_bytes.saturating_add(bytes);
            payload.peak_output_bytes = payload.peak_output_bytes.max(in_flight.output_bytes);
            payload.peak_working_set_bytes = payload
                .peak_working_set_bytes
                .max(in_flight.input_bytes.saturating_add(in_flight.output_bytes));
        }
    }

    pub fn record_node_resource_snapshot(
        &mut self,
        node_idx: usize,
        snapshot: crate::state::NodeResourceSnapshot,
    ) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        if snapshot == crate::state::NodeResourceSnapshot::default() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx).or_default();
        let resources = entry
            .resources
            .get_or_insert_with(NodeResourceMetrics::default);
        resources.observe_snapshot(&snapshot);
    }

    pub fn record_node_materialization(&mut self, node_idx: usize, bytes: u64) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() || bytes == 0 {
            return;
        }
        let entry = self.node_metrics.entry(node_idx).or_default();
        let resources = entry
            .resources
            .get_or_insert_with(NodeResourceMetrics::default);
        resources.materialization.record(bytes);
    }

    pub fn record_node_conversion(&mut self, node_idx: usize, bytes: u64) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() || bytes == 0 {
            return;
        }
        let entry = self.node_metrics.entry(node_idx).or_default();
        let resources = entry
            .resources
            .get_or_insert_with(NodeResourceMetrics::default);
        resources.conversion.record(bytes);
    }

    pub fn record_node_gpu_transfer(&mut self, node_idx: usize, upload: bool, bytes: u64) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() || bytes == 0 {
            return;
        }
        let entry = self.node_metrics.entry(node_idx).or_default();
        let resources = entry
            .resources
            .get_or_insert_with(NodeResourceMetrics::default);
        if upload {
            resources.gpu_upload.record(bytes);
        } else {
            resources.gpu_download.record(bytes);
        }
    }

    pub fn explain_node_allocation_spike(
        &self,
        node_idx: usize,
    ) -> Option<NodeAllocationSpikeExplanation> {
        let metrics = self.node_metrics.get(&node_idx)?;
        let resources = metrics.resources.as_ref()?;
        let mut dominant_sources = vec![
            ("frame_scratch", resources.frame_scratch.peak_retained_bytes),
            ("warm_cache", resources.warm_cache.peak_retained_bytes),
            (
                "persistent_state",
                resources.persistent_state.peak_retained_bytes,
            ),
            ("materialization", resources.materialization.total_bytes),
            ("conversion", resources.conversion.total_bytes),
            ("gpu_upload", resources.gpu_upload.total_bytes),
            ("gpu_download", resources.gpu_download.total_bytes),
        ];
        dominant_sources.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));

        Some(NodeAllocationSpikeExplanation {
            node_idx,
            frame_scratch: resources.frame_scratch.clone(),
            warm_cache: resources.warm_cache.clone(),
            persistent_state: resources.persistent_state.clone(),
            materialization: resources.materialization.clone(),
            conversion: resources.conversion.clone(),
            gpu_upload: resources.gpu_upload.clone(),
            gpu_download: resources.gpu_download.clone(),
            dominant_sources: dominant_sources
                .into_iter()
                .filter(|(_, bytes)| *bytes > 0)
                .map(|(name, bytes)| format!("{name}:{bytes}"))
                .collect(),
        })
    }

    pub fn record_edge_wait(&mut self, edge_idx: usize, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_basic() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.total_wait += duration;
        entry.samples += 1;
        if self.metrics_level.is_detailed() {
            entry
                .wait_histogram
                .get_or_insert_with(Histogram::default)
                .record_duration(duration);
        }
    }

    pub fn record_edge_depth(&mut self, edge_idx: usize, depth: usize) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        let depth_u64 = depth as u64;
        entry.current_depth = depth_u64;
        entry.max_depth = entry.max_depth.max(depth_u64);
        entry
            .depth_histogram
            .get_or_insert_with(Histogram::default)
            .record_value(depth_u64);
    }

    pub fn record_edge_capacity(&mut self, edge_idx: usize, capacity: Option<usize>) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let Some(capacity) = capacity else {
            return;
        };
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.capacity = Some(entry.capacity.unwrap_or(0).max(capacity as u64));
    }

    pub fn record_edge_queue_bytes(&mut self, edge_idx: usize, current_bytes: u64) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.current_queue_bytes = current_bytes;
        entry.peak_queue_bytes = entry.peak_queue_bytes.max(current_bytes);
    }

    pub fn record_edge_transport(&mut self, edge_idx: usize, bytes: Option<u64>) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.transport_count = entry.transport_count.saturating_add(1);
        if let Some(bytes) = bytes {
            entry.transport_bytes = entry.transport_bytes.saturating_add(bytes);
        }
    }

    pub fn record_edge_handoff(
        &mut self,
        edge_idx: usize,
        unique: bool,
        cloned_payload: bool,
        copied_bytes: u64,
    ) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        if unique {
            entry.unique_handoffs = entry.unique_handoffs.saturating_add(1);
        } else {
            entry.shared_handoffs = entry.shared_handoffs.saturating_add(1);
        }
        if cloned_payload {
            entry.payload_clone_count = entry.payload_clone_count.saturating_add(1);
        }
        entry.copied_bytes = entry.copied_bytes.saturating_add(copied_bytes);
    }

    pub fn record_edge_transport_apply_duration(&mut self, edge_idx: usize, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.transport_apply_duration += duration;
        entry.transport_apply_count = entry.transport_apply_count.saturating_add(1);
        if self.metrics_level.is_profile() {
            entry
                .transport_apply_histogram
                .get_or_insert_with(Histogram::default)
                .record_duration(duration);
        }
    }

    pub fn record_edge_adapter_duration(&mut self, edge_idx: usize, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.adapter_duration += duration;
        entry.adapter_count = entry.adapter_count.saturating_add(1);
        if self.metrics_level.is_profile() {
            entry
                .adapter_histogram
                .get_or_insert_with(Histogram::default)
                .record_duration(duration);
        }
    }

    pub fn record_edge_adapter_error(&mut self, edge_idx: usize) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.adapter_errors = entry.adapter_errors.saturating_add(1);
    }

    pub fn record_edge_gpu_transfer(&mut self, edge_idx: usize, upload: bool) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        if upload {
            entry.gpu_uploads = entry.gpu_uploads.saturating_add(1);
        } else {
            entry.gpu_downloads = entry.gpu_downloads.saturating_add(1);
        }
    }

    pub fn record_edge_drop(&mut self, edge_idx: usize, count: u64) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() || count == 0 {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.drops = entry.drops.saturating_add(count);
    }

    pub fn record_edge_pressure_event(
        &mut self,
        edge_idx: usize,
        reason: EdgePressureReason,
        dropped_count: u64,
    ) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.pressure_events.total = entry.pressure_events.total.saturating_add(1);
        match reason {
            EdgePressureReason::DropIncoming => {
                entry.pressure_events.drop_incoming =
                    entry.pressure_events.drop_incoming.saturating_add(1);
            }
            EdgePressureReason::DropOldest => {
                entry.pressure_events.drop_oldest =
                    entry.pressure_events.drop_oldest.saturating_add(1);
            }
            EdgePressureReason::DropNewest => {
                entry.pressure_events.drop_newest =
                    entry.pressure_events.drop_newest.saturating_add(1);
            }
            EdgePressureReason::Backpressure => {
                entry.pressure_events.backpressure =
                    entry.pressure_events.backpressure.saturating_add(1);
            }
            EdgePressureReason::ErrorOverflow => {
                entry.pressure_events.error_overflow =
                    entry.pressure_events.error_overflow.saturating_add(1);
            }
            EdgePressureReason::LatestReplace => {
                entry.pressure_events.latest_replace =
                    entry.pressure_events.latest_replace.saturating_add(1);
            }
            EdgePressureReason::CoalesceReplace => {
                entry.pressure_events.coalesce_replace =
                    entry.pressure_events.coalesce_replace.saturating_add(1);
            }
        }
        entry.drops = entry.drops.saturating_add(dropped_count);
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
        let origin = self.lifecycle_origin.get_or_insert_with(Instant::now);
        let at_ns = origin.elapsed().as_nanos() as u64;
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
        for (idx, metrics) in &self.node_metrics {
            let Some(node) = nodes.get(*idx) else {
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
