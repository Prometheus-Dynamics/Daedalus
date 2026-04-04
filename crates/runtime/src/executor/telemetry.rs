use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use super::RuntimeValue;
use crate::perf::PerfSample;

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MetricsLevel {
    Off,
    #[default]
    Basic,
    Detailed,
    Profile,
}

impl MetricsLevel {
    pub fn is_basic(self) -> bool {
        self >= MetricsLevel::Basic
    }

    pub fn is_detailed(self) -> bool {
        self >= MetricsLevel::Detailed
    }

    pub fn is_profile(self) -> bool {
        self >= MetricsLevel::Profile
    }
}

const HIST_BUCKETS: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Histogram {
    pub buckets: [u64; HIST_BUCKETS],
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: [0; HIST_BUCKETS],
        }
    }
}

impl Histogram {
    pub fn record_value(&mut self, value: u64) {
        let v = value.max(1);
        let idx = (63 - v.leading_zeros() as usize).min(HIST_BUCKETS - 1);
        self.buckets[idx] = self.buckets[idx].saturating_add(1);
    }

    pub fn record_duration(&mut self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        self.record_value(micros);
    }

    pub fn merge(&mut self, other: &Histogram) {
        for (dst, src) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *dst = dst.saturating_add(*src);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.iter().all(|v| *v == 0)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransportMetrics {
    pub in_bytes: u64,
    pub out_bytes: u64,
    pub in_count: u64,
    pub out_count: u64,
    #[serde(default)]
    pub peak_input_bytes: u64,
    #[serde(default)]
    pub peak_output_bytes: u64,
    #[serde(default)]
    pub peak_working_set_bytes: u64,
}

impl TransportMetrics {
    fn merge(&mut self, other: &TransportMetrics) {
        self.in_bytes = self.in_bytes.saturating_add(other.in_bytes);
        self.out_bytes = self.out_bytes.saturating_add(other.out_bytes);
        self.in_count = self.in_count.saturating_add(other.in_count);
        self.out_count = self.out_count.saturating_add(other.out_count);
        self.peak_input_bytes = self.peak_input_bytes.max(other.peak_input_bytes);
        self.peak_output_bytes = self.peak_output_bytes.max(other.peak_output_bytes);
        self.peak_working_set_bytes = self
            .peak_working_set_bytes
            .max(other.peak_working_set_bytes);
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct InFlightNodeTransportMetrics {
    input_bytes: u64,
    output_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TraceEvent {
    pub node_idx: usize,
    pub start_ns: u64,
    pub duration_ns: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeFailure {
    pub node_idx: usize,
    pub node_id: String,
    pub code: String,
    pub message: String,
}

pub type RuntimeDataSizeInspector = fn(&(dyn std::any::Any + Send + Sync)) -> Option<u64>;

fn runtime_data_size_inspectors() -> &'static RwLock<Vec<RuntimeDataSizeInspector>> {
    static INSPECTORS: OnceLock<RwLock<Vec<RuntimeDataSizeInspector>>> = OnceLock::new();
    INSPECTORS.get_or_init(|| RwLock::new(Vec::new()))
}

pub fn register_runtime_data_size_inspector(inspector: RuntimeDataSizeInspector) {
    let lock = runtime_data_size_inspectors();
    let mut inspectors = lock
        .write()
        .expect("runtime data size inspector registry poisoned");
    let inspector_addr = inspector as usize;
    if inspectors
        .iter()
        .any(|existing| *existing as usize == inspector_addr)
    {
        return;
    }
    inspectors.push(inspector);
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
    pub metrics_level: MetricsLevel,
    /// Per-node-instance metrics keyed by the planned node index (`NodeRef.0`).
    pub node_metrics: BTreeMap<usize, NodeMetrics>,
    /// Per-group aggregate metrics keyed by group id (e.g. embedded graphs).
    pub group_metrics: BTreeMap<String, NodeMetrics>,
    /// Per-edge queue wait metrics keyed by the planned edge index.
    pub edge_metrics: BTreeMap<usize, EdgeMetrics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<Vec<TraceEvent>>,
    #[serde(skip)]
    in_flight_node_transport_metrics: BTreeMap<usize, InFlightNodeTransportMetrics>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeMetrics {
    pub total_duration: Duration,
    pub calls: usize,
    #[serde(default)]
    pub cpu_duration: Duration,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub perf: Option<NodePerfMetrics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_histogram: Option<Histogram>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<TransportMetrics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<NodeResourceMetrics>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResourceMetrics {
    #[serde(default)]
    pub current_live_bytes: u64,
    #[serde(default)]
    pub current_retained_bytes: u64,
    #[serde(default)]
    pub current_touched_bytes: u64,
    #[serde(default)]
    pub peak_live_bytes: u64,
    #[serde(default)]
    pub peak_retained_bytes: u64,
    #[serde(default)]
    pub peak_touched_bytes: u64,
    #[serde(default)]
    pub current_allocation_events: u64,
    #[serde(default)]
    pub peak_allocation_events: u64,
}

impl ResourceMetrics {
    fn observe(&mut self, usage: crate::state::ResourceUsage) {
        self.current_live_bytes = usage.live_bytes;
        self.current_retained_bytes = usage.retained_bytes;
        self.current_touched_bytes = usage.touched_bytes;
        self.peak_live_bytes = self.peak_live_bytes.max(usage.live_bytes);
        self.peak_retained_bytes = self.peak_retained_bytes.max(usage.retained_bytes);
        self.peak_touched_bytes = self.peak_touched_bytes.max(usage.touched_bytes);
        self.current_allocation_events = usage.allocation_events;
        self.peak_allocation_events = self.peak_allocation_events.max(usage.allocation_events);
    }

    fn merge(&mut self, other: ResourceMetrics) {
        self.current_live_bytes = self.current_live_bytes.max(other.current_live_bytes);
        self.current_retained_bytes = self
            .current_retained_bytes
            .max(other.current_retained_bytes);
        self.current_touched_bytes = self.current_touched_bytes.max(other.current_touched_bytes);
        self.peak_live_bytes = self.peak_live_bytes.max(other.peak_live_bytes);
        self.peak_retained_bytes = self.peak_retained_bytes.max(other.peak_retained_bytes);
        self.peak_touched_bytes = self.peak_touched_bytes.max(other.peak_touched_bytes);
        self.current_allocation_events = self
            .current_allocation_events
            .max(other.current_allocation_events);
        self.peak_allocation_events = self
            .peak_allocation_events
            .max(other.peak_allocation_events);
    }

    fn is_empty(&self) -> bool {
        self.current_live_bytes == 0
            && self.current_retained_bytes == 0
            && self.current_touched_bytes == 0
            && self.peak_live_bytes == 0
            && self.peak_retained_bytes == 0
            && self.peak_touched_bytes == 0
            && self.current_allocation_events == 0
            && self.peak_allocation_events == 0
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InternalTransferMetrics {
    #[serde(default)]
    pub total_bytes: u64,
    #[serde(default)]
    pub count: u64,
    #[serde(default)]
    pub peak_bytes: u64,
}

impl InternalTransferMetrics {
    fn record(&mut self, bytes: u64) {
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        self.count = self.count.saturating_add(1);
        self.peak_bytes = self.peak_bytes.max(bytes);
    }

    fn merge(&mut self, other: InternalTransferMetrics) {
        self.total_bytes = self.total_bytes.saturating_add(other.total_bytes);
        self.count = self.count.saturating_add(other.count);
        self.peak_bytes = self.peak_bytes.max(other.peak_bytes);
    }

    fn is_empty(&self) -> bool {
        self.total_bytes == 0 && self.count == 0 && self.peak_bytes == 0
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeResourceMetrics {
    pub frame_scratch: ResourceMetrics,
    pub warm_cache: ResourceMetrics,
    pub persistent_state: ResourceMetrics,
    #[serde(default, skip_serializing_if = "InternalTransferMetrics::is_empty")]
    pub materialization: InternalTransferMetrics,
    #[serde(default, skip_serializing_if = "InternalTransferMetrics::is_empty")]
    pub conversion: InternalTransferMetrics,
    #[serde(default, skip_serializing_if = "InternalTransferMetrics::is_empty")]
    pub gpu_upload: InternalTransferMetrics,
    #[serde(default, skip_serializing_if = "InternalTransferMetrics::is_empty")]
    pub gpu_download: InternalTransferMetrics,
}

impl NodeResourceMetrics {
    fn observe_snapshot(&mut self, snapshot: &crate::state::NodeResourceSnapshot) {
        self.frame_scratch.observe(snapshot.frame_scratch);
        self.warm_cache.observe(snapshot.warm_cache);
        self.persistent_state.observe(snapshot.persistent_state);
    }

    fn merge(&mut self, other: NodeResourceMetrics) {
        self.frame_scratch.merge(other.frame_scratch);
        self.warm_cache.merge(other.warm_cache);
        self.persistent_state.merge(other.persistent_state);
        self.materialization.merge(other.materialization);
        self.conversion.merge(other.conversion);
        self.gpu_upload.merge(other.gpu_upload);
        self.gpu_download.merge(other.gpu_download);
    }

    fn is_empty(&self) -> bool {
        self.frame_scratch.is_empty()
            && self.warm_cache.is_empty()
            && self.persistent_state.is_empty()
            && self.materialization.is_empty()
            && self.conversion.is_empty()
            && self.gpu_upload.is_empty()
            && self.gpu_download.is_empty()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeAllocationSpikeExplanation {
    pub node_idx: usize,
    pub frame_scratch: ResourceMetrics,
    pub warm_cache: ResourceMetrics,
    pub persistent_state: ResourceMetrics,
    pub materialization: InternalTransferMetrics,
    pub conversion: InternalTransferMetrics,
    pub gpu_upload: InternalTransferMetrics,
    pub gpu_download: InternalTransferMetrics,
    #[serde(default)]
    pub dominant_sources: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodePerfMetrics {
    pub cache_misses: u64,
    pub branch_instructions: u64,
    pub branch_misses: u64,
}

impl NodePerfMetrics {
    fn record(&mut self, sample: PerfSample) {
        self.cache_misses = self.cache_misses.saturating_add(sample.cache_misses);
        self.branch_instructions = self
            .branch_instructions
            .saturating_add(sample.branch_instructions);
        self.branch_misses = self.branch_misses.saturating_add(sample.branch_misses);
    }

    fn merge(&mut self, other: NodePerfMetrics) {
        self.cache_misses = self.cache_misses.saturating_add(other.cache_misses);
        self.branch_instructions = self
            .branch_instructions
            .saturating_add(other.branch_instructions);
        self.branch_misses = self.branch_misses.saturating_add(other.branch_misses);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EdgeMetrics {
    pub total_wait: Duration,
    pub samples: usize,
    #[serde(default)]
    pub max_depth: u64,
    #[serde(default)]
    pub current_depth: u64,
    #[serde(default)]
    pub peak_queue_bytes: u64,
    #[serde(default)]
    pub current_queue_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<u64>,
    #[serde(default)]
    pub drops: u64,
    #[serde(default)]
    pub transport_bytes: u64,
    #[serde(default)]
    pub transport_count: u64,
    #[serde(default)]
    pub gpu_uploads: u64,
    #[serde(default)]
    pub gpu_downloads: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_histogram: Option<Histogram>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth_histogram: Option<Histogram>,
}

impl EdgeMetrics {
    pub fn merge(&mut self, other: EdgeMetrics) {
        self.total_wait += other.total_wait;
        self.samples += other.samples;
        self.max_depth = self.max_depth.max(other.max_depth);
        self.current_depth = self.current_depth.max(other.current_depth);
        self.peak_queue_bytes = self.peak_queue_bytes.max(other.peak_queue_bytes);
        self.current_queue_bytes = self.current_queue_bytes.max(other.current_queue_bytes);
        self.capacity = self.capacity.max(other.capacity);
        self.drops = self.drops.saturating_add(other.drops);
        self.transport_bytes = self.transport_bytes.saturating_add(other.transport_bytes);
        self.transport_count = self.transport_count.saturating_add(other.transport_count);
        self.gpu_uploads = self.gpu_uploads.saturating_add(other.gpu_uploads);
        self.gpu_downloads = self.gpu_downloads.saturating_add(other.gpu_downloads);
        if let Some(other_hist) = other.wait_histogram {
            let hist = self.wait_histogram.get_or_insert_with(Histogram::default);
            hist.merge(&other_hist);
        }
        if let Some(other_hist) = other.depth_histogram {
            let hist = self.depth_histogram.get_or_insert_with(Histogram::default);
            hist.merge(&other_hist);
        }
    }
}

impl NodeMetrics {
    pub fn record(&mut self, duration: Duration) {
        self.total_duration += duration;
        self.calls += 1;
    }

    pub fn record_perf(&mut self, sample: PerfSample) {
        let perf = self.perf.get_or_insert_with(NodePerfMetrics::default);
        perf.record(sample);
    }

    pub fn merge(&mut self, other: NodeMetrics) {
        self.total_duration += other.total_duration;
        self.calls += other.calls;
        self.cpu_duration += other.cpu_duration;
        if let Some(other_perf) = other.perf {
            let perf = self.perf.get_or_insert_with(NodePerfMetrics::default);
            perf.merge(other_perf);
        }
        if let Some(other_hist) = other.duration_histogram {
            let hist = self
                .duration_histogram
                .get_or_insert_with(Histogram::default);
            hist.merge(&other_hist);
        }
        if let Some(other_payload) = other.transport {
            let payload = self.transport.get_or_insert_with(TransportMetrics::default);
            payload.merge(&other_payload);
        }
        if let Some(other_resources) = other.resources {
            let resources = self
                .resources
                .get_or_insert_with(NodeResourceMetrics::default);
            resources.merge(other_resources);
            if resources.is_empty() {
                self.resources = None;
            }
        }
    }
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
            ..Default::default()
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
        self.metrics_level = if cfg!(feature = "metrics") {
            level
        } else {
            MetricsLevel::Off
        };
        self.node_metrics.clear();
        self.group_metrics.clear();
        self.edge_metrics.clear();
        if let Some(trace) = self.trace.as_mut() {
            trace.clear();
        }
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

    pub fn record_trace_event(&mut self, node_idx: usize, start: Duration, duration: Duration) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_profile() {
            return;
        }
        let trace = self.trace.get_or_insert_with(Vec::new);
        trace.push(TraceEvent {
            node_idx,
            start_ns: start.as_nanos() as u64,
            duration_ns: duration.as_nanos() as u64,
        });
    }

    pub fn aggregate_groups(&mut self, nodes: &[crate::plan::RuntimeNode]) {
        const GROUP_ID_KEY: &str = "daedalus.group_id";
        const GROUP_KEY: &str = "daedalus.embedded_group";
        for (idx, metrics) in &self.node_metrics {
            let Some(node) = nodes.get(*idx) else {
                continue;
            };
            let group = node
                .metadata
                .get(GROUP_ID_KEY)
                .and_then(|v| match v {
                    daedalus_data::model::Value::String(s) => Some(s.as_ref()),
                    _ => None,
                })
                .or_else(|| {
                    node.metadata.get(GROUP_KEY).and_then(|v| match v {
                        daedalus_data::model::Value::String(s) => Some(s.as_ref()),
                        _ => None,
                    })
                });
            let Some(group) = group else {
                continue;
            };
            let trimmed = group.trim();
            if trimmed.is_empty() {
                continue;
            }
            self.group_metrics
                .entry(trimmed.to_string())
                .or_default()
                .merge(metrics.clone());
        }
    }
}

pub(crate) fn runtime_value_size_bytes(payload: &RuntimeValue) -> Option<u64> {
    match payload {
        RuntimeValue::Unit => Some(0),
        RuntimeValue::Bytes(bytes) => Some(bytes.len() as u64),
        RuntimeValue::Value(value) => value_size_bytes(value),
        RuntimeValue::Any(any) => any_size_bytes(any),
        #[cfg(feature = "gpu")]
        RuntimeValue::Data(payload) => data_cell_size_bytes(payload),
    }
}

fn value_size_bytes(value: &daedalus_data::model::Value) -> Option<u64> {
    match value {
        daedalus_data::model::Value::String(s) => Some(s.len() as u64),
        daedalus_data::model::Value::Bytes(b) => Some(b.len() as u64),
        _ => None,
    }
}

pub(crate) fn any_ref_size_bytes(any: &(dyn std::any::Any + Send + Sync)) -> Option<u64> {
    if let Some(bytes) = any.downcast_ref::<Vec<u8>>() {
        return Some(bytes.len() as u64);
    }
    if let Some(bytes) = any.downcast_ref::<Arc<[u8]>>() {
        return Some(bytes.len() as u64);
    }
    if let Some(img) = any.downcast_ref::<image::DynamicImage>() {
        return Some(dynamic_image_size_bytes(img));
    }
    if let Some(img) = any.downcast_ref::<image::GrayImage>() {
        return Some(img.as_raw().len() as u64);
    }
    if let Some(img) = any.downcast_ref::<image::GrayAlphaImage>() {
        return Some(img.as_raw().len() as u64);
    }
    if let Some(img) = any.downcast_ref::<image::RgbImage>() {
        return Some(img.as_raw().len() as u64);
    }
    if let Some(img) = any.downcast_ref::<image::RgbaImage>() {
        return Some(img.as_raw().len() as u64);
    }
    #[cfg(feature = "gpu")]
    if let Some(handle) = any.downcast_ref::<daedalus_gpu::GpuImageHandle>() {
        return Some(gpu_image_size_bytes(handle));
    }
    if let Ok(inspectors) = runtime_data_size_inspectors().read() {
        for inspector in inspectors.iter() {
            if let Some(bytes) = inspector(any) {
                return Some(bytes);
            }
        }
    }
    None
}

fn any_size_bytes(any: &Arc<dyn std::any::Any + Send + Sync>) -> Option<u64> {
    any_ref_size_bytes(any.as_ref())
}

fn dynamic_image_size_bytes(img: &image::DynamicImage) -> u64 {
    match img {
        image::DynamicImage::ImageLuma8(i) => i.as_raw().len() as u64,
        image::DynamicImage::ImageLumaA8(i) => i.as_raw().len() as u64,
        image::DynamicImage::ImageRgb8(i) => i.as_raw().len() as u64,
        image::DynamicImage::ImageRgba8(i) => i.as_raw().len() as u64,
        image::DynamicImage::ImageLuma16(i) => {
            (i.as_raw().len() * std::mem::size_of::<u16>()) as u64
        }
        image::DynamicImage::ImageLumaA16(i) => {
            (i.as_raw().len() * std::mem::size_of::<u16>()) as u64
        }
        image::DynamicImage::ImageRgb16(i) => {
            (i.as_raw().len() * std::mem::size_of::<u16>()) as u64
        }
        image::DynamicImage::ImageRgba16(i) => {
            (i.as_raw().len() * std::mem::size_of::<u16>()) as u64
        }
        image::DynamicImage::ImageRgb32F(i) => {
            (i.as_raw().len() * std::mem::size_of::<f32>()) as u64
        }
        image::DynamicImage::ImageRgba32F(i) => {
            (i.as_raw().len() * std::mem::size_of::<f32>()) as u64
        }
        _ => 0,
    }
}

#[cfg(feature = "gpu")]
fn data_cell_size_bytes(payload: &daedalus_gpu::DataCell) -> Option<u64> {
    if payload.is_gpu() {
        if let Some(handle) = payload.as_gpu::<image::DynamicImage>() {
            Some(gpu_image_size_bytes(handle))
        } else if let Some(handle) = payload.as_gpu::<image::GrayImage>() {
            Some(gpu_image_size_bytes(handle))
        } else if let Some(handle) = payload.as_gpu::<image::RgbImage>() {
            Some(gpu_image_size_bytes(handle))
        } else {
            payload
                .as_gpu::<image::RgbaImage>()
                .map(gpu_image_size_bytes)
        }
    } else {
        if let Some(img) = payload.as_cpu::<image::DynamicImage>() {
            Some(dynamic_image_size_bytes(img))
        } else if let Some(img) = payload.as_cpu::<image::GrayImage>() {
            Some(img.as_raw().len() as u64)
        } else if let Some(img) = payload.as_cpu::<image::RgbImage>() {
            Some(img.as_raw().len() as u64)
        } else {
            payload
                .as_cpu::<image::RgbaImage>()
                .map(|img| img.as_raw().len() as u64)
        }
    }
}

#[cfg(feature = "gpu")]
fn gpu_image_size_bytes(handle: &daedalus_gpu::GpuImageHandle) -> u64 {
    let bpp = daedalus_gpu::format_bytes_per_pixel(handle.format).unwrap_or(4) as u64;
    handle.width as u64 * handle.height as u64 * bpp
}

#[cfg(test)]
mod tests {
    use super::{ExecutionTelemetry, MetricsLevel};
    use std::time::Duration;

    #[test]
    fn record_node_resource_snapshot_tracks_current_and_peak_usage() {
        let mut telemetry = ExecutionTelemetry::with_level(MetricsLevel::Detailed);
        telemetry.record_node_resource_snapshot(
            1,
            crate::state::NodeResourceSnapshot {
                frame_scratch: crate::state::ResourceUsage {
                    live_bytes: 32,
                    retained_bytes: 64,
                    touched_bytes: 48,
                    allocation_events: 2,
                },
                warm_cache: crate::state::ResourceUsage {
                    live_bytes: 10,
                    retained_bytes: 20,
                    touched_bytes: 14,
                    allocation_events: 1,
                },
                persistent_state: crate::state::ResourceUsage::default(),
            },
        );
        telemetry.record_node_resource_snapshot(
            1,
            crate::state::NodeResourceSnapshot {
                frame_scratch: crate::state::ResourceUsage {
                    live_bytes: 16,
                    retained_bytes: 96,
                    touched_bytes: 18,
                    allocation_events: 1,
                },
                warm_cache: crate::state::ResourceUsage::default(),
                persistent_state: crate::state::ResourceUsage {
                    live_bytes: 5,
                    retained_bytes: 5,
                    touched_bytes: 5,
                    allocation_events: 0,
                },
            },
        );

        let metrics = telemetry.node_metrics.get(&1).unwrap();
        let resources = metrics.resources.as_ref().unwrap();
        assert_eq!(resources.frame_scratch.current_live_bytes, 16);
        assert_eq!(resources.frame_scratch.current_retained_bytes, 96);
        assert_eq!(resources.frame_scratch.current_touched_bytes, 18);
        assert_eq!(resources.frame_scratch.peak_live_bytes, 32);
        assert_eq!(resources.frame_scratch.peak_retained_bytes, 96);
        assert_eq!(resources.frame_scratch.peak_touched_bytes, 48);
        assert_eq!(resources.frame_scratch.current_allocation_events, 1);
        assert_eq!(resources.frame_scratch.peak_allocation_events, 2);
        assert_eq!(resources.persistent_state.current_live_bytes, 5);
        assert_eq!(resources.warm_cache.peak_retained_bytes, 20);
        assert_eq!(resources.warm_cache.peak_touched_bytes, 14);
    }

    #[test]
    fn record_node_internal_transfers_tracks_materialization_conversion_and_gpu_flow() {
        let mut telemetry = ExecutionTelemetry::with_level(MetricsLevel::Detailed);
        telemetry.record_node_materialization(3, 64);
        telemetry.record_node_conversion(3, 24);
        telemetry.record_node_gpu_transfer(3, true, 48);
        telemetry.record_node_gpu_transfer(3, false, 32);

        let metrics = telemetry.node_metrics.get(&3).unwrap();
        let resources = metrics.resources.as_ref().unwrap();
        assert_eq!(resources.materialization.count, 1);
        assert_eq!(resources.materialization.total_bytes, 64);
        assert_eq!(resources.conversion.count, 1);
        assert_eq!(resources.conversion.total_bytes, 24);
        assert_eq!(resources.gpu_upload.count, 1);
        assert_eq!(resources.gpu_upload.total_bytes, 48);
        assert_eq!(resources.gpu_download.count, 1);
        assert_eq!(resources.gpu_download.total_bytes, 32);
    }

    #[test]
    fn explain_node_allocation_spike_reports_dominant_sources() {
        let mut telemetry = ExecutionTelemetry::with_level(MetricsLevel::Detailed);
        telemetry.record_node_resource_snapshot(
            2,
            crate::state::NodeResourceSnapshot {
                frame_scratch: crate::state::ResourceUsage {
                    live_bytes: 24,
                    retained_bytes: 80,
                    touched_bytes: 40,
                    allocation_events: 2,
                },
                warm_cache: crate::state::ResourceUsage {
                    live_bytes: 12,
                    retained_bytes: 48,
                    touched_bytes: 18,
                    allocation_events: 1,
                },
                persistent_state: crate::state::ResourceUsage::default(),
            },
        );
        telemetry.record_node_materialization(2, 96);
        telemetry.record_node_conversion(2, 24);

        let explanation = telemetry
            .explain_node_allocation_spike(2)
            .expect("allocation explanation");
        assert_eq!(explanation.node_idx, 2);
        assert_eq!(explanation.frame_scratch.peak_retained_bytes, 80);
        assert_eq!(explanation.materialization.total_bytes, 96);
        assert_eq!(explanation.conversion.total_bytes, 24);
        assert_eq!(explanation.dominant_sources[0], "materialization:96");
        assert!(
            explanation
                .dominant_sources
                .iter()
                .any(|entry| entry == "frame_scratch:80")
        );
    }

    #[test]
    fn reset_for_reuse_clears_accumulated_state() {
        let mut telemetry = ExecutionTelemetry::with_level(MetricsLevel::Detailed);
        telemetry.nodes_executed = 3;
        telemetry.cpu_segments = 1;
        telemetry.gpu_segments = 2;
        telemetry.gpu_fallbacks = 1;
        telemetry.backpressure_events = 4;
        telemetry.warnings.push("warn".to_string());
        telemetry.errors.push(super::NodeFailure {
            node_idx: 1,
            node_id: "node".to_string(),
            code: "code".to_string(),
            message: "message".to_string(),
        });
        telemetry.graph_duration = Duration::from_millis(12);
        telemetry.record_node_materialization(3, 64);
        telemetry.record_edge_drop(2, 1);
        telemetry.record_trace_event(1, Duration::default(), Duration::from_nanos(5));

        telemetry.reset_for_reuse(MetricsLevel::Basic);

        assert_eq!(telemetry.nodes_executed, 0);
        assert_eq!(telemetry.cpu_segments, 0);
        assert_eq!(telemetry.gpu_segments, 0);
        assert_eq!(telemetry.gpu_fallbacks, 0);
        assert_eq!(telemetry.backpressure_events, 0);
        assert!(telemetry.warnings.is_empty());
        assert!(telemetry.errors.is_empty());
        assert_eq!(telemetry.graph_duration, Duration::default());
        assert_eq!(telemetry.metrics_level, MetricsLevel::Basic);
        assert!(telemetry.node_metrics.is_empty());
        assert!(telemetry.group_metrics.is_empty());
        assert!(telemetry.edge_metrics.is_empty());
        assert!(telemetry.trace.as_ref().is_none_or(Vec::is_empty));
        assert!(telemetry.in_flight_node_transport_metrics.is_empty());
    }
}
