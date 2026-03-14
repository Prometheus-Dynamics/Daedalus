use std::collections::BTreeMap;
use std::time::Duration;

use super::EdgePayload;
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
pub struct PayloadMetrics {
    pub in_bytes: u64,
    pub out_bytes: u64,
    pub in_count: u64,
    pub out_count: u64,
}

impl PayloadMetrics {
    fn merge(&mut self, other: &PayloadMetrics) {
        self.in_bytes = self.in_bytes.saturating_add(other.in_bytes);
        self.out_bytes = self.out_bytes.saturating_add(other.out_bytes);
        self.in_count = self.in_count.saturating_add(other.in_count);
        self.out_count = self.out_count.saturating_add(other.out_count);
    }
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
    pub payload: Option<PayloadMetrics>,
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
    pub payload_bytes: u64,
    #[serde(default)]
    pub payload_count: u64,
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
        self.payload_bytes = self.payload_bytes.saturating_add(other.payload_bytes);
        self.payload_count = self.payload_count.saturating_add(other.payload_count);
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
        if let Some(other_payload) = other.payload {
            let payload = self.payload.get_or_insert_with(PayloadMetrics::default);
            payload.merge(&other_payload);
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

    pub fn record_node_payload_in(&mut self, node_idx: usize, bytes: Option<u64>) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx).or_default();
        let payload = entry.payload.get_or_insert_with(PayloadMetrics::default);
        payload.in_count = payload.in_count.saturating_add(1);
        if let Some(bytes) = bytes {
            payload.in_bytes = payload.in_bytes.saturating_add(bytes);
        }
    }

    pub fn record_node_payload_out(&mut self, node_idx: usize, bytes: Option<u64>) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.node_metrics.entry(node_idx).or_default();
        let payload = entry.payload.get_or_insert_with(PayloadMetrics::default);
        payload.out_count = payload.out_count.saturating_add(1);
        if let Some(bytes) = bytes {
            payload.out_bytes = payload.out_bytes.saturating_add(bytes);
        }
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

    pub fn record_edge_payload(&mut self, edge_idx: usize, bytes: Option<u64>) {
        if !cfg!(feature = "metrics") {
            return;
        }
        if !self.metrics_level.is_detailed() {
            return;
        }
        let entry = self.edge_metrics.entry(edge_idx).or_default();
        entry.payload_count = entry.payload_count.saturating_add(1);
        if let Some(bytes) = bytes {
            entry.payload_bytes = entry.payload_bytes.saturating_add(bytes);
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

pub(crate) fn payload_size_bytes(payload: &EdgePayload) -> Option<u64> {
    match payload {
        EdgePayload::Unit => Some(0),
        EdgePayload::Bytes(bytes) => Some(bytes.len() as u64),
        EdgePayload::Value(value) => value_size_bytes(value),
        EdgePayload::Any(any) => any_size_bytes(any),
        #[cfg(feature = "gpu")]
        EdgePayload::Payload(_) => None,
        #[cfg(feature = "gpu")]
        EdgePayload::GpuImage(_) => None,
    }
}

fn value_size_bytes(value: &daedalus_data::model::Value) -> Option<u64> {
    match value {
        daedalus_data::model::Value::String(s) => Some(s.len() as u64),
        daedalus_data::model::Value::Bytes(b) => Some(b.len() as u64),
        _ => None,
    }
}

fn any_size_bytes(any: &std::sync::Arc<dyn std::any::Any + Send + Sync>) -> Option<u64> {
    if let Some(bytes) = any.downcast_ref::<Vec<u8>>() {
        return Some(bytes.len() as u64);
    }
    if let Some(bytes) = any.downcast_ref::<std::sync::Arc<[u8]>>() {
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
    None
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
