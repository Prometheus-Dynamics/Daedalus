use std::collections::BTreeMap;
use std::time::Duration;

use crate::perf::PerfSample;

use super::{Histogram, NodePerfMetrics, NodeResourceMetrics};

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

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeMetrics {
    pub total_duration: Duration,
    pub calls: usize,
    #[serde(default)]
    pub handler_duration: Duration,
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
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub custom: BTreeMap<String, CustomMetricValue>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CustomMetricValue {
    Counter(u64),
    Gauge(f64),
    Duration(Duration),
    Bytes(u64),
    Text(String),
    Bool(bool),
    Json(serde_json::Value),
}

impl Eq for CustomMetricValue {}

impl CustomMetricValue {
    pub fn merge(&mut self, other: CustomMetricValue) {
        match (self, other) {
            (Self::Counter(left), Self::Counter(right))
            | (Self::Bytes(left), Self::Bytes(right)) => {
                *left = left.saturating_add(right);
            }
            (Self::Duration(left), Self::Duration(right)) => {
                *left += right;
            }
            (slot, value) => {
                *slot = value;
            }
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EdgeMetrics {
    pub total_wait: Duration,
    pub samples: usize,
    #[serde(default)]
    pub transport_apply_duration: Duration,
    #[serde(default)]
    pub transport_apply_count: u64,
    #[serde(default)]
    pub adapter_duration: Duration,
    #[serde(default)]
    pub adapter_count: u64,
    #[serde(default)]
    pub adapter_errors: u64,
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
    pub pressure_events: EdgePressureMetrics,
    #[serde(default)]
    pub transport_bytes: u64,
    #[serde(default)]
    pub transport_count: u64,
    #[serde(default)]
    pub payload_clone_count: u64,
    #[serde(default)]
    pub unique_handoffs: u64,
    #[serde(default)]
    pub shared_handoffs: u64,
    #[serde(default)]
    pub copied_bytes: u64,
    #[serde(default)]
    pub gpu_uploads: u64,
    #[serde(default)]
    pub gpu_downloads: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_histogram: Option<Histogram>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_apply_histogram: Option<Histogram>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter_histogram: Option<Histogram>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth_histogram: Option<Histogram>,
}

impl EdgeMetrics {
    pub fn merge(&mut self, other: EdgeMetrics) {
        self.total_wait += other.total_wait;
        self.samples += other.samples;
        self.transport_apply_duration += other.transport_apply_duration;
        self.transport_apply_count = self
            .transport_apply_count
            .saturating_add(other.transport_apply_count);
        self.adapter_duration += other.adapter_duration;
        self.adapter_count = self.adapter_count.saturating_add(other.adapter_count);
        self.adapter_errors = self.adapter_errors.saturating_add(other.adapter_errors);
        self.max_depth = self.max_depth.max(other.max_depth);
        self.current_depth = self.current_depth.max(other.current_depth);
        self.peak_queue_bytes = self.peak_queue_bytes.max(other.peak_queue_bytes);
        self.current_queue_bytes = self.current_queue_bytes.max(other.current_queue_bytes);
        self.capacity = self.capacity.max(other.capacity);
        self.drops = self.drops.saturating_add(other.drops);
        self.pressure_events.merge(other.pressure_events);
        self.transport_bytes = self.transport_bytes.saturating_add(other.transport_bytes);
        self.transport_count = self.transport_count.saturating_add(other.transport_count);
        self.payload_clone_count = self
            .payload_clone_count
            .saturating_add(other.payload_clone_count);
        self.unique_handoffs = self.unique_handoffs.saturating_add(other.unique_handoffs);
        self.shared_handoffs = self.shared_handoffs.saturating_add(other.shared_handoffs);
        self.copied_bytes = self.copied_bytes.saturating_add(other.copied_bytes);
        self.gpu_uploads = self.gpu_uploads.saturating_add(other.gpu_uploads);
        self.gpu_downloads = self.gpu_downloads.saturating_add(other.gpu_downloads);
        if let Some(other_hist) = other.wait_histogram {
            let hist = self.wait_histogram.get_or_insert_with(Histogram::default);
            hist.merge(&other_hist);
        }
        if let Some(other_hist) = other.transport_apply_histogram {
            let hist = self
                .transport_apply_histogram
                .get_or_insert_with(Histogram::default);
            hist.merge(&other_hist);
        }
        if let Some(other_hist) = other.adapter_histogram {
            let hist = self
                .adapter_histogram
                .get_or_insert_with(Histogram::default);
            hist.merge(&other_hist);
        }
        if let Some(other_hist) = other.depth_histogram {
            let hist = self.depth_histogram.get_or_insert_with(Histogram::default);
            hist.merge(&other_hist);
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EdgePressureMetrics {
    pub total: u64,
    pub drop_incoming: u64,
    pub drop_oldest: u64,
    pub drop_newest: u64,
    pub backpressure: u64,
    pub error_overflow: u64,
    pub latest_replace: u64,
    pub coalesce_replace: u64,
}

impl EdgePressureMetrics {
    fn merge(&mut self, other: EdgePressureMetrics) {
        self.total = self.total.saturating_add(other.total);
        self.drop_incoming = self.drop_incoming.saturating_add(other.drop_incoming);
        self.drop_oldest = self.drop_oldest.saturating_add(other.drop_oldest);
        self.drop_newest = self.drop_newest.saturating_add(other.drop_newest);
        self.backpressure = self.backpressure.saturating_add(other.backpressure);
        self.error_overflow = self.error_overflow.saturating_add(other.error_overflow);
        self.latest_replace = self.latest_replace.saturating_add(other.latest_replace);
        self.coalesce_replace = self.coalesce_replace.saturating_add(other.coalesce_replace);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgePressureReason {
    DropIncoming,
    DropOldest,
    DropNewest,
    Backpressure,
    ErrorOverflow,
    LatestReplace,
    CoalesceReplace,
}

impl EdgePressureReason {
    pub fn as_str(self) -> &'static str {
        match self {
            EdgePressureReason::DropIncoming => "drop_incoming",
            EdgePressureReason::DropOldest => "drop_oldest",
            EdgePressureReason::DropNewest => "drop_newest",
            EdgePressureReason::Backpressure => "backpressure",
            EdgePressureReason::ErrorOverflow => "error_overflow",
            EdgePressureReason::LatestReplace => "latest_replace",
            EdgePressureReason::CoalesceReplace => "coalesce_replace",
        }
    }
}

impl NodeMetrics {
    pub fn record(&mut self, duration: Duration) {
        self.total_duration += duration;
        self.calls += 1;
    }

    pub fn record_handler(&mut self, duration: Duration) {
        self.handler_duration += duration;
    }

    pub fn record_perf(&mut self, sample: PerfSample) {
        let perf = self.perf.get_or_insert_with(NodePerfMetrics::default);
        perf.record(sample);
    }

    pub fn merge(&mut self, other: NodeMetrics) {
        self.total_duration += other.total_duration;
        self.calls += other.calls;
        self.handler_duration += other.handler_duration;
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
        for (name, value) in other.custom {
            self.record_custom(name, value);
        }
    }

    pub fn record_custom(&mut self, name: impl Into<String>, value: CustomMetricValue) {
        let name = name.into();
        self.custom
            .entry(name)
            .and_modify(|existing| existing.merge(value.clone()))
            .or_insert(value);
    }
}
