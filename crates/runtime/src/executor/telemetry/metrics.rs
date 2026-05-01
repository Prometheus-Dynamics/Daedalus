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

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FfiTelemetryReport {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub packages: BTreeMap<String, FfiPackageTelemetry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub backends: BTreeMap<String, FfiBackendTelemetry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub workers: BTreeMap<String, FfiWorkerTelemetry>,
    #[serde(default)]
    pub payloads: FfiPayloadTelemetry,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub adapters: BTreeMap<String, FfiAdapterTelemetry>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FfiPackageTelemetry {
    pub package_id: String,
    #[serde(default)]
    pub validation_duration: Duration,
    #[serde(default)]
    pub load_duration: Duration,
    #[serde(default)]
    pub artifact_checks: u64,
    #[serde(default)]
    pub backend_resolutions: u64,
    #[serde(default)]
    pub bundle_path_resolutions: u64,
    #[serde(default)]
    pub install_failures: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FfiBackendTelemetry {
    pub backend_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(default)]
    pub runner_starts: u64,
    #[serde(default)]
    pub runner_reuses: u64,
    #[serde(default)]
    pub runner_restarts: u64,
    #[serde(default)]
    pub runner_failures: u64,
    #[serde(default)]
    pub runner_not_ready: u64,
    #[serde(default)]
    pub runner_shutdowns: u64,
    #[serde(default)]
    pub runner_pruned: u64,
    #[serde(default)]
    pub invokes: u64,
    #[serde(default)]
    pub invoke_duration: Duration,
    #[serde(default)]
    pub checkout_wait_duration: Duration,
    #[serde(default)]
    pub symbol_lookup_duration: Duration,
    #[serde(default)]
    pub dynamic_library_load_duration: Duration,
    #[serde(default)]
    pub abi_call_duration: Duration,
    #[serde(default)]
    pub bytes_sent: u64,
    #[serde(default)]
    pub bytes_received: u64,
    #[serde(default)]
    pub pointer_length_payload_calls: u64,
    #[serde(default)]
    pub abi_error_codes: u64,
    #[serde(default)]
    pub panic_boundary_errors: u64,
    #[serde(default)]
    pub idle_runners: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FfiWorkerTelemetry {
    pub worker_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_health: Option<String>,
    #[serde(default)]
    pub handshakes: u64,
    #[serde(default)]
    pub health_checks: u64,
    #[serde(default)]
    pub shutdowns: u64,
    #[serde(default)]
    pub unsupported_limit_errors: u64,
    #[serde(default)]
    pub timeout_failures: u64,
    #[serde(default)]
    pub handshake_duration: Duration,
    #[serde(default)]
    pub request_bytes: u64,
    #[serde(default)]
    pub response_bytes: u64,
    #[serde(default)]
    pub encode_duration: Duration,
    #[serde(default)]
    pub decode_duration: Duration,
    #[serde(default)]
    pub malformed_responses: u64,
    #[serde(default)]
    pub stderr_events: u64,
    #[serde(default)]
    pub typed_errors: u64,
    #[serde(default)]
    pub raw_io_events: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FfiPayloadTelemetry {
    #[serde(default)]
    pub handles_created: u64,
    #[serde(default)]
    pub handles_resolved: u64,
    #[serde(default)]
    pub borrows: u64,
    #[serde(default)]
    pub releases: u64,
    #[serde(default)]
    pub active_leases: u64,
    #[serde(default)]
    pub expired_leases: u64,
    #[serde(default)]
    pub zero_copy_hits: u64,
    #[serde(default)]
    pub shared_reference_hits: u64,
    #[serde(default)]
    pub cow_materializations: u64,
    #[serde(default)]
    pub mutable_in_place_hits: u64,
    #[serde(default)]
    pub owned_moves: u64,
    #[serde(default)]
    pub copied_bytes_estimate: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub by_access_mode: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub by_residency: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub by_layout: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FfiAdapterTelemetry {
    pub adapter_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_type_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_type_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(default)]
    pub calls: u64,
    #[serde(default)]
    pub duration: Duration,
    #[serde(default)]
    pub failures: u64,
}

impl FfiTelemetryReport {
    pub fn merge(&mut self, other: FfiTelemetryReport) {
        for (key, package) in other.packages {
            self.packages.entry(key).or_default().merge(package);
        }
        for (key, backend) in other.backends {
            self.backends.entry(key).or_default().merge(backend);
        }
        for (key, worker) in other.workers {
            self.workers.entry(key).or_default().merge(worker);
        }
        self.payloads.merge(other.payloads);
        for (key, adapter) in other.adapters {
            self.adapters.entry(key).or_default().merge(adapter);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.packages.is_empty()
            && self.backends.is_empty()
            && self.workers.is_empty()
            && self.payloads.is_empty()
            && self.adapters.is_empty()
    }
}

impl FfiPackageTelemetry {
    pub fn merge(&mut self, other: FfiPackageTelemetry) {
        if self.package_id.is_empty() {
            self.package_id = other.package_id;
        }
        self.validation_duration += other.validation_duration;
        self.load_duration += other.load_duration;
        self.artifact_checks = self.artifact_checks.saturating_add(other.artifact_checks);
        self.backend_resolutions = self
            .backend_resolutions
            .saturating_add(other.backend_resolutions);
        self.bundle_path_resolutions = self
            .bundle_path_resolutions
            .saturating_add(other.bundle_path_resolutions);
        self.install_failures = self.install_failures.saturating_add(other.install_failures);
    }
}

impl FfiBackendTelemetry {
    pub fn merge(&mut self, other: FfiBackendTelemetry) {
        if self.backend_key.is_empty() {
            self.backend_key = other.backend_key;
        }
        self.backend_kind = self.backend_kind.take().or(other.backend_kind);
        self.language = self.language.take().or(other.language);
        self.package_id = self.package_id.take().or(other.package_id);
        self.node_id = self.node_id.take().or(other.node_id);
        self.runner_starts = self.runner_starts.saturating_add(other.runner_starts);
        self.runner_reuses = self.runner_reuses.saturating_add(other.runner_reuses);
        self.runner_restarts = self.runner_restarts.saturating_add(other.runner_restarts);
        self.runner_failures = self.runner_failures.saturating_add(other.runner_failures);
        self.runner_not_ready = self.runner_not_ready.saturating_add(other.runner_not_ready);
        self.runner_shutdowns = self.runner_shutdowns.saturating_add(other.runner_shutdowns);
        self.runner_pruned = self.runner_pruned.saturating_add(other.runner_pruned);
        self.invokes = self.invokes.saturating_add(other.invokes);
        self.invoke_duration += other.invoke_duration;
        self.checkout_wait_duration += other.checkout_wait_duration;
        self.symbol_lookup_duration += other.symbol_lookup_duration;
        self.dynamic_library_load_duration += other.dynamic_library_load_duration;
        self.abi_call_duration += other.abi_call_duration;
        self.bytes_sent = self.bytes_sent.saturating_add(other.bytes_sent);
        self.bytes_received = self.bytes_received.saturating_add(other.bytes_received);
        self.pointer_length_payload_calls = self
            .pointer_length_payload_calls
            .saturating_add(other.pointer_length_payload_calls);
        self.abi_error_codes = self.abi_error_codes.saturating_add(other.abi_error_codes);
        self.panic_boundary_errors = self
            .panic_boundary_errors
            .saturating_add(other.panic_boundary_errors);
        self.idle_runners = self.idle_runners.max(other.idle_runners);
        self.capacity = self.capacity.max(other.capacity);
    }
}

impl FfiWorkerTelemetry {
    pub fn merge(&mut self, other: FfiWorkerTelemetry) {
        if self.worker_id.is_empty() {
            self.worker_id = other.worker_id;
        }
        self.backend_key = self.backend_key.take().or(other.backend_key);
        self.language = self.language.take().or(other.language);
        self.last_health = other.last_health.or(self.last_health.take());
        self.handshakes = self.handshakes.saturating_add(other.handshakes);
        self.health_checks = self.health_checks.saturating_add(other.health_checks);
        self.shutdowns = self.shutdowns.saturating_add(other.shutdowns);
        self.unsupported_limit_errors = self
            .unsupported_limit_errors
            .saturating_add(other.unsupported_limit_errors);
        self.timeout_failures = self.timeout_failures.saturating_add(other.timeout_failures);
        self.handshake_duration += other.handshake_duration;
        self.request_bytes = self.request_bytes.saturating_add(other.request_bytes);
        self.response_bytes = self.response_bytes.saturating_add(other.response_bytes);
        self.encode_duration += other.encode_duration;
        self.decode_duration += other.decode_duration;
        self.malformed_responses = self
            .malformed_responses
            .saturating_add(other.malformed_responses);
        self.stderr_events = self.stderr_events.saturating_add(other.stderr_events);
        self.typed_errors = self.typed_errors.saturating_add(other.typed_errors);
        self.raw_io_events = self.raw_io_events.saturating_add(other.raw_io_events);
    }
}

impl FfiPayloadTelemetry {
    pub fn merge(&mut self, other: FfiPayloadTelemetry) {
        self.handles_created = self.handles_created.saturating_add(other.handles_created);
        self.handles_resolved = self.handles_resolved.saturating_add(other.handles_resolved);
        self.borrows = self.borrows.saturating_add(other.borrows);
        self.releases = self.releases.saturating_add(other.releases);
        self.active_leases = self.active_leases.max(other.active_leases);
        self.expired_leases = self.expired_leases.saturating_add(other.expired_leases);
        self.zero_copy_hits = self.zero_copy_hits.saturating_add(other.zero_copy_hits);
        self.shared_reference_hits = self
            .shared_reference_hits
            .saturating_add(other.shared_reference_hits);
        self.cow_materializations = self
            .cow_materializations
            .saturating_add(other.cow_materializations);
        self.mutable_in_place_hits = self
            .mutable_in_place_hits
            .saturating_add(other.mutable_in_place_hits);
        self.owned_moves = self.owned_moves.saturating_add(other.owned_moves);
        self.copied_bytes_estimate = self
            .copied_bytes_estimate
            .saturating_add(other.copied_bytes_estimate);
        merge_counter_map(&mut self.by_access_mode, other.by_access_mode);
        merge_counter_map(&mut self.by_residency, other.by_residency);
        merge_counter_map(&mut self.by_layout, other.by_layout);
    }

    pub fn is_empty(&self) -> bool {
        self.handles_created == 0
            && self.handles_resolved == 0
            && self.borrows == 0
            && self.releases == 0
            && self.active_leases == 0
            && self.expired_leases == 0
            && self.zero_copy_hits == 0
            && self.shared_reference_hits == 0
            && self.cow_materializations == 0
            && self.mutable_in_place_hits == 0
            && self.owned_moves == 0
            && self.copied_bytes_estimate == 0
            && self.by_access_mode.is_empty()
            && self.by_residency.is_empty()
            && self.by_layout.is_empty()
    }
}

impl FfiAdapterTelemetry {
    pub fn merge(&mut self, other: FfiAdapterTelemetry) {
        if self.adapter_id.is_empty() {
            self.adapter_id = other.adapter_id;
        }
        self.source_type_key = self.source_type_key.take().or(other.source_type_key);
        self.target_type_key = self.target_type_key.take().or(other.target_type_key);
        self.origin = self.origin.take().or(other.origin);
        self.calls = self.calls.saturating_add(other.calls);
        self.duration += other.duration;
        self.failures = self.failures.saturating_add(other.failures);
    }
}

fn merge_counter_map(left: &mut BTreeMap<String, u64>, right: BTreeMap<String, u64>) {
    for (key, value) in right {
        left.entry(key)
            .and_modify(|existing| *existing = existing.saturating_add(value))
            .or_insert(value);
    }
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
