use crate::portable::Arc;
use crate::prelude::*;
use alloc::collections::BTreeMap;
use core::fmt;
use core::time::Duration;
use daedalus_core::platform::{Clock, Instant};
use daedalus_planner::GroupMetadata;
use daedalus_transport::IdStr;

mod adapter_text;
mod basics;
mod dense;
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

pub(crate) use adapter_text::AdapterText;
use adapter_text::AdapterTexts;
pub use basics::{Histogram, MetricsLevel, ProfileLevel, Profiler};
use dense::DenseSlots;
pub use frame::{EdgeAdapterClass, EdgeTickSample, FrameProbe, FrameTickSample};
pub(crate) use frame::{ProbeCount, ProbeTime, TickStart};
pub use frame_report::{EdgeOverheadStats, FrameOverheadReport, FrameOverheadWindow, FrameStat};
pub use lifecycle::{
    AdapterSteps, DataLifecycleEvent, DataLifecycleRecord, DataLifecycleStage, NodeFailure,
    TraceEvent,
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
    /// Requests for the engine's GPU context so far (`ExecutionContext::gpu` calls and the edge
    /// adapters' device uses), whether or not they opened the device. Cumulative for the
    /// executor's GPU source, not per run.
    #[serde(default)]
    pub gpu_accesses: u64,
    pub nodes_executed: usize,
    /// Node runs that replayed their last outputs instead of running (`#[node(reusable)]`): they
    /// are among `nodes_executed` too, since their outputs were published.
    #[serde(default)]
    pub nodes_reused: usize,
    pub cpu_segments: usize,
    pub gpu_segments: usize,
    pub gpu_fallbacks: usize,
    pub backpressure_events: usize,
    /// Node runs deferred to a later tick because a blocking output queue was full
    /// (`with_blocking_backpressure`).
    #[serde(default)]
    pub backpressure_deferrals: usize,
    pub warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<NodeFailure>,
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
    /// Node failures recovered under a [`NodeRecovery`](super::NodeRecovery) policy; `None`
    /// until a run recovers one (boxed so telemetry moved per tick stays small). Read it with
    /// [`Self::recovery`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<Box<super::RecoveryTelemetry>>,
    /// The nodes that have failed in this executor, with their backoff state (`with_backoff`);
    /// empty while none has. Read by field name, see [`NodeHealthReport`](super::NodeHealthReport).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub node_health: Vec<super::NodeHealthReport>,
    /// The executor's clock, which lifecycle timestamps and edge timings read.
    #[serde(skip)]
    clock: Clock,
    #[serde(skip)]
    lifecycle_origin: Option<Instant>,
    /// Edges recorded since the run began: [`Self::aggregate_group_ids`] keeps those and drops the
    /// other edges, which [`Self::reset_for_run`] zeroed.
    #[serde(skip)]
    edge_touched: DenseSlots<()>,
    #[serde(skip)]
    in_flight_node_transport_metrics: DenseSlots<InFlightNodeTransportMetrics>,
    /// The `Payload(<type key>)` texts lifecycle records share (see [`Self::payload_text`]). Kept
    /// across runs, and not part of a telemetry's value.
    #[serde(skip)]
    payload_texts: Option<Arc<PayloadTexts>>,
    /// The adapter path texts lifecycle records share (see [`Self::adapter_text`]). Kept across
    /// runs, and not part of a telemetry's value.
    #[serde(skip)]
    adapter_texts: AdapterTexts,
}

/// The lifecycle payload texts of one executor, by type key. Its telemetries share one table
/// (see [`ExecutionTelemetry::share_texts_with`]), so a text any run built is found by every later
/// run, serial or parallel: a hit reads the table and allocates nothing, and a miss writes it once
/// per type for the life of the executor. Profile and Trace only.
#[derive(Default)]
pub(crate) struct PayloadTexts(crate::sync::RwLock<BTreeMap<String, IdStr>>);

impl fmt::Debug for PayloadTexts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PayloadTexts")
            .field("types", &self.0.read().len())
            .finish()
    }
}

impl PartialEq for PayloadTexts {
    /// Caches never make two telemetries differ.
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for PayloadTexts {}

/// Lifecycle records one payload makes on one edge hop: the sites on the edge path, each at most
/// once per payload per hop (`queue/policy.rs` 4, `serial/edges.rs` 5, `serial_direct_slot.rs` 1).
/// Nodes record none of their own, so this is the whole lifecycle cost of one payload per edge.
#[cfg(feature = "threads")]
const LIFECYCLE_PER_EDGE_HOP: usize = 10;

/// Lifecycle and trace events a frame records, counted over all its workers. The pool keeps its
/// telemetry with room for the largest frame it has seen, so a frame that splits its nodes
/// differently across workers never grows any one worker's record vectors.
#[cfg(feature = "threads")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FrameEvents {
    pub(crate) lifecycle: usize,
    pub(crate) trace: usize,
}

#[cfg(feature = "threads")]
impl FrameEvents {
    /// The events `telemetry` holds (after a frame's workers are absorbed into it).
    pub(crate) fn of(telemetry: &ExecutionTelemetry) -> Self {
        Self {
            lifecycle: telemetry.data_lifecycle.len(),
            trace: telemetry.trace.as_ref().map_or(0, Vec::len),
        }
    }

    /// The bound for one frame of a plan of `nodes` nodes and `edges` edges at `level`, with one
    /// payload per edge per tick: lifecycle records only at `Profile` and above, one trace event
    /// per node only at `Trace` (the only site that records one, once per node run).
    pub(crate) fn plan(nodes: usize, edges: usize, level: MetricsLevel) -> Self {
        Self {
            lifecycle: if level.is_profile() {
                edges * LIFECYCLE_PER_EDGE_HOP
            } else {
                0
            },
            trace: if level.is_trace() { nodes } else { 0 },
        }
    }

    /// The larger of each count.
    pub(crate) fn raise(&mut self, other: Self) {
        self.lifecycle = self.lifecycle.max(other.lifecycle);
        self.trace = self.trace.max(other.trace);
    }
}

impl ExecutionTelemetry {
    /// The run's recoveries ([`Self::recovery`]), zero when none happened.
    pub fn recovery(&self) -> super::RecoveryTelemetry {
        self.recovery.as_deref().copied().unwrap_or_default()
    }

    pub(crate) fn recovery_mut(&mut self) -> &mut super::RecoveryTelemetry {
        self.recovery.get_or_insert_with(Default::default)
    }

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

    /// Read timestamps from `clock` from now on.
    pub(crate) fn set_clock(&mut self, clock: Clock) {
        self.clock = clock;
    }

    /// The clock timings are read from.
    pub fn clock(&self) -> &Clock {
        &self.clock
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
                port: event.port.as_ref().map(|port| port.as_str().to_string()),
                correlation_id: event.correlation_id,
                steps: event
                    .adapter_steps
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
                detail: event.detail.as_ref().map(ToString::to_string),
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
            backpressure_deferrals: self.backpressure_deferrals,
            recovery: self.recovery(),
            node_timing: self.node_metrics.to_btree_map(),
            edge_timing: self.edge_metrics.clone(),
            transport: self.edge_metrics.clone(),
            ownership,
            adapter_paths,
            capability_sources: Vec::new(),
            lifecycle: self.data_lifecycle.clone(),
            warnings: self.warnings.clone(),
            errors: self.errors.clone(),
            fallbacks,
            skipped_nodes,
            hardware_counters,
            ffi: self.ffi.clone(),
        }
    }

    pub fn reset_for_reuse(&mut self, level: MetricsLevel) {
        self.nodes_executed = 0;
        self.nodes_reused = 0;
        self.cpu_segments = 0;
        self.gpu_segments = 0;
        self.gpu_fallbacks = 0;
        self.backpressure_events = 0;
        self.backpressure_deferrals = 0;
        self.recovery = None;
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
        self.edge_touched.clear();
        if let Some(trace) = self.trace.as_mut() {
            trace.clear();
        }
        self.data_lifecycle.clear();
        // Usually empty: skip rebuilding them.
        if !self.demand.is_empty() {
            self.demand = crate::plan::DemandTelemetry::default();
        }
        if !self.ffi.is_empty() {
            self.ffi = FfiTelemetryReport::default();
        }
        self.in_flight_node_transport_metrics.clear();
    }

    /// [`Self::reset_for_reuse`] before a run, keeping the group and edge keys (and their tree
    /// nodes) with zeroed metrics, so a run on a recycled telemetry records into them without
    /// allocating. [`Self::aggregate_group_ids`] drops the groups and edges the run did not touch.
    pub(crate) fn reset_for_run(&mut self, level: MetricsLevel) {
        let mut groups = core::mem::take(&mut self.group_metrics);
        let mut edges = core::mem::take(&mut self.edge_metrics);
        self.reset_for_reuse(level);
        groups
            .values_mut()
            .for_each(|metrics| *metrics = NodeMetrics::default());
        edges
            .values_mut()
            .for_each(|metrics| *metrics = EdgeMetrics::default());
        self.group_metrics = groups;
        self.edge_metrics = edges;
    }

    /// The metrics of edge `edge_idx`, inserted empty if missing; the edge counts as touched by
    /// the current run.
    pub(crate) fn edge_entry(&mut self, edge_idx: usize) -> &mut EdgeMetrics {
        self.edge_touched.set(edge_idx, ());
        self.edge_metrics.entry(edge_idx).or_default()
    }

    pub fn merge(&mut self, other: ExecutionTelemetry) {
        self.nodes_executed += other.nodes_executed;
        self.nodes_reused += other.nodes_reused;
        self.cpu_segments += other.cpu_segments;
        self.gpu_segments += other.gpu_segments;
        self.gpu_fallbacks += other.gpu_fallbacks;
        self.backpressure_events += other.backpressure_events;
        self.backpressure_deferrals += other.backpressure_deferrals;
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
            self.edge_entry(edge).merge(metrics);
        }
        if let Some(other_trace) = other.trace {
            let trace = self.trace.get_or_insert_with(Vec::new);
            trace.extend(other_trace);
        }
        self.data_lifecycle.extend(other.data_lifecycle);
        self.ffi.merge(other.ffi);
        if let Some(other) = other.recovery {
            self.recovery_mut().merge(*other);
        }
    }

    /// The run's lifecycle origin (see [`Self::begin_pooled_run`]).
    #[cfg(feature = "threads")]
    pub(crate) fn lifecycle_origin(&self) -> Option<Instant> {
        self.lifecycle_origin
    }

    /// Start a run on a telemetry the worker pool keeps: its clock, metrics level and lifecycle
    /// origin are the run's, and what it already holds is kept (see [`Self::absorb`]).
    #[cfg(feature = "threads")]
    pub(crate) fn begin_pooled_run(
        &mut self,
        clock: &Clock,
        level: MetricsLevel,
        origin: Option<Instant>,
    ) {
        self.set_clock(clock.clone());
        self.metrics_level = if cfg!(feature = "metrics") {
            level
        } else {
            MetricsLevel::Off
        };
        self.lifecycle_origin = origin;
    }

    /// The lifecycle text `Payload(<type_key>)`, shared by the records that name the type: built
    /// the first time a type key is seen and cloned after that, so recording a payload's lifecycle
    /// allocates nothing once its type is warm.
    pub(crate) fn payload_text(&mut self, type_key: &str) -> IdStr {
        // A telemetry an executor shares has its table; one that has none builds its own (a
        // fresh telemetry allocates nothing until a text is built).
        let table = self
            .payload_texts
            .get_or_insert_with(|| Arc::new(PayloadTexts::default()));
        let hit = table.0.read().get(type_key).cloned();
        if let Some(text) = hit {
            return text;
        }
        let mut texts = table.0.write();
        // Another run may have built it between the read and the write.
        if let Some(text) = texts.get(type_key) {
            return text.clone();
        }
        let text = IdStr::new(format!("Payload({type_key})"));
        texts.insert(type_key.to_string(), text.clone());
        text
    }

    /// Use `texts` as this telemetry's payload text table: an executor's, shared by the telemetry
    /// it owns and the telemetry its pool keeps for its workers.
    pub(crate) fn share_texts_with(&mut self, texts: &Arc<PayloadTexts>) {
        self.payload_texts = Some(texts.clone());
    }

    /// The adapter path text of `transport`, the path of edge `edge_idx`: built the first time
    /// and cloned after that (see [`AdapterTexts`]).
    pub(crate) fn adapter_text(
        &mut self,
        edge_idx: usize,
        transport: &crate::plan::RuntimeEdgeTransport,
    ) -> AdapterText {
        self.adapter_texts.get(edge_idx, transport)
    }

    /// Build the adapter path texts of `transports` now, so the first lifecycle record of each
    /// adapter edge does not build one.
    #[cfg(feature = "threads")]
    pub(crate) fn reserve_adapter_texts(
        &mut self,
        transports: &[Option<crate::plan::RuntimeEdgeTransport>],
    ) {
        for (edge, transport) in transports.iter().enumerate() {
            if let Some(transport) = transport
                && !transport.adapter_steps.is_empty()
            {
                let _ = self.adapter_texts.get(edge, transport);
            }
        }
    }

    /// Size the per-index tables for a plan of `nodes` nodes and `edges` edges, so a telemetry
    /// that a worker takes later records into any node or edge without growing its tables.
    #[cfg(feature = "threads")]
    pub(crate) fn reserve_plan(&mut self, nodes: usize, edges: usize) {
        self.node_metrics.reserve_nodes(nodes);
        self.in_flight_node_transport_metrics.reserve(nodes);
        self.edge_touched.reserve(edges);
        self.reserve_edges(edges);
    }

    /// Copy into this telemetry the payload texts of `other` that it lacks.
    /// Room for `events` lifecycle records and trace events, so recording that many allocates
    /// nothing. Only grows: a vector already at or past its count is left as it is.
    #[cfg(feature = "threads")]
    pub(crate) fn reserve_events(&mut self, events: FrameEvents) {
        self.data_lifecycle
            .reserve(events.lifecycle.saturating_sub(self.data_lifecycle.len()));
        if events.trace > 0 {
            let trace = self.trace.get_or_insert_with(Vec::new);
            trace.reserve(events.trace.saturating_sub(trace.len()));
        }
    }

    /// Give each of `edges` edge indices an empty entry, so a worker that records into any of them
    /// allocates nothing: a telemetry the pool hands to another worker keeps its entries.
    #[cfg(feature = "threads")]
    pub(crate) fn reserve_edges(&mut self, edges: usize) {
        for edge in 0..edges {
            self.edge_metrics.entry(edge).or_default();
        }
    }

    /// Fold `other`, a worker's telemetry for the run in progress, into this run's telemetry, and
    /// zero `other` while keeping its storage. [`Self::merge`] folds an owned telemetry instead,
    /// and reads the same as this.
    #[cfg(feature = "threads")]
    pub(crate) fn absorb(&mut self, other: &mut ExecutionTelemetry) {
        self.nodes_executed += core::mem::take(&mut other.nodes_executed);
        self.nodes_reused += core::mem::take(&mut other.nodes_reused);
        self.cpu_segments += core::mem::take(&mut other.cpu_segments);
        self.gpu_segments += core::mem::take(&mut other.gpu_segments);
        self.gpu_fallbacks += core::mem::take(&mut other.gpu_fallbacks);
        self.backpressure_events += core::mem::take(&mut other.backpressure_events);
        self.backpressure_deferrals += core::mem::take(&mut other.backpressure_deferrals);
        self.warnings.append(&mut other.warnings);
        self.errors.append(&mut other.errors);
        self.graph_duration = self
            .graph_duration
            .max(core::mem::take(&mut other.graph_duration));
        self.unattributed_runtime_duration = self
            .unattributed_runtime_duration
            .saturating_add(core::mem::take(&mut other.unattributed_runtime_duration));
        self.metrics_level = self.metrics_level.max(other.metrics_level);
        for (node, metrics) in other.node_metrics.take_present() {
            self.node_metrics.entry(node).merge(metrics);
        }
        // Only the edges the worker recorded into; the rest are zeroed by `reset_for_run`.
        for (edge, metrics) in other.edge_metrics.iter_mut() {
            if other.edge_touched.get(*edge).is_some() {
                let metrics = core::mem::take(metrics);
                self.edge_entry(*edge).merge(metrics);
            }
        }
        other.edge_touched.clear();
        if let Some(trace) = other.trace.as_mut() {
            self.trace.get_or_insert_with(Vec::new).append(trace);
        }
        self.data_lifecycle.append(&mut other.data_lifecycle);
        self.ffi.merge(core::mem::take(&mut other.ffi));
        if let Some(recovery) = other.recovery.take() {
            self.recovery_mut().merge(*recovery);
        }
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
        let groups: Vec<Option<crate::portable::Arc<str>>> = nodes
            .iter()
            .map(|node| {
                GroupMetadata::from_node_metadata(&node.metadata)
                    .preferred_id()
                    .map(crate::portable::Arc::from)
            })
            .collect();
        self.aggregate_group_ids(&groups);
    }

    /// Merge each node's metrics into its group's (`groups`: each node's preferred group id, see
    /// `ExecutorCore::node_groups`). The runs call this with the table built once per executor.
    pub(crate) fn aggregate_group_ids(&mut self, groups: &[Option<crate::portable::Arc<str>>]) {
        for (idx, metrics) in self.node_metrics.iter() {
            let Some(Some(group)) = groups.get(idx) else {
                continue;
            };
            // A group already present takes the merge without a new key string.
            match self.group_metrics.get_mut(group.as_ref()) {
                Some(entry) => entry.merge(metrics.clone()),
                None => self
                    .group_metrics
                    .entry(group.to_string())
                    .or_default()
                    .merge(metrics.clone()),
            }
        }
        // Groups kept from a recycled telemetry whose nodes did not run this time.
        self.group_metrics
            .retain(|_, metrics| *metrics != NodeMetrics::default());
        // Likewise the edges no run touched (zeroed by `reset_for_run`).
        let touched = &self.edge_touched;
        self.edge_metrics.retain(|edge, metrics| {
            touched.get(*edge).is_some() || *metrics != EdgeMetrics::default()
        });
        self.edge_touched.clear();
    }
}

#[cfg(test)]
#[path = "telemetry_tests.rs"]
mod tests;
