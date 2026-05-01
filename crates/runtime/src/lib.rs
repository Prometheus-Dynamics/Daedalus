//! Runtime execution layer for planner-produced graphs.
//!
//! This crate owns runtime plans, executor paths, handler dispatch, host bridge
//! queues, streaming workers, state/resources, transport execution, and
//! telemetry.

pub mod capabilities;
pub mod config;
pub mod debug;
pub mod executor;
pub mod fanin;
pub mod graph_builder;
pub mod handler_registry;
pub mod handles;
pub mod host_bridge;
pub mod io;
mod perf;
mod plan;
#[cfg(feature = "plugins")]
pub mod plugins;
mod scheduler;
pub mod snapshot;
pub mod state;
mod state_error;
pub mod stream;
pub mod transport;
pub use daedalus_transport as transport_types;

/// Apply a plugin prefix to a node id without duplicating overlapping segments.
///
/// Prefixes already present at the start of the id are not duplicated.
pub fn apply_node_prefix(prefix: &str, id: &str) -> String {
    let prefix = prefix.trim_matches(':').trim();
    let id = id.trim_matches(':').trim();

    if prefix.is_empty() {
        return id.to_string();
    }
    if id.is_empty() {
        return prefix.to_string();
    }

    let prefix_parts: Vec<&str> = prefix
        .split(':')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect();
    let id_parts: Vec<&str> = id
        .split(':')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect();

    if prefix_parts.is_empty() {
        return id.to_string();
    }
    if id_parts.is_empty() {
        return prefix.to_string();
    }

    let max_overlap = std::cmp::min(prefix_parts.len(), id_parts.len());
    let mut overlap = 0usize;
    while overlap < max_overlap && prefix_parts[overlap] == id_parts[overlap] {
        overlap += 1;
    }

    if overlap == prefix_parts.len() {
        // Already fully prefixed.
        return id.to_string();
    }

    let mut out: Vec<&str> =
        Vec::with_capacity(prefix_parts.len() + id_parts.len().saturating_sub(overlap));
    out.extend_from_slice(&prefix_parts);
    out.extend_from_slice(&id_parts[overlap..]);
    out.join(":")
}

pub use config::*;
pub use daedalus_core::metadata::{EMBEDDED_GRAPH_KEY, EMBEDDED_HOST_KEY, NODE_OVERLOADS_KEY};
pub use executor::{
    AdapterPathReport, CustomMetricValue, DataLifecycleEvent, DataLifecycleStage, DirectPayloadFn,
    EdgePressureMetrics, EdgePressureReason, ExecuteError, ExecutionTelemetry, Executor,
    ExecutorMaskError, FfiAdapterTelemetry, FfiBackendTelemetry, FfiPackageTelemetry,
    FfiPayloadTelemetry, FfiTelemetryReport, FfiWorkerTelemetry, InternalTransferMetrics,
    MetricsLevel, NodeAllocationSpikeExplanation, NodeError, NodeHandler, NodeMetrics,
    NodeResourceMetrics, OwnedExecutor, OwnershipReport, ProfileLevel, Profiler, ResourceMetrics,
    TelemetryReport, TelemetryReportFilter, estimate_payload_bytes,
    register_runtime_data_size_inspector,
};
pub use fanin::FanIn;
pub use handles::{
    CapabilityId, FeatureFlag, HostAlias, NodeAlias, NodeHandle, NodeHandleId, PortHandle, PortId,
};
pub use host_bridge::{
    DEFAULT_HOST_BRIDGE_EVENT_LIMIT, HOST_BRIDGE_META_KEY, HostBridgeConfig, HostBridgeHandle,
    HostBridgeManager, bridge_handler,
};
pub use io::{DEFAULT_OUTPUT_PORT, NodeIo, TypedInputResolution, TypedInputResolutionKind};
pub use plan::{
    BackpressureStrategy, DemandError, DemandSlice, DemandSliceEntry, DemandTelemetry,
    NODE_EXECUTION_KIND_META_KEY, NodeExecutionKind, RuntimeBranchExplanation, RuntimeEdge,
    RuntimeEdgeExplanation, RuntimeEdgeHandoff, RuntimeEdgePolicy, RuntimeNode,
    RuntimeNodeExplanation, RuntimePlan, RuntimePlanError, RuntimePlanExplanation, RuntimeSegment,
    RuntimeSink,
};
pub use scheduler::{SchedulerConfig, build_runtime};
pub use state::{
    ExecutionContext, ManagedByteBuffer, ManagedResource, NodeResourceSnapshot, ResourceClass,
    ResourceLifecycleEvent, ResourceUsage, RuntimeResources, StateStore,
};
pub use state_error::StateError;
pub use stream::{
    DEFAULT_STREAM_IDLE_SLEEP, GraphInput, GraphOutput, InputStats, OutputStats,
    OutputSubscription, SharedStreamGraph, StreamExecutionMode, StreamGraph,
    StreamGraphDiagnostics, StreamGraphState, StreamGraphWorker, StreamTelemetrySummary,
    StreamWorkerConfig, StreamWorkerDiagnostics, StreamWorkerState, StreamWorkerStopError,
};
pub use transport::RuntimeTransport;
