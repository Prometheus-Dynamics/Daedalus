use crate::plan::{BackpressureStrategy, RuntimeEdge, RuntimeNode, RuntimePlan, RuntimeSegment};
use crate::portable::Arc;
use crate::prelude::*;
use crate::state::{ExecutionContext, ResourceLifecycleEvent, StateStore};
use crate::sync::RwLock;
use ::core::time::Duration;
use alloc::collections::BTreeMap;
use daedalus_planner::{GraphPatch, NodeRef, PatchReport};

mod adaptive;
mod config;
mod config_target;
mod core;
mod direct_slot;
mod errors;
mod handler;
mod init;
mod node_policy;
mod owned;
mod owned_direct_host;
#[cfg(feature = "threads")]
mod parallel;
mod patching;
mod payload;
pub mod queue;
mod schedule_compile;
mod serial;
mod serial_direct_slot;
mod telemetry;
mod telemetry_size;
#[cfg(feature = "threads")]
mod workers;

pub use adaptive::DEFAULT_DISPATCH_OVERHEAD;
pub(crate) use adaptive::{AdaptiveState, run_adaptive_on};

pub(crate) use config::ExecutorRunConfig;
pub(crate) use config_target::{ExecutorConfigTarget, executor_config_api};
pub(crate) use core::{CoreRef, ExecutorCore};
pub(crate) use direct_slot::{DirectSlot, DirectSlotAccess};
pub use errors::{ExecuteError, ExecutorBuildError, ExecutorMaskError, NodeError};
pub use handler::{DirectPayloadFn, NodeHandler};
pub(crate) use init::{ExecutorInit, build_executor_init};
pub(crate) use node_policy::NodePolicies;
pub use node_policy::{
    NODE_PRIORITY_META_KEY, NODE_RECOVERY_META_KEY, NodeRecovery, RecoveryTelemetry,
    UnknownNodeError,
};
pub use owned::OwnedExecutor;
pub(crate) use patching::apply_patch_to_const_inputs;
pub use payload::CorrelatedPayload;
pub use queue::EdgeStorage;
#[cfg(feature = "threads")]
pub(crate) use schedule_compile::CompiledSegmentGraph;
pub(crate) use schedule_compile::{
    CompiledSchedule, FusionTables, ScheduleSource, build_compiled_schedule,
    build_node_execution_metadata, direct_edge_set, direct_slots, is_host_bridge_node,
    resolve_parallel_workers,
};
pub use telemetry::{
    AdapterPathReport, CustomMetricValue, DataLifecycleEvent, DataLifecycleRecord,
    DataLifecycleStage, EdgeAdapterClass, EdgeMetrics, EdgeOverheadStats, EdgePressureMetrics,
    EdgePressureReason, EdgeTickSample, ExecutionTelemetry, FfiAdapterTelemetry,
    FfiBackendTelemetry, FfiPackageTelemetry, FfiPayloadTelemetry, FfiTelemetryReport,
    FfiWorkerTelemetry, FrameOverheadReport, FrameOverheadWindow, FrameProbe, FrameStat,
    FrameTickSample, InternalTransferMetrics, MetricsLevel, NodeAllocationSpikeExplanation,
    NodeFailure, NodeMetrics, NodeMetricsMap, NodeResourceMetrics, OwnershipReport, ProfileLevel,
    Profiler, ResourceMetrics, TelemetryReport, TelemetryReportFilter,
};
pub(crate) use telemetry::{ProbeCount, ProbeTime, TickStart};
pub use telemetry_size::{
    RuntimeDataSizeInspector, RuntimeDataSizeInspectors, estimate_payload_bytes,
    register_runtime_data_size_inspector,
};
#[cfg(feature = "threads")]
pub(crate) use workers::WorkerPool;

#[derive(Clone)]
pub struct DirectHostRoute {
    input_edge: usize,
    output_edge: usize,
    active_direct_edges: Arc<Vec<bool>>,
    single_node: Option<DirectHostSingleNodeRoute>,
}

impl DirectHostRoute {
    /// Whether the route is one node between the host ports, run without the scheduler.
    pub fn is_single_node(&self) -> bool {
        self.single_node.is_some()
    }
}

#[derive(Clone)]
struct DirectHostSingleNodeRoute {
    node: RuntimeNode,
    node_idx: usize,
    ctx: ExecutionContext,
    input_port: crate::handles::PortId,
    output_port: crate::handles::PortId,
    direct_payload: Option<DirectPayloadFn>,
}

/// Runtime executor for planner-generated runtime plans.
///
pub struct Executor<'a, H: NodeHandler> {
    pub(crate) nodes: Arc<[RuntimeNode]>,
    pub(crate) edges: &'a [EdgeSpec],
    pub(crate) edge_transports: &'a [Option<crate::plan::RuntimeEdgeTransport>],
    pub(crate) incoming_edges: Arc<Vec<Vec<usize>>>,
    pub(crate) outgoing_edges: Arc<Vec<Vec<usize>>>,
    pub(crate) schedule: Arc<CompiledSchedule>,
    #[cfg(feature = "gpu")]
    pub(crate) gpu_entries: &'a [usize],
    #[cfg(feature = "gpu")]
    pub(crate) gpu_exits: &'a [usize],
    #[cfg(feature = "gpu")]
    pub(crate) gpu_entry_set: Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")]
    pub(crate) gpu_exit_set: Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")]
    pub(crate) data_edges: Arc<HashSet<usize>>,
    /// The plan's segments (the schedule's merge fused units).
    pub(crate) segments: &'a [RuntimeSegment],
    pub(crate) schedule_order: &'a [NodeRef],
    pub(crate) const_inputs: ConstInputStore,
    pub(crate) backpressure: BackpressureStrategy,
    pub(crate) handler: Arc<H>,
    pub(crate) core: CoreRef<'a>,
    /// Optional execution scope: when set, nodes with `false` are skipped.
    pub(crate) direct_slot_access: DirectSlotAccess,
    /// Measured costs behind `run_adaptive_in_place`.
    pub(crate) adaptive: AdaptiveState,
}

#[cfg(feature = "threads")]
pub(crate) fn segment_failure(segment_idx: usize, error: &ExecuteError) -> NodeFailure {
    match error {
        ExecuteError::HandlerFailed { node, error } => NodeFailure {
            node_idx: usize::MAX,
            node_id: format!("segment_{segment_idx}:{node}"),
            code: error.code().to_string(),
            message: error.to_string(),
        },
        ExecuteError::HandlerPanicked { node, message } => NodeFailure {
            node_idx: usize::MAX,
            node_id: format!("segment_{segment_idx}:{node}"),
            code: error.code().to_string(),
            message: message.clone(),
        },
        ExecuteError::GpuUnavailable { segment } => NodeFailure {
            node_idx: usize::MAX,
            node_id: format!("segment_{segment_idx}"),
            code: error.code().to_string(),
            message: format!("gpu unavailable for segment {segment:?}"),
        },
    }
}

/// Text of a caught panic payload (`&str` or `String`), or a placeholder for other payloads.
pub fn panic_message(payload: &(dyn ::core::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

#[cfg(feature = "gpu")]
type MaybeGpu = Option<daedalus_gpu::GpuContextHandle>;
#[cfg(not(feature = "gpu"))]
type MaybeGpu = Option<()>;

/// A node's const inputs as ready-made `Value` payloads, shared with each call's `NodeIo`.
pub use crate::io::NodeConstInputs;
/// Each node's const inputs; a patch replaces (copy-on-write) the node's list.
pub type ConstInputs = Vec<Arc<NodeConstInputs>>;
pub type ConstInputStore = Arc<RwLock<ConstInputs>>;
type EdgeSpec = RuntimeEdge;

/// Const inputs keyed by pre-built port ids so ticks do not allocate port names or payloads.
pub(crate) fn node_const_inputs(node: &RuntimeNode) -> Arc<NodeConstInputs> {
    Arc::new(
        node.const_inputs
            .iter()
            .map(|(port, value)| {
                let payload = CorrelatedPayload::from_edge(const_payload(value.clone()));
                (port.into(), payload)
            })
            .collect(),
    )
}

/// Node `node_idx`'s const inputs (one `Arc` clone; `None` without any), which every call
/// (scheduled or direct) sees after its edge inputs.
pub(crate) fn node_consts(
    store: &ConstInputStore,
    node_idx: usize,
) -> Option<Arc<NodeConstInputs>> {
    store
        .read()
        .get(node_idx)
        .filter(|consts| !consts.is_empty())
        .cloned()
}

/// The payload a const input delivers on every tick.
pub(crate) fn const_payload(value: daedalus_data::model::Value) -> daedalus_transport::Payload {
    daedalus_transport::Payload::owned("value", value)
}
type NodeMetadataStore = Arc<Vec<Arc<BTreeMap<String, daedalus_data::model::Value>>>>;

pub(crate) fn reset_run_storage(
    edges: &[RuntimeEdge],
    queues: &[EdgeStorage],
    direct_slots: &[DirectSlot],
    active_edges: Option<&[bool]>,
) {
    for (idx, storage) in queues.iter().enumerate() {
        if active_edges
            .and_then(|mask| mask.get(idx).copied())
            .is_some_and(|active| !active)
        {
            continue;
        }
        match storage {
            EdgeStorage::Locked { queue, metrics } => {
                let mut q = queue.lock();
                if let Some(edge) = edges.get(idx) {
                    q.set_policy(&edge.policy().pressure);
                }
                q.clear();
                metrics.set_current_bytes(0);
            }
            #[cfg(all(feature = "lockfree-queues", target_has_atomic = "ptr"))]
            EdgeStorage::BoundedLf { queue, metrics } => {
                while queue.pop().is_some() {}
                metrics.set_current_bytes(0);
            }
        }
    }
    for (idx, slot) in direct_slots.iter().enumerate() {
        if active_edges
            .and_then(|mask| mask.get(idx).copied())
            .is_some_and(|active| !active)
        {
            continue;
        }
        slot.clear();
    }
}

impl<'a, H: NodeHandler> Executor<'a, H> {
    executor_config_api!();

    /// Build an executor from a runtime plan and handler.
    ///
    /// # Panics
    ///
    /// Panics when the runtime plan has colliding stable node ids. Use
    /// [`Self::try_new`] to receive a typed build error instead.
    pub fn new(plan: &'a RuntimePlan, handler: H) -> Self {
        Self::try_new(plan, handler).unwrap_or_else(|err| panic!("daedalus-runtime: {err}"))
    }

    /// Build an executor and report invalid runtime-plan state without panicking.
    pub fn try_new(plan: &'a RuntimePlan, handler: H) -> Result<Self, ExecutorBuildError> {
        let init = build_executor_init(plan)?;
        let core = ExecutorCore::from_init(&init, &plan.graph_metadata);
        Ok(Self {
            nodes: init.nodes,
            edges: &plan.edges,
            edge_transports: &plan.edge_transports,
            incoming_edges: init.incoming_edges,
            outgoing_edges: init.outgoing_edges,
            schedule: init.schedule,
            #[cfg(feature = "gpu")]
            gpu_entries: &plan.gpu_entries,
            #[cfg(feature = "gpu")]
            gpu_exits: &plan.gpu_exits,
            #[cfg(feature = "gpu")]
            gpu_entry_set: Arc::new(plan.gpu_entries.iter().cloned().collect()),
            #[cfg(feature = "gpu")]
            gpu_exit_set: Arc::new(plan.gpu_exits.iter().cloned().collect()),
            #[cfg(feature = "gpu")]
            data_edges: init.data_edges,
            segments: &plan.segments,
            schedule_order: &plan.schedule_order,
            const_inputs: Arc::new(RwLock::new(
                plan.nodes.iter().map(node_const_inputs).collect(),
            )),
            backpressure: plan.backpressure.clone(),
            handler: Arc::new(handler),
            core: CoreRef::Owned(core),
            direct_slot_access: DirectSlotAccess::Shared,
            adaptive: AdaptiveState::default(),
        })
    }

    /// Enable demand-driven execution by selecting a set of sink nodes/ports and computing the
    /// upstream closure.
    ///
    /// This is the core "responsiveness" knob: it prevents unrelated slow branches from dragging
    /// down outputs the UI is currently watching.
    #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
    pub fn with_demand_sinks(mut self, sinks: Vec<crate::plan::RuntimeSink>) -> Self {
        match crate::plan::active_nodes_mask_for_sinks(self.nodes.as_ref(), self.edges, &sinks) {
            Ok(mask) => {
                self.core.run_config.active_nodes = Some(Arc::new(mask));
            }
            Err(err) => {
                // If the selector can't be resolved, keep the graph running rather than silently
                // disabling everything. Callers that need strictness can validate up-front.
                crate::trace::warn!(
                    target: "daedalus_runtime::executor",
                    error = %err,
                    "demand-driven sink selection failed"
                );
            }
        }
        self
    }

    /// Run chains of single-consumer CPU nodes as fused units (the default) or, with `false`,
    /// every node and edge on its own (see "Node fusion" in `docs/development.md`).
    pub fn with_node_fusion(mut self, enabled: bool) -> Self {
        self.schedule = Arc::new(build_compiled_schedule(
            ScheduleSource {
                nodes: &self.nodes,
                edges: self.edges,
                transports: self.edge_transports,
                segments: self.segments,
                schedule_order: self.schedule_order,
                backpressure: &self.backpressure,
            },
            enabled,
        ));
        self.adaptive.forget_segments();
        let pool_size = self.core.run_config.pool_size;
        self.apply_pool_size(pool_size);
        self
    }

    /// Start the parallel worker threads now instead of on the first parallel run.
    #[cfg(feature = "threads")]
    pub fn prewarm_worker_pool(&self) -> Result<(), ExecuteError> {
        WorkerPool::get_or_init(&self.core.worker_pool, self.parallel_workers()).map(drop)
    }

    fn parallel_workers(&self) -> usize {
        self.core
            .parallel_workers
            .min(self.schedule.host_deferred_graph.width)
    }

    /// Attach a host bridge manager to enable implicit host I/O nodes.
    /// Attach host bridges; each host-bridge node is resolved to its bridge once, here.
    pub fn with_host_bridges(mut self, mgr: crate::host_bridge::HostBridgeManager) -> Self {
        self.core.host_nodes = serial::resolve_host_nodes(
            &mgr,
            &self.nodes,
            self.edges,
            &self.schedule.host_nodes,
            &self.incoming_edges,
            &self.outgoing_edges,
        );
        self
    }

    /// Reset per-run state (queues, telemetry, warnings) so this executor can be reused.
    pub fn reset(&mut self) {
        let metrics_level = self.core.run_config.metrics_level;
        self.core.telemetry.reset_for_reuse(metrics_level);
        self.core.warnings_seen.lock().clear();

        reset_run_storage(
            self.edges,
            &self.core.queues,
            &self.core.direct_slots,
            self.core
                .run_config
                .active_edges
                .as_deref()
                .map(Vec::as_slice),
        );
    }

    /// Build a lightweight snapshot for a single run without re-planning.
    fn snapshot(&self) -> Self {
        self.snapshot_with_direct_slot_access(self.direct_slot_access)
    }

    fn snapshot_with_direct_slot_access(&self, direct_slot_access: DirectSlotAccess) -> Self {
        Self {
            nodes: self.nodes.clone(),
            edges: self.edges,
            edge_transports: self.edge_transports,
            incoming_edges: self.incoming_edges.clone(),
            outgoing_edges: self.outgoing_edges.clone(),
            schedule: self.schedule.clone(),
            #[cfg(feature = "gpu")]
            gpu_entries: self.gpu_entries,
            #[cfg(feature = "gpu")]
            gpu_exits: self.gpu_exits,
            #[cfg(feature = "gpu")]
            gpu_entry_set: self.gpu_entry_set.clone(),
            #[cfg(feature = "gpu")]
            gpu_exit_set: self.gpu_exit_set.clone(),
            #[cfg(feature = "gpu")]
            data_edges: self.data_edges.clone(),
            segments: self.segments,
            schedule_order: self.schedule_order,
            const_inputs: self.const_inputs.clone(),
            backpressure: self.backpressure.clone(),
            handler: self.handler.clone(),
            core: CoreRef::Owned(self.core.snapshot()),
            direct_slot_access,
            adaptive: AdaptiveState::default(),
        }
    }

    /// Execute the runtime plan serially in segment order.
    pub fn run(self) -> Result<ExecutionTelemetry, ExecuteError> {
        serial::run(self)
    }

    /// Execute the runtime plan serially without rebuilding the executor.
    pub fn run_in_place(&mut self) -> Result<ExecutionTelemetry, ExecuteError> {
        self.reset();
        let exec = self.snapshot();
        let result = serial::run(exec);
        serial::drain_host_outputs(self);
        if result.is_err() {
            self.reset();
        }
        result
    }

    /// Execute the runtime plan allowing independent segments to run in parallel (serially
    /// without the `threads` feature).
    ///
    /// With fail-fast enabled, this stops scheduling new ready segments after the first segment
    /// error. Segments already running still finish before the error is returned.
    pub fn run_parallel(mut self) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
        run_parallel_on(&mut self)
    }

    /// Execute once, in parallel when the segment graph has independent work (several ready
    /// segments or fan-out), serially otherwise. A single run has nothing measured to go on; for
    /// repeated runs use [`Self::run_adaptive_in_place`], which decides from measured costs.
    pub fn run_adaptive(self) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
        if adaptive::can_run_parallel(&self.schedule) {
            self.run_parallel()
        } else {
            serial::run(self)
        }
    }

    /// Execute the runtime plan in parallel without rebuilding the executor.
    ///
    /// Fail-fast semantics match [`Self::run_parallel`].
    pub fn run_parallel_in_place(&mut self) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
        self.reset();
        let mut exec = self.snapshot_with_direct_slot_access(DirectSlotAccess::Shared);
        let result = run_parallel_on(&mut exec);
        self.finish_in_place(exec, result)
    }

    /// Execute without rebuilding, serially or in parallel as the measured costs of earlier runs
    /// suggest: parallel only when the work that could overlap outweighs the dispatch overhead.
    /// Graphs of cheap nodes run serially.
    pub fn run_adaptive_in_place(&mut self) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
        self.reset();
        let mut adaptive = ::core::mem::take(&mut self.adaptive);
        let workers = self.parallel_workers();
        let parallel = adaptive.choose(&self.schedule, &self.nodes, workers);
        let access = if parallel {
            DirectSlotAccess::Shared
        } else {
            self.direct_slot_access
        };
        let mut exec = self.snapshot_with_direct_slot_access(access);
        let result = run_adaptive_on(&mut exec, &mut adaptive, parallel, workers);
        self.adaptive = adaptive;
        self.finish_in_place(exec, result)
    }

    fn finish_in_place(
        &mut self,
        mut exec: Executor<'a, H>,
        result: Result<ExecutionTelemetry, ExecuteError>,
    ) -> Result<ExecutionTelemetry, ExecuteError> {
        serial::drain_host_outputs(&mut exec);
        drop(exec);
        if result.is_err() {
            self.reset();
        }
        result
    }
}

/// Parallel run of `exec`, or the serial path when its segments form one chain (or without
/// `threads`).
pub(crate) fn run_parallel_on<H>(
    exec: &mut Executor<'_, H>,
) -> Result<ExecutionTelemetry, ExecuteError>
where
    H: NodeHandler + Send + Sync + 'static,
{
    #[cfg(feature = "threads")]
    if !exec.schedule.linear_segment_flow {
        return parallel::run(exec, None).map(|(telemetry, _)| telemetry);
    }
    serial::run_with_boundaries(exec)
}

#[cfg(feature = "gpu")]
fn collect_data_edges(nodes: &[RuntimeNode], edges: &[EdgeSpec]) -> HashSet<usize> {
    let _ = nodes;
    let _ = edges;
    // `io.host_output` carries typed transport payloads directly. Forcing host-output edges
    // through device materialization eagerly clones CPU images on GPU-enabled builds, which turns
    // host publication into a hidden hot-path tax.
    HashSet::new()
}

/// Attribute this thread's allocations to the runtime until the guard drops (`alloc-probe`;
/// nothing otherwise).
#[inline]
pub(crate) fn runtime_alloc_scope() -> impl Sized {
    #[cfg(feature = "alloc-probe")]
    {
        crate::alloc_probe::enter(crate::alloc_probe::AllocScope::Runtime)
    }
}

/// Attribute this thread's allocations to a node handler until the guard drops (`alloc-probe`;
/// nothing otherwise).
#[inline]
pub(crate) fn node_alloc_scope() -> impl Sized {
    #[cfg(feature = "alloc-probe")]
    {
        crate::alloc_probe::enter(crate::alloc_probe::AllocScope::Node)
    }
}

pub(crate) fn thread_cpu_time() -> Option<Duration> {
    #[cfg(all(feature = "std", target_os = "linux"))]
    unsafe {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        if libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) == 0 {
            return Some(Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32));
        }
    }
    None
}

#[cfg(test)]
mod tests;
