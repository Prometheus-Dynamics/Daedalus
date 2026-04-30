use crate::plan::{BackpressureStrategy, RuntimeEdge, RuntimeNode, RuntimePlan, RuntimeSegment};
use crate::state::{ExecutionContext, ResourceLifecycleEvent, StateError, StateStore};
use daedalus_planner::{GraphPatch, NodeRef, PatchReport};
use std::collections::{BTreeMap, HashSet};
#[cfg(feature = "executor-pool")]
use std::sync::OnceLock;
use std::sync::{Arc, RwLock};
use std::time::Duration;

mod config;
mod core;
mod direct_slot;
mod errors;
mod handler;
mod init;
mod owned;
mod owned_direct_host;
#[cfg(not(feature = "executor-pool"))]
mod parallel;
mod patching;
mod payload;
#[cfg(feature = "executor-pool")]
mod pool;
pub mod queue;
mod schedule;
mod schedule_compile;
mod serial;
mod serial_direct_slot;
mod telemetry;
mod telemetry_size;

pub(crate) use config::ExecutorRunConfig;
pub(crate) use core::ExecutorCore;
pub(crate) use direct_slot::{DirectSlot, DirectSlotAccess};
pub use errors::{ExecuteError, ExecutorBuildError, ExecutorMaskError, NodeError};
pub use handler::{DirectPayloadFn, NodeHandler};
pub(crate) use init::{ExecutorInit, build_executor_init};
pub use owned::OwnedExecutor;
pub(crate) use patching::apply_patch_to_const_inputs;
pub use payload::{CorrelatedPayload, next_correlation_id};
pub use queue::EdgeStorage;
pub(crate) use schedule_compile::{
    CompiledSchedule, CompiledSegmentGraph, build_compiled_schedule, build_node_execution_metadata,
    direct_edge_set, direct_slots, is_host_bridge_node,
};
#[cfg(feature = "executor-pool")]
pub(crate) use schedule_compile::{compiled_worker_pool, resolve_pool_workers};
pub use telemetry::{
    AdapterPathReport, CustomMetricValue, DataLifecycleEvent, DataLifecycleRecord,
    DataLifecycleStage, EdgeMetrics, EdgePressureMetrics, EdgePressureReason, ExecutionTelemetry,
    InternalTransferMetrics, MetricsLevel, NodeAllocationSpikeExplanation, NodeFailure,
    NodeMetrics, NodeResourceMetrics, OwnershipReport, ProfileLevel, Profiler, ResourceMetrics,
    TelemetryReport, TelemetryReportFilter,
};
pub use telemetry_size::{
    RuntimeDataSizeInspector, RuntimeDataSizeInspectors, estimate_payload_bytes,
    register_runtime_data_size_inspector,
};

#[derive(Clone)]
pub struct DirectHostRoute {
    input_edge: usize,
    output_edge: usize,
    active_direct_edges: Arc<Vec<bool>>,
    single_node: Option<DirectHostSingleNodeRoute>,
}

#[derive(Clone)]
struct DirectHostSingleNodeRoute {
    node: RuntimeNode,
    node_idx: usize,
    ctx: ExecutionContext,
    input_port: String,
    output_port: String,
    direct_payload: Option<DirectPayloadFn>,
}

/// Runtime executor for planner-generated runtime plans.
///
/// ```no_run
/// use daedalus_runtime::executor::Executor;
/// use daedalus_planner::{ExecutionPlan, Graph};
/// use daedalus_runtime::RuntimePlan;
///
/// fn handler(
///     _node: &daedalus_runtime::RuntimeNode,
///     _ctx: &daedalus_runtime::state::ExecutionContext,
///     _io: &mut daedalus_runtime::io::NodeIo,
/// ) -> Result<(), daedalus_runtime::executor::NodeError> {
///     Ok(())
/// }
///
/// let plan = RuntimePlan::from_execution(&ExecutionPlan::new(Graph::default(), vec![]));
/// let _exec = Executor::new(&plan, handler);
/// ```
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
    pub(crate) segments: &'a [RuntimeSegment],
    pub(crate) schedule_order: &'a [NodeRef],
    pub(crate) const_inputs: ConstInputStore,
    pub(crate) backpressure: BackpressureStrategy,
    pub(crate) handler: Arc<H>,
    pub(crate) core: ExecutorCore,
    /// Optional execution scope: when set, nodes with `false` are skipped.
    pub(crate) direct_slot_access: DirectSlotAccess,
}

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

pub(crate) fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "handler panicked with non-string payload".to_string()
    }
}

#[cfg(feature = "gpu")]
type MaybeGpu = Option<daedalus_gpu::GpuContextHandle>;
#[cfg(not(feature = "gpu"))]
type MaybeGpu = Option<()>;

pub type NodeConstInputs = Vec<(String, daedalus_data::model::Value)>;
pub type ConstInputs = Vec<NodeConstInputs>;
pub type ConstInputStore = Arc<RwLock<ConstInputs>>;
type EdgeSpec = RuntimeEdge;
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
                if let Ok(mut q) = queue.lock() {
                    if let Some(edge) = edges.get(idx) {
                        q.ensure_policy(edge.policy());
                    }
                    q.clear();
                    metrics.set_current_bytes(0);
                }
            }
            #[cfg(feature = "lockfree-queues")]
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

pub(crate) fn normalize_runtime_nodes(
    nodes: &[RuntimeNode],
) -> Result<Vec<RuntimeNode>, ExecutorBuildError> {
    let mut nodes_vec = nodes.to_vec();
    for node in &mut nodes_vec {
        if node.stable_id == 0 {
            node.stable_id = daedalus_core::stable_id::stable_id128("node", &node.id);
        }
    }

    let mut seen: std::collections::HashMap<u128, &str> = std::collections::HashMap::new();
    for node in &nodes_vec {
        if let Some(previous) = seen.insert(node.stable_id, node.id.as_str())
            && previous != node.id
        {
            return Err(ExecutorBuildError::StableIdCollision {
                previous: previous.to_string(),
                current: node.id.clone(),
                stable_id: node.stable_id,
            });
        }
    }
    Ok(nodes_vec)
}

impl<'a, H: NodeHandler> Executor<'a, H> {
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
                plan.nodes.iter().map(|n| n.const_inputs.clone()).collect(),
            )),
            backpressure: plan.backpressure.clone(),
            handler: Arc::new(handler),
            core,
            direct_slot_access: DirectSlotAccess::Shared,
        })
    }

    /// Restrict execution to a subset of nodes (by index).
    ///
    /// `active_nodes.len()` must equal `plan.nodes.len()`.
    pub fn with_active_nodes(mut self, active_nodes: Vec<bool>) -> Self {
        self.try_set_active_nodes_mask(Some(Arc::new(active_nodes)))
            .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"));
        self
    }

    pub fn with_active_nodes_mask(mut self, active_nodes: Option<Arc<Vec<bool>>>) -> Self {
        self.try_set_active_nodes_mask(active_nodes)
            .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"));
        self
    }

    pub fn with_active_edges_mask(mut self, active_edges: Option<Arc<Vec<bool>>>) -> Self {
        self.try_set_active_edges_mask(active_edges)
            .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"));
        self
    }

    pub fn with_active_direct_edges_mask(
        mut self,
        active_direct_edges: Option<Arc<Vec<bool>>>,
    ) -> Self {
        self.try_set_active_direct_edges_mask(active_direct_edges)
            .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"));
        self
    }

    pub fn try_with_active_nodes(
        mut self,
        active_nodes: Vec<bool>,
    ) -> Result<Self, ExecutorMaskError> {
        self.try_set_active_nodes_mask(Some(Arc::new(active_nodes)))?;
        Ok(self)
    }

    pub fn try_with_active_nodes_mask(
        mut self,
        active_nodes: Option<Arc<Vec<bool>>>,
    ) -> Result<Self, ExecutorMaskError> {
        self.try_set_active_nodes_mask(active_nodes)?;
        Ok(self)
    }

    pub fn try_with_active_edges_mask(
        mut self,
        active_edges: Option<Arc<Vec<bool>>>,
    ) -> Result<Self, ExecutorMaskError> {
        self.try_set_active_edges_mask(active_edges)?;
        Ok(self)
    }

    pub fn try_with_active_direct_edges_mask(
        mut self,
        active_direct_edges: Option<Arc<Vec<bool>>>,
    ) -> Result<Self, ExecutorMaskError> {
        self.try_set_active_direct_edges_mask(active_direct_edges)?;
        Ok(self)
    }

    pub fn try_set_active_nodes_mask(
        &mut self,
        active_nodes: Option<Arc<Vec<bool>>>,
    ) -> Result<(), ExecutorMaskError> {
        let expected = self.nodes.len();
        self.core
            .run_config
            .set_active_nodes_mask(active_nodes, expected)
    }

    pub fn try_set_active_edges_mask(
        &mut self,
        active_edges: Option<Arc<Vec<bool>>>,
    ) -> Result<(), ExecutorMaskError> {
        let expected = self.edges.len();
        self.core
            .run_config
            .set_active_edges_mask(active_edges, expected)
    }

    pub fn try_set_active_direct_edges_mask(
        &mut self,
        active_direct_edges: Option<Arc<Vec<bool>>>,
    ) -> Result<(), ExecutorMaskError> {
        let expected = self.edges.len();
        self.core
            .run_config
            .set_active_direct_edges_mask(active_direct_edges, expected)
    }

    pub fn with_selected_host_output_ports(mut self, ports: Option<Arc<HashSet<String>>>) -> Self {
        self.core.run_config.set_selected_host_output_ports(ports);
        self
    }

    /// Enable demand-driven execution by selecting a set of sink nodes/ports and computing the
    /// upstream closure.
    ///
    /// This is the core "responsiveness" knob: it prevents unrelated slow branches from dragging
    /// down outputs the UI is currently watching.
    pub fn with_demand_sinks(mut self, sinks: Vec<crate::plan::RuntimeSink>) -> Self {
        match crate::plan::active_nodes_mask_for_sinks(self.nodes.as_ref(), self.edges, &sinks) {
            Ok(mask) => {
                self.core.run_config.active_nodes = Some(Arc::new(mask));
            }
            Err(err) => {
                // If the selector can't be resolved, keep the graph running rather than silently
                // disabling everything. Callers that need strictness can validate up-front.
                tracing::warn!("daedalus-runtime: demand-driven sink selection failed: {err}");
            }
        }
        self
    }

    /// Control how executor errors affect the current run.
    ///
    /// When enabled, serial execution returns the first node error immediately. Parallel execution
    /// stops scheduling additional ready segments after the first segment error, then waits for
    /// already-running scoped segments to return before propagating that error. When disabled, the
    /// executor records segment errors in telemetry and continues scheduling remaining ready work.
    pub fn with_fail_fast(mut self, enabled: bool) -> Self {
        self.core.run_config.set_fail_fast(enabled);
        self
    }

    /// Provide a shared constant coercer registry (used by dynamic plugins).
    pub fn with_const_coercers(mut self, coercers: crate::io::ConstCoercerMap) -> Self {
        self.core.const_coercers = Some(coercers);
        self
    }

    pub fn with_data_size_inspectors(mut self, inspectors: RuntimeDataSizeInspectors) -> Self {
        self.core.data_size_inspectors = inspectors;
        self
    }

    pub fn with_runtime_transport(mut self, transport: crate::transport::RuntimeTransport) -> Self {
        self.core.runtime_transport = Some(Arc::new(transport));
        self
    }

    pub fn with_capabilities(
        mut self,
        capabilities: crate::capabilities::CapabilityRegistry,
    ) -> Self {
        self.core.capabilities = Arc::new(capabilities);
        self
    }

    /// Inject shared state store (optional).
    pub fn with_state(mut self, state: StateStore) -> Self {
        self.core.state = state;
        self
    }

    pub fn apply_resource_lifecycle(
        &self,
        event: ResourceLifecycleEvent,
    ) -> Result<(), StateError> {
        self.core.state.apply_resource_lifecycle(event)
    }

    pub fn on_memory_pressure(&self) -> Result<(), StateError> {
        self.apply_resource_lifecycle(ResourceLifecycleEvent::MemoryPressure)
    }

    pub fn on_idle(&self) -> Result<(), StateError> {
        self.apply_resource_lifecycle(ResourceLifecycleEvent::Idle)
    }

    pub fn shutdown_resources(&self) -> Result<(), StateError> {
        self.apply_resource_lifecycle(ResourceLifecycleEvent::Stop)
    }

    /// Provide a GPU handle when available.
    #[cfg(feature = "gpu")]
    pub fn with_gpu(mut self, gpu: daedalus_gpu::GpuContextHandle) -> Self {
        self.core.gpu_available = true;
        self.core.gpu = Some(gpu);
        self
    }

    #[cfg(not(feature = "gpu"))]
    pub fn without_gpu(mut self) -> Self {
        self.core.gpu_available = false;
        self
    }

    /// Override pool size when using the pool-based parallel executor.
    pub fn with_pool_size(mut self, size: Option<usize>) -> Self {
        self.core.run_config.set_pool_size(size);
        #[cfg(feature = "executor-pool")]
        {
            self.core.pool_workers = resolve_pool_workers(size, self.segments.len());
            self.core.worker_pool = Arc::new(OnceLock::new());
        }
        self
    }

    #[cfg(feature = "executor-pool")]
    pub fn prewarm_worker_pool(&self) -> Result<(), ExecuteError> {
        let _ = compiled_worker_pool(&self.core.worker_pool, self.core.pool_workers)?;
        Ok(())
    }

    pub fn with_metrics_level(mut self, level: MetricsLevel) -> Self {
        self.core.run_config.metrics_level = level;
        self.core.telemetry.metrics_level = level;
        self
    }

    pub fn with_runtime_debug_config(mut self, config: crate::config::RuntimeDebugConfig) -> Self {
        let pool_size = self.core.run_config.set_runtime_debug_config(config);
        self.with_pool_size(pool_size)
    }

    /// Apply a graph patch to this executor's constant inputs without rebuilding the graph.
    pub fn apply_patch(&self, patch: &GraphPatch) -> PatchReport {
        let mut guard = self
            .const_inputs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        apply_patch_to_const_inputs(patch, &self.nodes, guard.as_mut_slice())
    }

    /// Attach a host bridge manager to enable implicit host I/O nodes.
    pub fn with_host_bridges(mut self, mgr: crate::host_bridge::HostBridgeManager) -> Self {
        self.core.host_bridges = Some(mgr);
        self
    }

    /// Reset per-run state (queues, telemetry, warnings) so this executor can be reused.
    pub fn reset(&mut self) {
        let metrics_level = self.core.run_config.metrics_level;
        self.core.telemetry.reset_for_reuse(metrics_level);
        if let Ok(mut warnings) = self.core.warnings_seen.lock() {
            warnings.clear();
        }

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
            core: self.core.snapshot(),
            direct_slot_access,
        }
    }

    pub(crate) fn segment_snapshot(&self, segment_idx: usize) -> (Self, Vec<NodeRef>) {
        let order = self
            .segments
            .get(segment_idx)
            .map(|segment| segment.nodes.clone())
            .unwrap_or_default();
        let exec = self.snapshot_with_direct_slot_access(DirectSlotAccess::Shared);
        (exec, order)
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

    /// Execute the runtime plan allowing independent segments to run in parallel.
    ///
    /// With fail-fast enabled, this stops scheduling new ready segments after the first segment
    /// error. Scoped threads or Rayon tasks already running are still allowed to finish before the
    /// error is returned.
    pub fn run_parallel(self) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
        if self.schedule.linear_segment_flow {
            return serial::run_fused_linear(self);
        }
        #[cfg(feature = "executor-pool")]
        {
            pool::run(self)
        }
        #[cfg(not(feature = "executor-pool"))]
        {
            parallel::run(self)
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
        let exec = self.snapshot();
        let result = self.run_parallel_from_snapshot(exec);
        serial::drain_host_outputs(self);
        if result.is_err() {
            self.reset();
        }
        result
    }

    fn run_parallel_from_snapshot(
        &self,
        exec: Executor<'a, H>,
    ) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
        if exec.schedule.linear_segment_flow {
            return serial::run_fused_linear(exec);
        }
        #[cfg(feature = "executor-pool")]
        {
            pool::run(exec)
        }
        #[cfg(not(feature = "executor-pool"))]
        {
            parallel::run(exec)
        }
    }
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

pub(crate) fn thread_cpu_time() -> Option<Duration> {
    #[cfg(target_os = "linux")]
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

/// Build adjacency maps of incoming/outgoing edge indices per node.
pub(crate) fn edge_maps(edges: &[EdgeSpec]) -> (Vec<Vec<usize>>, Vec<Vec<usize>>) {
    let mut incoming: Vec<Vec<usize>> = Vec::new();
    let mut outgoing: Vec<Vec<usize>> = Vec::new();
    let grow = |v: &mut Vec<Vec<usize>>, idx: usize| {
        while v.len() <= idx {
            v.push(Vec::new());
        }
    };
    for (idx, edge) in edges.iter().enumerate() {
        let f = edge.from().0;
        let t = edge.to().0;
        grow(&mut incoming, f.max(t));
        grow(&mut outgoing, f.max(t));
        outgoing[f].push(idx);
        incoming[t].push(idx);
    }
    (incoming, outgoing)
}

#[cfg(test)]
mod tests;
