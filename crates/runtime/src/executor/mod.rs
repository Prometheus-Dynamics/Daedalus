use crate::plan::{BackpressureStrategy, RuntimeEdge, RuntimeNode, RuntimePlan, RuntimeSegment};
use crate::state::{ExecutionContext, ResourceLifecycleEvent, StateStore};
use daedalus_planner::{GraphPatch, NodeRef, PatchReport};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

mod adaptive;
mod config;
mod config_target;
mod core;
mod direct_slot;
mod errors;
mod handler;
mod init;
mod owned;
mod owned_direct_host;
mod parallel;
mod patching;
mod payload;
pub mod queue;
mod schedule_compile;
mod serial;
mod serial_direct_slot;
mod telemetry;
mod telemetry_size;
mod workers;

pub(crate) use adaptive::AdaptiveState;
pub use adaptive::DEFAULT_DISPATCH_OVERHEAD;

pub(crate) use config::ExecutorRunConfig;
pub(crate) use config_target::ExecutorConfigTarget;
pub(crate) use core::ExecutorCore;
pub(crate) use direct_slot::{DirectSlot, DirectSlotAccess};
pub use errors::{ExecuteError, ExecutorBuildError, ExecutorMaskError, NodeError};
pub use handler::{DirectPayloadFn, NodeHandler};
pub(crate) use init::{ExecutorInit, build_executor_init};
pub use owned::OwnedExecutor;
pub(crate) use patching::apply_patch_to_const_inputs;
pub use payload::CorrelatedPayload;
pub use queue::EdgeStorage;
pub(crate) use schedule_compile::{
    CompiledSchedule, CompiledSegmentGraph, build_compiled_schedule, build_node_execution_metadata,
    direct_edge_set, direct_slots, is_host_bridge_node, resolve_parallel_workers,
};
pub use telemetry::{
    AdapterPathReport, CustomMetricValue, DataLifecycleEvent, DataLifecycleRecord,
    DataLifecycleStage, EdgeMetrics, EdgePressureMetrics, EdgePressureReason, ExecutionTelemetry,
    FfiAdapterTelemetry, FfiBackendTelemetry, FfiPackageTelemetry, FfiPayloadTelemetry,
    FfiTelemetryReport, FfiWorkerTelemetry, InternalTransferMetrics, MetricsLevel,
    NodeAllocationSpikeExplanation, NodeFailure, NodeMetrics, NodeMetricsMap, NodeResourceMetrics,
    OwnershipReport, ProfileLevel, Profiler, ResourceMetrics, TelemetryReport,
    TelemetryReportFilter,
};
pub use telemetry_size::{
    RuntimeDataSizeInspector, RuntimeDataSizeInspectors, estimate_payload_bytes,
    register_runtime_data_size_inspector,
};
pub(crate) use workers::WorkerPool;

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
    pub(crate) segments: &'a [RuntimeSegment],
    pub(crate) schedule_order: &'a [NodeRef],
    pub(crate) const_inputs: ConstInputStore,
    pub(crate) backpressure: BackpressureStrategy,
    pub(crate) handler: Arc<H>,
    pub(crate) core: ExecutorCore,
    /// Optional execution scope: when set, nodes with `false` are skipped.
    pub(crate) direct_slot_access: DirectSlotAccess,
    /// Measured costs behind `run_adaptive_in_place`.
    pub(crate) adaptive: AdaptiveState,
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

/// Text of a caught panic payload (`&str` or `String`), or a placeholder for other payloads.
pub fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
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

/// A node's const inputs as ready-made `Value` payloads: ticks hand out shared clones instead of
/// rebuilding a payload per port.
pub type NodeConstInputs = Vec<(crate::handles::PortId, daedalus_transport::Payload)>;
pub type ConstInputs = Vec<NodeConstInputs>;
pub type ConstInputStore = Arc<RwLock<ConstInputs>>;
type EdgeSpec = RuntimeEdge;

/// Const inputs keyed by pre-built port ids so ticks do not allocate port names or payloads.
pub(crate) fn node_const_inputs(node: &RuntimeNode) -> NodeConstInputs {
    node.const_inputs
        .iter()
        .map(|(port, value)| (port.into(), const_payload(value.clone())))
        .collect()
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
                plan.nodes.iter().map(node_const_inputs).collect(),
            )),
            backpressure: plan.backpressure.clone(),
            handler: Arc::new(handler),
            core,
            direct_slot_access: DirectSlotAccess::Shared,
            adaptive: AdaptiveState::default(),
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
        self.apply_active_nodes_mask(active_nodes)
    }

    pub fn try_set_active_edges_mask(
        &mut self,
        active_edges: Option<Arc<Vec<bool>>>,
    ) -> Result<(), ExecutorMaskError> {
        self.apply_active_edges_mask(active_edges)
    }

    pub fn try_set_active_direct_edges_mask(
        &mut self,
        active_direct_edges: Option<Arc<Vec<bool>>>,
    ) -> Result<(), ExecutorMaskError> {
        self.apply_active_direct_edges_mask(active_direct_edges)
    }

    pub fn with_selected_host_output_ports(mut self, ports: Option<Arc<HashSet<String>>>) -> Self {
        self.apply_selected_host_output_ports(ports);
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
                tracing::warn!(
                    target: "daedalus_runtime::executor",
                    error = %err,
                    "demand-driven sink selection failed"
                );
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
        self.apply_fail_fast(enabled);
        self
    }

    /// Provide a shared constant coercer registry (used by dynamic plugins).
    pub fn with_const_coercers(mut self, coercers: crate::io::ConstCoercerMap) -> Self {
        self.apply_const_coercers(coercers);
        self
    }

    /// Resolve handlers' generic pushes (`NodeIo::push_to`) through a registry's type index
    /// (`PluginRegistry::type_index`).
    pub fn with_type_index(mut self, types: crate::type_index::TypeIndex) -> Self {
        self.apply_type_index(types);
        self
    }

    pub fn with_data_size_inspectors(mut self, inspectors: RuntimeDataSizeInspectors) -> Self {
        self.apply_data_size_inspectors(inspectors);
        self
    }

    pub fn with_runtime_transport(mut self, transport: crate::transport::RuntimeTransport) -> Self {
        self.apply_runtime_transport(transport);
        self
    }

    pub fn with_capabilities(
        mut self,
        capabilities: crate::capabilities::CapabilityRegistry,
    ) -> Self {
        self.apply_capabilities(capabilities);
        self
    }

    /// Inject shared state store (optional).
    pub fn with_state(mut self, state: StateStore) -> Self {
        self.apply_state(state);
        self
    }

    pub fn apply_resource_lifecycle(&self, event: ResourceLifecycleEvent) {
        self.core.state.apply_resource_lifecycle(event)
    }

    pub fn on_memory_pressure(&self) {
        self.apply_resource_lifecycle(ResourceLifecycleEvent::MemoryPressure)
    }

    pub fn on_idle(&self) {
        self.apply_resource_lifecycle(ResourceLifecycleEvent::Idle)
    }

    pub fn shutdown_resources(&self) {
        self.apply_resource_lifecycle(ResourceLifecycleEvent::Stop)
    }

    /// Provide a GPU handle when available.
    #[cfg(feature = "gpu")]
    pub fn with_gpu(mut self, gpu: daedalus_gpu::GpuContextHandle) -> Self {
        self.apply_gpu(gpu);
        self
    }

    #[cfg(not(feature = "gpu"))]
    pub fn without_gpu(mut self) -> Self {
        self.clear_gpu();
        self
    }

    /// Override the number of parallel workers (default: available parallelism).
    pub fn with_pool_size(mut self, size: Option<usize>) -> Self {
        self.apply_pool_size(size);
        self
    }

    /// Initial estimate of parallel dispatch cost per segment for `run_adaptive_in_place`
    /// (default [`DEFAULT_DISPATCH_OVERHEAD`]); parallel frames refine it.
    pub fn with_adaptive_dispatch_overhead(mut self, overhead: Duration) -> Self {
        self.adaptive.set_dispatch_overhead(overhead);
        self
    }

    /// Start the parallel worker threads now instead of on the first parallel run.
    pub fn prewarm_worker_pool(&self) -> Result<(), ExecuteError> {
        WorkerPool::get_or_init(&self.core.worker_pool, self.parallel_workers()).map(drop)
    }

    fn parallel_workers(&self) -> usize {
        self.core
            .parallel_workers
            .min(self.schedule.host_deferred_graph.width)
    }

    pub fn with_metrics_level(mut self, level: MetricsLevel) -> Self {
        self.apply_metrics_level(level);
        self
    }

    pub fn with_runtime_debug_config(mut self, config: crate::config::RuntimeDebugConfig) -> Self {
        self.apply_runtime_debug_config(config);
        self
    }

    /// Apply a graph patch to this executor's constant inputs without rebuilding the graph.
    pub fn apply_patch(&self, patch: &GraphPatch) -> PatchReport {
        let mut guard = self.const_inputs.write();
        apply_patch_to_const_inputs(patch, &self.nodes, guard.as_mut_slice())
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
            core: self.core.snapshot(),
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

    /// Execute the runtime plan allowing independent segments to run in parallel.
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
        let mut adaptive = std::mem::take(&mut self.adaptive);
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

/// Parallel run of `exec`, or the serial path when its segments form one chain.
pub(crate) fn run_parallel_on<H>(
    exec: &mut Executor<'_, H>,
) -> Result<ExecutionTelemetry, ExecuteError>
where
    H: NodeHandler + Send + Sync + 'static,
{
    if exec.schedule.linear_segment_flow {
        return serial::run_with_boundaries(exec);
    }
    parallel::run(exec, None)
}

/// One adaptive frame on `exec` in the chosen mode, timed into `adaptive`.
pub(crate) fn run_adaptive_on<H>(
    exec: &mut Executor<'_, H>,
    adaptive: &mut AdaptiveState,
    parallel: bool,
    workers: usize,
) -> Result<ExecutionTelemetry, ExecuteError>
where
    H: NodeHandler + Send + Sync + 'static,
{
    if !adaptive::can_run_parallel(&exec.schedule) {
        return serial::run_with_boundaries(exec);
    }
    let schedule = exec.schedule.clone();
    let Some(costs) = adaptive.frame_costs() else {
        return serial::run_with_boundaries(exec);
    };
    let (result, wall) = if parallel {
        let start = daedalus_core::platform::Instant::now();
        let result = parallel::run(exec, Some(costs));
        (result, Some(start.elapsed()))
    } else {
        let costs = serial::SegmentCosts {
            segment_of: &schedule.segment_of,
            costs,
        };
        (serial::run_with_boundaries_timed(exec, Some(costs)), None)
    };
    if result.is_ok() {
        adaptive.observe(&schedule, workers, wall);
    }
    result
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
