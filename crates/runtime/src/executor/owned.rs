use super::ExecutorConfigTarget;
use super::{
    AdaptiveState, CompiledSchedule, ConstInputStore, DirectSlotAccess, ExecuteError,
    ExecutionTelemetry, Executor, ExecutorBuildError, ExecutorCore, ExecutorMaskError,
    MetricsLevel, NodeHandler, RuntimeDataSizeInspectors, apply_patch_to_const_inputs,
    build_executor_init, node_const_inputs, reset_run_storage, run_adaptive_on, run_parallel_on,
    serial,
};
use crate::plan::{BackpressureStrategy, RuntimeEdge, RuntimeNode, RuntimePlan, RuntimeSegment};
use crate::state::{ResourceLifecycleEvent, StateStore};
use crate::sync::RwLock;
use daedalus_planner::{GraphPatch, NodeRef, PatchReport};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// Owned executor that can be reused across runs without leaking the plan.
pub struct OwnedExecutor<H: NodeHandler> {
    pub(crate) nodes: Arc<[RuntimeNode]>,
    pub(crate) edges: Arc<Vec<RuntimeEdge>>,
    pub(crate) edge_transports: Arc<Vec<Option<crate::plan::RuntimeEdgeTransport>>>,
    pub(crate) incoming_edges: Arc<Vec<Vec<usize>>>,
    pub(crate) outgoing_edges: Arc<Vec<Vec<usize>>>,
    pub(crate) schedule: Arc<CompiledSchedule>,
    #[cfg(feature = "gpu")]
    pub(crate) gpu_entries: Arc<Vec<usize>>,
    #[cfg(feature = "gpu")]
    pub(crate) gpu_exits: Arc<Vec<usize>>,
    #[cfg(feature = "gpu")]
    pub(crate) gpu_entry_set: Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")]
    pub(crate) gpu_exit_set: Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")]
    pub(crate) data_edges: Arc<HashSet<usize>>,
    pub(crate) segments: Arc<Vec<RuntimeSegment>>,
    pub(crate) schedule_order: Arc<Vec<NodeRef>>,
    pub(crate) const_inputs: ConstInputStore,
    pub(crate) backpressure: BackpressureStrategy,
    pub(crate) handler: Arc<H>,
    pub(crate) core: ExecutorCore,
    pub(super) storage_needs_reset: bool,
    /// Measured costs behind `run_adaptive_in_place`.
    pub(super) adaptive: AdaptiveState,
}

impl<H: NodeHandler> ExecutorConfigTarget for OwnedExecutor<H> {
    fn core_mut(&mut self) -> &mut ExecutorCore {
        &mut self.core
    }

    fn nodes_len(&self) -> usize {
        self.nodes.len()
    }

    fn edges_len(&self) -> usize {
        self.edges.len()
    }

    fn segments_len(&self) -> usize {
        self.segments.len()
    }
}

impl<H: NodeHandler> OwnedExecutor<H> {
    /// Build an owned executor from a runtime plan and handler.
    ///
    /// # Panics
    ///
    /// Panics when the runtime plan has colliding stable node ids. Use
    /// [`Self::try_new`] to receive a typed build error instead.
    pub fn new(plan: Arc<RuntimePlan>, handler: H) -> Self {
        Self::try_new(plan, handler).unwrap_or_else(|err| panic!("daedalus-runtime: {err}"))
    }

    /// Build an owned executor and report invalid runtime-plan state without panicking.
    pub fn try_new(plan: Arc<RuntimePlan>, handler: H) -> Result<Self, ExecutorBuildError> {
        let init = build_executor_init(&plan)?;
        let core = ExecutorCore::from_init(&init, &plan.graph_metadata);
        Ok(Self {
            nodes: init.nodes,
            edges: Arc::new(plan.edges.clone()),
            edge_transports: Arc::new(plan.edge_transports.clone()),
            incoming_edges: init.incoming_edges,
            outgoing_edges: init.outgoing_edges,
            schedule: init.schedule,
            #[cfg(feature = "gpu")]
            gpu_entries: Arc::new(plan.gpu_entries.clone()),
            #[cfg(feature = "gpu")]
            gpu_exits: Arc::new(plan.gpu_exits.clone()),
            #[cfg(feature = "gpu")]
            gpu_entry_set: Arc::new(plan.gpu_entries.iter().cloned().collect()),
            #[cfg(feature = "gpu")]
            gpu_exit_set: Arc::new(plan.gpu_exits.iter().cloned().collect()),
            #[cfg(feature = "gpu")]
            data_edges: init.data_edges,
            segments: Arc::new(plan.segments.clone()),
            schedule_order: Arc::new(plan.schedule_order.clone()),
            const_inputs: Arc::new(RwLock::new(
                plan.nodes.iter().map(node_const_inputs).collect(),
            )),
            backpressure: plan.backpressure.clone(),
            handler: Arc::new(handler),
            core,
            storage_needs_reset: true,
            adaptive: AdaptiveState::default(),
        })
    }

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

    pub fn with_selected_host_output_ports(mut self, ports: Option<Arc<HashSet<String>>>) -> Self {
        self.apply_selected_host_output_ports(ports);
        self
    }

    /// Enable demand-driven execution by selecting a set of sink nodes/ports and computing the
    /// upstream closure.
    pub fn with_demand_sinks(mut self, sinks: Vec<crate::plan::RuntimeSink>) -> Self {
        match crate::plan::active_nodes_mask_for_sinks(
            self.nodes.as_ref(),
            self.edges.as_slice(),
            &sinks,
        ) {
            Ok(mask) => {
                self.core.run_config.active_nodes = Some(Arc::new(mask));
            }
            Err(err) => {
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

    pub fn with_pool_size(mut self, size: Option<usize>) -> Self {
        self.apply_pool_size(size);
        self
    }

    /// Initial estimate of parallel dispatch cost per segment for `run_adaptive_in_place`
    /// (default [`super::DEFAULT_DISPATCH_OVERHEAD`]); parallel frames refine it.
    pub fn with_adaptive_dispatch_overhead(mut self, overhead: Duration) -> Self {
        self.adaptive.set_dispatch_overhead(overhead);
        self
    }

    /// Start the parallel worker threads now instead of on the first parallel run.
    #[cfg(feature = "threads")]
    pub fn prewarm_worker_pool(&self) -> Result<(), ExecuteError> {
        super::WorkerPool::get_or_init(&self.core.worker_pool, self.parallel_workers()).map(drop)
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

    /// Attach host bridges; each host-bridge node is resolved to its bridge once, here.
    pub fn with_host_bridges(mut self, mgr: crate::host_bridge::HostBridgeManager) -> Self {
        self.core.host_nodes = serial::resolve_host_nodes(
            &mgr,
            &self.nodes,
            &self.edges,
            &self.schedule.host_nodes,
            &self.incoming_edges,
            &self.outgoing_edges,
        );
        self
    }

    pub fn reset(&mut self) {
        let metrics_level = self.core.run_config.metrics_level;
        self.core.telemetry.reset_for_reuse(metrics_level);
        self.core.warnings_seen.lock().clear();
        self.reset_storage();
        self.storage_needs_reset = false;
    }

    pub(super) fn reset_for_run(&mut self) {
        let metrics_level = self.core.run_config.metrics_level;
        self.core.telemetry.reset_for_reuse(metrics_level);
        self.core.warnings_seen.lock().clear();
        if self.storage_needs_reset {
            self.reset_storage();
            self.storage_needs_reset = false;
        }
    }

    fn reset_storage(&mut self) {
        reset_run_storage(
            &self.edges,
            &self.core.queues,
            &self.core.direct_slots,
            self.core
                .run_config
                .active_edges
                .as_deref()
                .map(Vec::as_slice),
        );
    }

    pub fn set_active_nodes_mask(&mut self, active_nodes: Option<Arc<Vec<bool>>>) {
        self.try_set_active_nodes_mask(active_nodes)
            .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"));
    }

    pub fn set_active_edges_mask(&mut self, active_edges: Option<Arc<Vec<bool>>>) {
        self.try_set_active_edges_mask(active_edges)
            .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"));
    }

    pub fn set_active_direct_edges_mask(&mut self, active_direct_edges: Option<Arc<Vec<bool>>>) {
        self.try_set_active_direct_edges_mask(active_direct_edges)
            .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"));
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

    pub fn set_selected_host_output_ports(&mut self, ports: Option<Arc<HashSet<String>>>) {
        self.apply_selected_host_output_ports(ports);
    }

    pub(super) fn snapshot<'a>(&'a self, direct_slot_access: DirectSlotAccess) -> Executor<'a, H> {
        Executor {
            nodes: self.nodes.clone(),
            edges: self.edges.as_slice(),
            edge_transports: self.edge_transports.as_slice(),
            incoming_edges: self.incoming_edges.clone(),
            outgoing_edges: self.outgoing_edges.clone(),
            schedule: self.schedule.clone(),
            #[cfg(feature = "gpu")]
            gpu_entries: self.gpu_entries.as_slice(),
            #[cfg(feature = "gpu")]
            gpu_exits: self.gpu_exits.as_slice(),
            #[cfg(feature = "gpu")]
            gpu_entry_set: self.gpu_entry_set.clone(),
            #[cfg(feature = "gpu")]
            gpu_exit_set: self.gpu_exit_set.clone(),
            #[cfg(feature = "gpu")]
            data_edges: self.data_edges.clone(),
            segments: self.segments.as_slice(),
            schedule_order: self.schedule_order.as_slice(),
            const_inputs: self.const_inputs.clone(),
            backpressure: self.backpressure.clone(),
            handler: self.handler.clone(),
            core: self.core.snapshot(),
            direct_slot_access,
            adaptive: AdaptiveState::default(),
        }
    }

    pub fn run_in_place(&mut self) -> Result<ExecutionTelemetry, ExecuteError> {
        self.reset_for_run();
        let mut exec = self.snapshot(DirectSlotAccess::Serial);
        let res = serial::run_with_boundaries(&mut exec);
        serial::drain_host_outputs(&mut exec);
        if res.is_err() {
            self.storage_needs_reset = true;
        }
        res
    }

    /// Execute the runtime plan in parallel without rebuilding the executor (serially without
    /// the `threads` feature).
    ///
    /// With fail-fast enabled, this stops scheduling new ready segments after the first segment
    /// error. Segments already running still finish before the error is returned.
    pub fn run_parallel_in_place(&mut self) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
        self.reset_for_run();
        let mut exec = self.snapshot(DirectSlotAccess::Shared);
        let res = run_parallel_on(&mut exec);
        serial::drain_host_outputs(&mut exec);
        drop(exec);
        if res.is_err() {
            self.storage_needs_reset = true;
        }
        res
    }

    /// Execute without rebuilding, serially or in parallel as the measured costs of earlier runs
    /// suggest: parallel only when the work that could overlap outweighs the dispatch overhead.
    /// Graphs of cheap nodes run serially.
    pub fn run_adaptive_in_place(&mut self) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
        self.reset_for_run();
        let mut adaptive = std::mem::take(&mut self.adaptive);
        let workers = self.parallel_workers();
        let parallel = adaptive.choose(&self.schedule, &self.nodes, workers);
        let access = if parallel {
            DirectSlotAccess::Shared
        } else {
            DirectSlotAccess::Serial
        };
        let mut exec = self.snapshot(access);
        let res = run_adaptive_on(&mut exec, &mut adaptive, parallel, workers);
        serial::drain_host_outputs(&mut exec);
        drop(exec);
        self.adaptive = adaptive;
        if res.is_err() {
            self.storage_needs_reset = true;
        }
        res
    }

    /// Apply a graph patch to this executor's constant inputs without rebuilding the graph.
    pub fn apply_patch(&self, patch: &GraphPatch) -> PatchReport {
        let mut guard = self.const_inputs.write();
        apply_patch_to_const_inputs(patch, &self.nodes, guard.as_mut_slice())
    }
}
