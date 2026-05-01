use super::ExecutorConfigTarget;
#[cfg(not(feature = "executor-pool"))]
use super::parallel;
use super::{
    CompiledSchedule, ConstInputStore, DirectSlotAccess, ExecuteError, ExecutionTelemetry,
    Executor, ExecutorBuildError, ExecutorCore, ExecutorMaskError, MetricsLevel, NodeHandler,
    RuntimeDataSizeInspectors, apply_patch_to_const_inputs, build_executor_init, reset_run_storage,
    serial, should_run_parallel_adaptive,
};
#[cfg(feature = "executor-pool")]
use super::{compiled_worker_pool, pool};
use crate::plan::{BackpressureStrategy, RuntimeEdge, RuntimeNode, RuntimePlan, RuntimeSegment};
use crate::state::{ResourceLifecycleEvent, StateError, StateStore};
use daedalus_planner::{GraphPatch, NodeRef, PatchReport};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

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

    #[cfg(feature = "executor-pool")]
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
                plan.nodes.iter().map(|n| n.const_inputs.clone()).collect(),
            )),
            backpressure: plan.backpressure.clone(),
            handler: Arc::new(handler),
            core,
            storage_needs_reset: true,
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

    #[cfg(feature = "executor-pool")]
    pub fn prewarm_worker_pool(&self) -> Result<(), ExecuteError> {
        let _ = compiled_worker_pool(&self.core.worker_pool, self.core.pool_workers)?;
        Ok(())
    }

    pub fn with_metrics_level(mut self, level: MetricsLevel) -> Self {
        self.apply_metrics_level(level);
        self
    }

    pub fn with_runtime_debug_config(mut self, config: crate::config::RuntimeDebugConfig) -> Self {
        self.apply_runtime_debug_config(config);
        self
    }

    pub fn with_host_bridges(mut self, mgr: crate::host_bridge::HostBridgeManager) -> Self {
        self.apply_host_bridges(mgr);
        self
    }

    pub fn reset(&mut self) {
        let metrics_level = self.core.run_config.metrics_level;
        self.core.telemetry.reset_for_reuse(metrics_level);
        if let Ok(mut warnings) = self.core.warnings_seen.lock() {
            warnings.clear();
        }
        self.reset_storage();
        self.storage_needs_reset = false;
    }

    pub(super) fn reset_for_run(&mut self) {
        let metrics_level = self.core.run_config.metrics_level;
        self.core.telemetry.reset_for_reuse(metrics_level);
        if let Ok(mut warnings) = self.core.warnings_seen.lock() {
            warnings.clear();
        }
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
        }
    }

    pub fn run_in_place(&mut self) -> Result<ExecutionTelemetry, ExecuteError> {
        self.reset_for_run();
        let exec = self.snapshot(DirectSlotAccess::Serial);
        let res = serial::run(exec);
        let mut drain_exec = self.snapshot(DirectSlotAccess::Serial);
        serial::drain_host_outputs(&mut drain_exec);
        if res.is_err() {
            self.storage_needs_reset = true;
        }
        res
    }

    /// Execute the runtime plan in parallel without rebuilding the executor.
    ///
    /// With fail-fast enabled, this stops scheduling new ready segments after the first segment
    /// error. Scoped threads or Rayon tasks already running are still allowed to finish before the
    /// error is returned.
    pub fn run_parallel_in_place(&mut self) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
        self.reset_for_run();
        let exec = self.snapshot(DirectSlotAccess::Shared);
        let res = {
            if exec.schedule.linear_segment_flow {
                serial::run_fused_linear(exec)
            } else {
                #[cfg(feature = "executor-pool")]
                {
                    pool::run(exec)
                }
                #[cfg(not(feature = "executor-pool"))]
                {
                    parallel::run(exec)
                }
            }
        };
        let mut drain_exec = self.snapshot(DirectSlotAccess::Shared);
        serial::drain_host_outputs(&mut drain_exec);
        if res.is_err() {
            self.storage_needs_reset = true;
        }
        res
    }

    /// Execute the runtime plan with adaptive serial/parallel selection without rebuilding.
    pub fn run_adaptive_in_place(&mut self) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
        self.reset_for_run();
        let exec = self.snapshot(DirectSlotAccess::Shared);
        let res = if should_run_parallel_adaptive(&exec.schedule) {
            #[cfg(feature = "executor-pool")]
            {
                pool::run(exec)
            }
            #[cfg(not(feature = "executor-pool"))]
            {
                parallel::run(exec)
            }
        } else {
            serial::run(exec)
        };
        let mut drain_exec = self.snapshot(DirectSlotAccess::Shared);
        serial::drain_host_outputs(&mut drain_exec);
        if res.is_err() {
            self.storage_needs_reset = true;
        }
        res
    }

    /// Apply a graph patch to this executor's constant inputs without rebuilding the graph.
    pub fn apply_patch(&self, patch: &GraphPatch) -> PatchReport {
        let mut guard = self
            .const_inputs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        apply_patch_to_const_inputs(patch, &self.nodes, guard.as_mut_slice())
    }
}
