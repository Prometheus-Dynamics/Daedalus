use super::{
    AdaptiveState, CompiledSchedule, ConstInputStore, DirectSlotAccess, ExecuteError,
    ExecutionTelemetry, Executor, ExecutorBuildError, ExecutorCore, ExecutorMaskError,
    MetricsLevel, NodeHandler, RuntimeDataSizeInspectors, ScheduleSource,
    apply_patch_to_const_inputs, build_compiled_schedule, build_executor_init, node_const_inputs,
    reset_run_storage, run_adaptive_on, run_parallel_on, serial,
};
use super::{ExecutorConfigTarget, executor_config_api};
use crate::plan::{BackpressureStrategy, RuntimeNode, RuntimePlan};
use crate::portable::Arc;
use crate::prelude::*;
use crate::state::{ResourceLifecycleEvent, StateStore};
use crate::sync::RwLock;
use core::time::Duration;
use daedalus_planner::{GraphPatch, PatchReport};

/// Owned executor that can be reused across runs without leaking the plan.
pub struct OwnedExecutor<H: NodeHandler> {
    pub(crate) nodes: Arc<[RuntimeNode]>,
    /// The plan the executor was built from: edges, transports, segments, schedule order and GPU
    /// entry/exit lists are read from it rather than copied.
    pub(crate) plan: Arc<RuntimePlan>,
    pub(crate) incoming_edges: Arc<Vec<Vec<usize>>>,
    pub(crate) outgoing_edges: Arc<Vec<Vec<usize>>>,
    pub(crate) schedule: Arc<CompiledSchedule>,
    #[cfg(feature = "gpu")]
    pub(crate) gpu_entry_set: Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")]
    pub(crate) gpu_exit_set: Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")]
    pub(crate) data_edges: Arc<HashSet<usize>>,
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
        self.plan.edges.len()
    }

    fn segments_len(&self) -> usize {
        self.schedule.segments.len()
    }
}

impl<H: NodeHandler> OwnedExecutor<H> {
    executor_config_api!();

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
        #[cfg(feature = "gpu")]
        let gpu_entry_set = Arc::new(plan.gpu_entries.iter().cloned().collect());
        #[cfg(feature = "gpu")]
        let gpu_exit_set = Arc::new(plan.gpu_exits.iter().cloned().collect());
        let const_inputs = Arc::new(RwLock::new(
            plan.nodes.iter().map(node_const_inputs).collect(),
        ));
        let backpressure = plan.backpressure.clone();
        Ok(Self {
            nodes: init.nodes,
            incoming_edges: init.incoming_edges,
            outgoing_edges: init.outgoing_edges,
            schedule: init.schedule,
            #[cfg(feature = "gpu")]
            gpu_entry_set,
            #[cfg(feature = "gpu")]
            gpu_exit_set,
            #[cfg(feature = "gpu")]
            data_edges: init.data_edges,
            const_inputs,
            backpressure,
            handler: Arc::new(handler),
            core,
            storage_needs_reset: true,
            adaptive: AdaptiveState::default(),
            plan,
        })
    }

    /// Enable demand-driven execution by selecting a set of sink nodes/ports and computing the
    /// upstream closure.
    #[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
    pub fn with_demand_sinks(mut self, sinks: Vec<crate::plan::RuntimeSink>) -> Self {
        match crate::plan::active_nodes_mask_for_sinks(
            self.nodes.as_ref(),
            self.plan.edges.as_slice(),
            &sinks,
        ) {
            Ok(mask) => {
                self.core.run_config.active_nodes = Some(Arc::new(mask));
            }
            Err(err) => {
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
                edges: &self.plan.edges,
                transports: &self.plan.edge_transports,
                segments: &self.plan.segments,
                schedule_order: &self.plan.schedule_order,
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
        super::WorkerPool::get_or_init(&self.core.worker_pool, self.parallel_workers()).map(drop)
    }

    fn parallel_workers(&self) -> usize {
        self.core
            .parallel_workers
            .min(self.schedule.host_deferred_graph.width)
    }

    /// The metrics level this executor records at.
    pub fn metrics_level(&self) -> MetricsLevel {
        self.core.run_config.metrics_level
    }

    /// The state store the node contexts run against.
    pub fn state_store(&self) -> &StateStore {
        &self.core.state
    }

    /// Run the node contexts against `state` (e.g. `StateStore::successor_for_reload` of the
    /// executor a graph reload replaces). Rebuilds the per-node contexts.
    pub fn set_state_store(&mut self, state: StateStore) {
        self.core.state = state;
        self.core.refresh_contexts();
    }

    /// Attach host bridges; each host-bridge node is resolved to its bridge once, here.
    pub fn with_host_bridges(mut self, mgr: crate::host_bridge::HostBridgeManager) -> Self {
        self.core.host_nodes = serial::resolve_host_nodes(
            &mgr,
            &self.nodes,
            &self.plan.edges,
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
        // A serial run hands its telemetry (this one) to the caller and leaves a default behind.
        let clock = self.core.clock.clone();
        self.core.telemetry.set_clock(clock);
        self.core.telemetry.reset_for_reuse(metrics_level);
        self.core.warnings_seen.lock().clear();
        if self.storage_needs_reset {
            self.reset_storage();
            self.storage_needs_reset = false;
        }
    }

    fn reset_storage(&mut self) {
        reset_run_storage(
            &self.plan.edges,
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

    pub fn set_selected_host_output_ports(&mut self, ports: Option<Arc<HashSet<String>>>) {
        self.apply_selected_host_output_ports(ports);
    }

    /// Fill `probe` with every run's frame-path overhead (any metrics level); `None` stops.
    /// Build it with [`FrameProbe::for_plan`](super::FrameProbe::for_plan) for this plan.
    pub fn set_frame_probe(&mut self, probe: Option<Arc<super::FrameProbe>>) {
        self.apply_frame_probe(probe);
    }

    /// The attached frame probe.
    pub fn frame_probe(&self) -> Option<&Arc<super::FrameProbe>> {
        self.core.run_config.frame_probe.as_ref()
    }

    /// Start timing a run for the frame probe, if one is attached.
    #[inline]
    pub(super) fn begin_probe_tick(&self) -> Option<super::TickStart> {
        let probe = self.core.run_config.frame_probe.as_ref()?;
        Some(probe.begin_tick(&self.core.clock))
    }

    #[inline]
    pub(super) fn end_probe_tick(&self, tick: Option<super::TickStart>) {
        if let (Some(probe), Some(tick)) = (&self.core.run_config.frame_probe, tick) {
            probe.end_tick(&self.core.clock, tick);
        }
    }

    /// An executor for one serial run that works on this executor's own core (its queues,
    /// telemetry and per-node tables) instead of a snapshot of it: nothing is cloned per tick.
    fn serial_view(&mut self) -> Executor<'_, H> {
        let Self {
            nodes,
            plan,
            incoming_edges,
            outgoing_edges,
            schedule,
            #[cfg(feature = "gpu")]
            gpu_entry_set,
            #[cfg(feature = "gpu")]
            gpu_exit_set,
            #[cfg(feature = "gpu")]
            data_edges,
            const_inputs,
            backpressure,
            handler,
            core,
            ..
        } = self;
        Executor {
            nodes: nodes.clone(),
            edges: plan.edges.as_slice(),
            edge_transports: plan.edge_transports.as_slice(),
            incoming_edges: incoming_edges.clone(),
            outgoing_edges: outgoing_edges.clone(),
            schedule: schedule.clone(),
            #[cfg(feature = "gpu")]
            gpu_entries: plan.gpu_entries.as_slice(),
            #[cfg(feature = "gpu")]
            gpu_exits: plan.gpu_exits.as_slice(),
            #[cfg(feature = "gpu")]
            gpu_entry_set: gpu_entry_set.clone(),
            #[cfg(feature = "gpu")]
            gpu_exit_set: gpu_exit_set.clone(),
            #[cfg(feature = "gpu")]
            data_edges: data_edges.clone(),
            segments: plan.segments.as_slice(),
            schedule_order: plan.schedule_order.as_slice(),
            const_inputs: const_inputs.clone(),
            backpressure: backpressure.clone(),
            handler: handler.clone(),
            core: super::CoreRef::Borrowed(core),
            direct_slot_access: DirectSlotAccess::Serial,
            adaptive: AdaptiveState::default(),
        }
    }

    pub(super) fn snapshot<'a>(&'a self, direct_slot_access: DirectSlotAccess) -> Executor<'a, H> {
        Executor {
            nodes: self.nodes.clone(),
            edges: self.plan.edges.as_slice(),
            edge_transports: self.plan.edge_transports.as_slice(),
            incoming_edges: self.incoming_edges.clone(),
            outgoing_edges: self.outgoing_edges.clone(),
            schedule: self.schedule.clone(),
            #[cfg(feature = "gpu")]
            gpu_entries: self.plan.gpu_entries.as_slice(),
            #[cfg(feature = "gpu")]
            gpu_exits: self.plan.gpu_exits.as_slice(),
            #[cfg(feature = "gpu")]
            gpu_entry_set: self.gpu_entry_set.clone(),
            #[cfg(feature = "gpu")]
            gpu_exit_set: self.gpu_exit_set.clone(),
            #[cfg(feature = "gpu")]
            data_edges: self.data_edges.clone(),
            segments: self.plan.segments.as_slice(),
            schedule_order: self.plan.schedule_order.as_slice(),
            const_inputs: self.const_inputs.clone(),
            backpressure: self.backpressure.clone(),
            handler: self.handler.clone(),
            core: super::CoreRef::Owned(self.core.snapshot()),
            direct_slot_access,
            adaptive: AdaptiveState::default(),
        }
    }

    pub fn run_in_place(&mut self) -> Result<ExecutionTelemetry, ExecuteError> {
        let _scope = super::runtime_alloc_scope();
        let tick = self.begin_probe_tick();
        self.reset_for_run();
        let mut exec = self.serial_view();
        let res = serial::run_with_boundaries(&mut exec);
        serial::drain_host_outputs(&mut exec);
        drop(exec);
        self.end_probe_tick(tick);
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
        let _scope = super::runtime_alloc_scope();
        let tick = self.begin_probe_tick();
        self.reset_for_run();
        let mut exec = self.snapshot(DirectSlotAccess::Shared);
        let res = run_parallel_on(&mut exec);
        serial::drain_host_outputs(&mut exec);
        drop(exec);
        self.end_probe_tick(tick);
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
        let _scope = super::runtime_alloc_scope();
        let tick = self.begin_probe_tick();
        self.reset_for_run();
        let mut adaptive = core::mem::take(&mut self.adaptive);
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
        self.end_probe_tick(tick);
        self.adaptive = adaptive;
        if res.is_err() {
            self.storage_needs_reset = true;
        }
        res
    }
}
