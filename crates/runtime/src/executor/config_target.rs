use crate::portable::Arc;
use crate::prelude::*;

use super::{
    Executor, ExecutorCore, ExecutorMaskError, MetricsLevel, NodeHandler,
    RuntimeDataSizeInspectors, resolve_parallel_workers,
};
use crate::state::StateStore;

pub(crate) trait ExecutorConfigTarget {
    fn core_mut(&mut self) -> &mut ExecutorCore;
    fn nodes_len(&self) -> usize;
    fn edges_len(&self) -> usize;
    fn segments_len(&self) -> usize;

    fn apply_active_nodes_mask(
        &mut self,
        active_nodes: Option<Arc<Vec<bool>>>,
    ) -> Result<(), ExecutorMaskError> {
        let expected = self.nodes_len();
        self.core_mut()
            .run_config
            .set_active_nodes_mask(active_nodes, expected)
    }

    fn apply_active_edges_mask(
        &mut self,
        active_edges: Option<Arc<Vec<bool>>>,
    ) -> Result<(), ExecutorMaskError> {
        let expected = self.edges_len();
        self.core_mut()
            .run_config
            .set_active_edges_mask(active_edges, expected)
    }

    fn apply_active_direct_edges_mask(
        &mut self,
        active_direct_edges: Option<Arc<Vec<bool>>>,
    ) -> Result<(), ExecutorMaskError> {
        let expected = self.edges_len();
        let core = self.core_mut();
        let active_direct_edges = match (active_direct_edges, &core.queued_edges) {
            (Some(mask), Some(queued)) => Some(Arc::new(
                mask.iter()
                    .enumerate()
                    .map(|(idx, &direct)| direct && !queued.get(idx).copied().unwrap_or(false))
                    .collect(),
            )),
            (mask, _) => mask,
        };
        core.run_config
            .set_active_direct_edges_mask(active_direct_edges, expected)
    }

    fn apply_selected_host_output_ports(&mut self, ports: Option<Arc<HashSet<String>>>) {
        self.core_mut()
            .run_config
            .set_selected_host_output_ports(ports);
    }

    fn apply_frame_probe(&mut self, probe: Option<Arc<super::FrameProbe>>) {
        self.core_mut().run_config.frame_probe = probe;
    }

    fn apply_fail_fast(&mut self, enabled: bool) {
        self.core_mut().run_config.set_fail_fast(enabled);
    }

    fn apply_const_coercers(&mut self, coercers: crate::io::ConstCoercerMap) {
        let core = self.core_mut();
        core.const_coercers = Some(coercers);
        core.refresh_io_envs();
    }

    fn apply_type_index(&mut self, types: crate::type_index::TypeIndex) {
        let core = self.core_mut();
        core.type_index = Some(types);
        core.refresh_io_envs();
    }

    fn apply_data_size_inspectors(&mut self, inspectors: RuntimeDataSizeInspectors) {
        self.core_mut().data_size_inspectors = inspectors;
    }

    fn apply_runtime_transport(&mut self, transport: crate::transport::RuntimeTransport) {
        self.core_mut().runtime_transport = Some(Arc::new(transport));
    }

    fn apply_capabilities(&mut self, capabilities: crate::capabilities::CapabilityRegistry) {
        let core = self.core_mut();
        core.capabilities = Arc::new(capabilities);
        core.refresh_contexts();
    }

    fn apply_state(&mut self, state: StateStore) {
        let core = self.core_mut();
        core.state = state;
        core.refresh_contexts();
    }

    fn apply_pool_size(&mut self, size: Option<usize>) {
        self.core_mut().run_config.set_pool_size(size);
        let workers = resolve_parallel_workers(size, self.segments_len());
        let core = self.core_mut();
        core.parallel_workers = workers;
        #[cfg(feature = "threads")]
        {
            core.worker_pool = Arc::default();
        }
    }

    fn apply_clock(&mut self, clock: daedalus_core::platform::Clock) {
        let core = self.core_mut();
        core.telemetry = core::mem::take(&mut core.telemetry).with_clock(&clock);
        core.clock = clock;
        core.refresh_io_envs();
    }

    fn apply_metrics_level(&mut self, level: MetricsLevel) {
        let core = self.core_mut();
        core.run_config.metrics_level = level;
        core.telemetry.metrics_level = level;
    }

    fn apply_runtime_debug_config(&mut self, config: crate::config::RuntimeDebugConfig) {
        let pool_size = self.core_mut().run_config.set_runtime_debug_config(config);
        self.apply_pool_size(pool_size);
    }

    #[cfg(feature = "gpu")]
    fn apply_gpu(&mut self, gpu: daedalus_gpu::GpuContextHandle) {
        let core = self.core_mut();
        core.gpu_available = true;
        core.gpu = Some(gpu);
        core.refresh_contexts();
    }

    #[cfg(not(feature = "gpu"))]
    fn clear_gpu(&mut self) {
        self.core_mut().gpu_available = false;
    }
}

impl<H: NodeHandler> ExecutorConfigTarget for Executor<'_, H> {
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
        self.schedule.segments.len()
    }
}

/// The configuration and lifecycle methods `Executor` and `OwnedExecutor` share. Each type
/// expands this list into its inherent impl: the bodies only call `ExecutorConfigTarget`,
/// and the methods must stay inherent (public, without the trait in scope).
macro_rules! executor_config_api {
    () => {
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

        pub fn with_selected_host_output_ports(
            mut self,
            ports: Option<Arc<HashSet<String>>>,
        ) -> Self {
            self.apply_selected_host_output_ports(ports);
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

        pub fn with_runtime_transport(
            mut self,
            transport: crate::transport::RuntimeTransport,
        ) -> Self {
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
        /// (default [`crate::executor::DEFAULT_DISPATCH_OVERHEAD`]); parallel frames refine it.
        pub fn with_adaptive_dispatch_overhead(mut self, overhead: Duration) -> Self {
            self.adaptive.set_dispatch_overhead(overhead);
            self
        }

        pub fn with_metrics_level(mut self, level: MetricsLevel) -> Self {
            self.apply_metrics_level(level);
            self
        }

        /// Read every timing (telemetry, adaptive costs, edge waits) from `clock` instead of the
        /// platform clock.
        pub fn with_clock(mut self, clock: daedalus_core::platform::Clock) -> Self {
            self.apply_clock(clock);
            self
        }

        /// The clock this executor's timings read.
        pub fn clock(&self) -> &daedalus_core::platform::Clock {
            &self.core.clock
        }

        pub fn with_runtime_debug_config(
            mut self,
            config: crate::config::RuntimeDebugConfig,
        ) -> Self {
            self.apply_runtime_debug_config(config);
            self
        }

        /// Apply a graph patch to this executor's constant inputs without rebuilding the graph.
        pub fn apply_patch(&self, patch: &GraphPatch) -> PatchReport {
            let mut guard = self.const_inputs.write();
            apply_patch_to_const_inputs(patch, &self.nodes, guard.as_mut_slice())
        }
    };
}

pub(crate) use executor_config_api;
