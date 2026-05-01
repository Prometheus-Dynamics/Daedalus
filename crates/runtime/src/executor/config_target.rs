use std::collections::HashSet;
use std::sync::Arc;
#[cfg(feature = "executor-pool")]
use std::sync::OnceLock;

#[cfg(feature = "executor-pool")]
use super::resolve_pool_workers;
use super::{
    Executor, ExecutorCore, ExecutorMaskError, MetricsLevel, NodeHandler, RuntimeDataSizeInspectors,
};
use crate::state::StateStore;

pub(crate) trait ExecutorConfigTarget {
    fn core_mut(&mut self) -> &mut ExecutorCore;
    fn nodes_len(&self) -> usize;
    fn edges_len(&self) -> usize;
    #[cfg(feature = "executor-pool")]
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
        self.core_mut()
            .run_config
            .set_active_direct_edges_mask(active_direct_edges, expected)
    }

    fn apply_selected_host_output_ports(&mut self, ports: Option<Arc<HashSet<String>>>) {
        self.core_mut()
            .run_config
            .set_selected_host_output_ports(ports);
    }

    fn apply_fail_fast(&mut self, enabled: bool) {
        self.core_mut().run_config.set_fail_fast(enabled);
    }

    fn apply_const_coercers(&mut self, coercers: crate::io::ConstCoercerMap) {
        self.core_mut().const_coercers = Some(coercers);
    }

    fn apply_data_size_inspectors(&mut self, inspectors: RuntimeDataSizeInspectors) {
        self.core_mut().data_size_inspectors = inspectors;
    }

    fn apply_runtime_transport(&mut self, transport: crate::transport::RuntimeTransport) {
        self.core_mut().runtime_transport = Some(Arc::new(transport));
    }

    fn apply_capabilities(&mut self, capabilities: crate::capabilities::CapabilityRegistry) {
        self.core_mut().capabilities = Arc::new(capabilities);
    }

    fn apply_state(&mut self, state: StateStore) {
        self.core_mut().state = state;
    }

    fn apply_pool_size(&mut self, size: Option<usize>) {
        self.core_mut().run_config.set_pool_size(size);
        #[cfg(feature = "executor-pool")]
        {
            let workers = resolve_pool_workers(size, self.segments_len());
            let core = self.core_mut();
            core.pool_workers = workers;
            core.worker_pool = Arc::new(OnceLock::new());
        }
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

    fn apply_host_bridges(&mut self, mgr: crate::host_bridge::HostBridgeManager) {
        self.core_mut().host_bridges = Some(mgr);
    }

    #[cfg(feature = "gpu")]
    fn apply_gpu(&mut self, gpu: daedalus_gpu::GpuContextHandle) {
        let core = self.core_mut();
        core.gpu_available = true;
        core.gpu = Some(gpu);
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

    #[cfg(feature = "executor-pool")]
    fn segments_len(&self) -> usize {
        self.segments.len()
    }
}
