use super::{
    DirectSlot, EdgeStorage, ExecutionTelemetry, ExecutorInit, ExecutorRunConfig, MaybeGpu,
    MetricsLevel, NodeMetadataStore, RuntimeDataSizeInspectors,
};
use crate::state::StateStore;
use std::collections::{BTreeMap, HashSet};
#[cfg(feature = "executor-pool")]
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};

pub(crate) struct ExecutorCore {
    pub(crate) state: StateStore,
    pub(crate) gpu_available: bool,
    pub(crate) gpu: MaybeGpu,
    pub(crate) queues: Arc<Vec<EdgeStorage>>,
    pub(crate) direct_edges: Arc<HashSet<usize>>,
    pub(crate) direct_slots: Arc<Vec<DirectSlot>>,
    pub(crate) warnings_seen: Arc<Mutex<HashSet<String>>>,
    pub(crate) telemetry: ExecutionTelemetry,
    pub(crate) data_size_inspectors: RuntimeDataSizeInspectors,
    pub(crate) run_config: ExecutorRunConfig,
    #[cfg(feature = "executor-pool")]
    pub(crate) pool_workers: usize,
    #[cfg(feature = "executor-pool")]
    pub(crate) worker_pool: Arc<OnceLock<Arc<rayon::ThreadPool>>>,
    pub(crate) host_bridges: Option<crate::host_bridge::HostBridgeManager>,
    pub(crate) const_coercers: Option<crate::io::ConstCoercerMap>,
    pub(crate) runtime_transport: Option<Arc<crate::transport::RuntimeTransport>>,
    pub(crate) graph_metadata: Arc<BTreeMap<String, daedalus_data::model::Value>>,
    pub(crate) node_metadata: NodeMetadataStore,
    pub(crate) capabilities: Arc<crate::capabilities::CapabilityRegistry>,
}

impl ExecutorCore {
    pub(crate) fn from_init(
        init: &ExecutorInit,
        graph_metadata: &BTreeMap<String, daedalus_data::model::Value>,
    ) -> Self {
        Self {
            state: StateStore::default(),
            gpu_available: false,
            #[cfg(feature = "gpu")]
            gpu: None,
            #[cfg(not(feature = "gpu"))]
            gpu: None,
            queues: init.queues.clone(),
            direct_edges: init.direct_edges.clone(),
            direct_slots: init.direct_slots.clone(),
            warnings_seen: Arc::new(Mutex::new(HashSet::new())),
            telemetry: ExecutionTelemetry::with_level(MetricsLevel::default()),
            data_size_inspectors: RuntimeDataSizeInspectors::global(),
            run_config: ExecutorRunConfig::default(),
            #[cfg(feature = "executor-pool")]
            pool_workers: init.pool_workers,
            #[cfg(feature = "executor-pool")]
            worker_pool: Arc::new(OnceLock::new()),
            host_bridges: None,
            const_coercers: None,
            runtime_transport: None,
            graph_metadata: Arc::new(graph_metadata.clone()),
            node_metadata: init.node_metadata.clone(),
            capabilities: Arc::new(crate::capabilities::CapabilityRegistry::new()),
        }
    }

    pub(crate) fn snapshot(&self) -> Self {
        Self {
            state: self.state.clone(),
            gpu_available: self.gpu_available,
            #[cfg(feature = "gpu")]
            gpu: self.gpu.clone(),
            #[cfg(not(feature = "gpu"))]
            gpu: self.gpu,
            queues: self.queues.clone(),
            direct_edges: self.direct_edges.clone(),
            direct_slots: self.direct_slots.clone(),
            warnings_seen: self.warnings_seen.clone(),
            telemetry: ExecutionTelemetry::with_level(self.run_config.metrics_level),
            data_size_inspectors: self.data_size_inspectors.clone(),
            run_config: self.run_config.clone(),
            #[cfg(feature = "executor-pool")]
            pool_workers: self.pool_workers,
            #[cfg(feature = "executor-pool")]
            worker_pool: self.worker_pool.clone(),
            host_bridges: self.host_bridges.clone(),
            const_coercers: self.const_coercers.clone(),
            runtime_transport: self.runtime_transport.clone(),
            graph_metadata: self.graph_metadata.clone(),
            node_metadata: self.node_metadata.clone(),
            capabilities: self.capabilities.clone(),
        }
    }
}
