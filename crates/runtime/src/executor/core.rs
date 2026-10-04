use super::{
    DirectSlot, EdgeStorage, ExecutionTelemetry, ExecutorInit, ExecutorRunConfig, MaybeGpu,
    MetricsLevel, NodeMetadataStore, RuntimeDataSizeInspectors,
};
use crate::state::StateStore;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::OnceLock;

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
    /// Workers a parallel run may use (see `resolve_parallel_workers`).
    pub(crate) parallel_workers: usize,
    /// Threads parallel runs fan out to, created on first use and shared by snapshots.
    pub(crate) worker_pool: Arc<OnceLock<Arc<super::WorkerPool>>>,
    /// Host-bridge nodes resolved when bridges were attached; empty without bridges.
    pub(crate) host_nodes: Arc<[super::serial::HostNodeIo]>,
    pub(crate) const_coercers: Option<crate::io::ConstCoercerMap>,
    /// Handed to every `NodeIo` so generic pushes resolve through the graph's registry.
    pub(crate) type_index: Option<crate::type_index::TypeIndex>,
    pub(crate) runtime_transport: Option<Arc<crate::transport::RuntimeTransport>>,
    pub(crate) graph_metadata: Arc<BTreeMap<String, daedalus_data::model::Value>>,
    pub(crate) node_metadata: NodeMetadataStore,
    pub(crate) output_ports: Arc<[Arc<[crate::handles::PortId]>]>,
    /// Node ids shared with `ExecutionContext::node_id` so ticks do not allocate them.
    pub(crate) node_ids: Arc<[Arc<str>]>,
    /// Per node, the incoming edges into its required inputs (see `ExecutorInit`).
    pub(crate) required_inputs: Arc<[Box<[usize]>]>,
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
            parallel_workers: init.parallel_workers,
            worker_pool: Arc::new(OnceLock::new()),
            host_nodes: Arc::new([]),
            const_coercers: None,
            type_index: None,
            runtime_transport: None,
            graph_metadata: Arc::new(graph_metadata.clone()),
            node_metadata: init.node_metadata.clone(),
            output_ports: init.output_ports.clone(),
            node_ids: init
                .nodes
                .iter()
                .map(|node| Arc::from(node.id.as_str()))
                .collect(),
            required_inputs: init.required_inputs.clone(),
            capabilities: Arc::new(crate::capabilities::CapabilityRegistry::new()),
        }
    }

    /// A `NodeIo` for node `node_idx` over `inputs` (a port buffer), wired to this executor's coercers, type
    /// index and the node's output ports.
    pub(crate) fn node_io(
        &self,
        node_idx: usize,
        inputs: Vec<crate::io::NodePort>,
    ) -> crate::io::NodeIo {
        crate::io::NodeIo::from_port_buffer(inputs)
            .with_const_coercers(self.const_coercers.clone())
            .with_type_index(self.type_index.clone())
            .with_output_ports(self.output_ports.get(node_idx).cloned())
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
            parallel_workers: self.parallel_workers,
            worker_pool: self.worker_pool.clone(),
            host_nodes: self.host_nodes.clone(),
            const_coercers: self.const_coercers.clone(),
            type_index: self.type_index.clone(),
            runtime_transport: self.runtime_transport.clone(),
            graph_metadata: self.graph_metadata.clone(),
            node_metadata: self.node_metadata.clone(),
            output_ports: self.output_ports.clone(),
            node_ids: self.node_ids.clone(),
            required_inputs: self.required_inputs.clone(),
            capabilities: self.capabilities.clone(),
        }
    }
}
