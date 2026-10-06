use super::{
    DirectSlot, EdgeStorage, ExecutionTelemetry, ExecutorInit, ExecutorRunConfig, MaybeGpu,
    MetricsLevel, NodeMetadataStore, RuntimeDataSizeInspectors,
};
use crate::io::{NodeConstInputs, NodeIo, NodeIoEnv, NodePort};
use crate::portable::Arc;
use crate::prelude::*;
use crate::state::{ExecutionContext, StateStore};
use crate::sync::Mutex;
use alloc::collections::BTreeMap;
use daedalus_core::platform::Clock;

/// An executor's core: its own (a snapshot, or a borrowed executor's) or borrowed from an
/// [`OwnedExecutor`](super::OwnedExecutor) for a serial run.
// Boxing the owned core would allocate per snapshot (parallel ticks take one per worker).
#[allow(clippy::large_enum_variant)]
pub(crate) enum CoreRef<'a> {
    Owned(ExecutorCore),
    Borrowed(&'a mut ExecutorCore),
}

impl core::ops::Deref for CoreRef<'_> {
    type Target = ExecutorCore;

    #[inline]
    fn deref(&self) -> &ExecutorCore {
        match self {
            Self::Owned(core) => core,
            Self::Borrowed(core) => core,
        }
    }
}

impl core::ops::DerefMut for CoreRef<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut ExecutorCore {
        match self {
            Self::Owned(core) => core,
            Self::Borrowed(core) => core,
        }
    }
}

pub(crate) struct ExecutorCore {
    pub(crate) state: StateStore,
    pub(crate) gpu_available: bool,
    pub(crate) gpu: MaybeGpu,
    pub(crate) queues: Arc<Vec<EdgeStorage>>,
    /// Per edge, whether it uses a direct slot (unless `run_config` masks override it).
    pub(crate) direct_edges: Arc<[bool]>,
    /// Edges that stay queues under the graph's backpressure strategy (see `ExecutorInit`).
    pub(crate) queued_edges: Option<Arc<[bool]>>,
    /// Per node, whether it is a host-bridge node.
    pub(crate) host_bridges: Arc<[bool]>,
    pub(crate) direct_slots: Arc<Vec<DirectSlot>>,
    pub(crate) warnings_seen: Arc<Mutex<HashSet<String>>>,
    pub(crate) telemetry: ExecutionTelemetry,
    pub(crate) data_size_inspectors: RuntimeDataSizeInspectors,
    pub(crate) run_config: ExecutorRunConfig,
    /// Workers a parallel run may use (see `resolve_parallel_workers`).
    pub(crate) parallel_workers: usize,
    /// Threads parallel runs fan out to, created on first use and shared by snapshots.
    #[cfg(feature = "threads")]
    pub(crate) worker_pool: Arc<std::sync::OnceLock<Arc<super::WorkerPool>>>,
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
    pub(crate) required_inputs: Arc<[super::init::RequiredInputs]>,
    pub(crate) capabilities: Arc<crate::capabilities::CapabilityRegistry>,
    /// Clock behind every timing this executor records (`Executor::with_clock`).
    pub(crate) clock: Clock,
    /// Each node's `ExecutionContext`, built when the state, capabilities or GPU change rather
    /// than per call.
    pub(crate) contexts: Arc<[ExecutionContext]>,
    /// Each node's `NodeIo` environment, built when the coercers, type index or clock change.
    pub(crate) io_envs: Arc<[Arc<NodeIoEnv>]>,
}

impl ExecutorCore {
    pub(crate) fn from_init(
        init: &ExecutorInit,
        graph_metadata: &BTreeMap<String, daedalus_data::model::Value>,
    ) -> Self {
        let mut core = Self {
            state: StateStore::default(),
            gpu_available: false,
            #[cfg(feature = "gpu")]
            gpu: None,
            #[cfg(not(feature = "gpu"))]
            gpu: None,
            queues: init.queues.clone(),
            direct_edges: init.direct_edges.clone(),
            queued_edges: init.queued_edges.clone(),
            host_bridges: init.host_bridges.clone(),
            direct_slots: init.direct_slots.clone(),
            warnings_seen: Arc::new(Mutex::new(HashSet::new())),
            telemetry: ExecutionTelemetry::with_level(MetricsLevel::default()),
            data_size_inspectors: RuntimeDataSizeInspectors::global(),
            run_config: ExecutorRunConfig::default(),
            parallel_workers: init.parallel_workers,
            #[cfg(feature = "threads")]
            worker_pool: Arc::default(),
            host_nodes: Arc::from(Vec::new()),
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
            clock: Clock::default(),
            contexts: Arc::from(Vec::new()),
            io_envs: Arc::from(Vec::new()),
        };
        core.refresh_contexts();
        core.refresh_io_envs();
        core
    }

    /// Rebuild [`Self::io_envs`] from the current coercers, type index and clock.
    pub(crate) fn refresh_io_envs(&mut self) {
        self.io_envs = self
            .output_ports
            .iter()
            .map(|ports| {
                Arc::new(NodeIoEnv::new(
                    self.const_coercers.clone(),
                    self.type_index.clone(),
                    Some(ports.clone()),
                    self.clock.clone(),
                ))
            })
            .collect();
    }

    /// Rebuild [`Self::contexts`] from the current state, capabilities and GPU.
    pub(crate) fn refresh_contexts(&mut self) {
        self.contexts = self
            .node_ids
            .iter()
            .zip(self.node_metadata.iter())
            .map(|(node_id, metadata)| {
                #[allow(unused_mut)]
                let mut ctx = ExecutionContext::new(
                    self.state.clone(),
                    node_id.clone(),
                    metadata.clone(),
                    self.graph_metadata.clone(),
                    self.capabilities.clone(),
                );
                #[cfg(feature = "gpu")]
                {
                    ctx.gpu = self.gpu.clone();
                }
                ctx
            })
            .collect();
    }

    /// A `NodeIo` for node `node_idx` over its edge `inputs` (a port buffer) and `consts`, wired
    /// to the node's environment (this executor's coercers, type index and clock, and the node's
    /// output ports).
    pub(crate) fn node_io(
        &self,
        node_idx: usize,
        inputs: Vec<NodePort>,
        consts: Option<&Arc<NodeConstInputs>>,
    ) -> NodeIo {
        let env = self.io_envs.get(node_idx).cloned().unwrap_or_default();
        NodeIo::for_call(inputs, env, consts)
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
            queued_edges: self.queued_edges.clone(),
            host_bridges: self.host_bridges.clone(),
            direct_slots: self.direct_slots.clone(),
            warnings_seen: self.warnings_seen.clone(),
            telemetry: ExecutionTelemetry::with_level(self.run_config.metrics_level)
                .with_clock(&self.clock),
            data_size_inspectors: self.data_size_inspectors.clone(),
            run_config: self.run_config.clone(),
            parallel_workers: self.parallel_workers,
            #[cfg(feature = "threads")]
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
            clock: self.clock.clone(),
            contexts: self.contexts.clone(),
            io_envs: self.io_envs.clone(),
        }
    }
}
