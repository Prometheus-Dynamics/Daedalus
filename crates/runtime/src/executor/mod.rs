use crate::plan::{BackpressureStrategy, EdgePolicyKind, RuntimeNode, RuntimePlan, RuntimeSegment};
use crate::state::StateStore;
use daedalus_planner::NodeRef;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

mod errors;
mod crash_diag;
mod handler;
mod parallel;
mod payload;
#[cfg(feature = "executor-pool")]
mod pool;
pub mod queue;
mod serial;
mod telemetry;

pub use errors::{ExecuteError, NodeError};
pub use handler::NodeHandler;
pub use payload::{CorrelatedPayload, EdgePayload, next_correlation_id};
pub use queue::EdgeStorage;
pub use telemetry::{EdgeMetrics, ExecutionTelemetry, NodeMetrics};
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
    #[cfg(feature = "gpu")]
    #[allow(dead_code)]
    pub(crate) gpu_edges: &'a [daedalus_planner::EdgeBufferInfo],
    #[cfg(feature = "gpu")]
    pub(crate) _gpu_entries: &'a [usize],
    #[cfg(feature = "gpu")]
    pub(crate) _gpu_exits: &'a [usize],
    #[cfg(feature = "gpu")]
    pub(crate) gpu_entry_set: Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")]
    pub(crate) gpu_exit_set: Arc<HashSet<usize>>,
    #[cfg(not(feature = "gpu"))]
    #[allow(dead_code)]
    pub(crate) gpu_edges: &'a [()],
    pub(crate) segments: &'a [RuntimeSegment],
    pub(crate) schedule_order: &'a [NodeRef],
    pub(crate) const_inputs: Arc<Vec<Vec<(String, daedalus_data::model::Value)>>>,
    pub(crate) backpressure: BackpressureStrategy,
    pub(crate) handler: Arc<H>,
    pub(crate) state: StateStore,
    pub(crate) gpu_available: bool,
    pub(crate) gpu: MaybeGpu,
    pub(crate) queues: Arc<Vec<EdgeStorage>>,
    pub(crate) warnings_seen: Arc<Mutex<HashSet<String>>>,
    pub(crate) telemetry: ExecutionTelemetry,
    pub(crate) pool_size: Option<usize>,
    pub(crate) host_bridges: Option<crate::host_bridge::HostBridgeManager>,
    pub(crate) const_coercers: Option<crate::io::ConstCoercerMap>,
    pub(crate) output_packers: Option<crate::io::OutputPackerMap>,
    pub(crate) graph_metadata: Arc<BTreeMap<String, daedalus_data::model::Value>>,
}

#[cfg(feature = "gpu")]
type MaybeGpu = Option<daedalus_gpu::GpuContextHandle>;
#[cfg(not(feature = "gpu"))]
type MaybeGpu = Option<()>;

type EdgeSpec = (NodeRef, String, NodeRef, String, EdgePolicyKind);

impl<'a, H: NodeHandler> Executor<'a, H> {
    /// Build an executor from a runtime plan and handler.
    pub fn new(plan: &'a RuntimePlan, handler: H) -> Self {
        let nodes: Arc<[RuntimeNode]> = plan.nodes.clone().into();
        let queues = queue::build_queues(plan);
        Self {
            nodes,
            edges: &plan.edges,
            #[cfg(feature = "gpu")]
            gpu_edges: &plan.gpu_edges,
            #[cfg(feature = "gpu")]
            _gpu_entries: &plan.gpu_entries,
            #[cfg(feature = "gpu")]
            _gpu_exits: &plan.gpu_exits,
            #[cfg(feature = "gpu")]
            gpu_entry_set: Arc::new(plan.gpu_entries.iter().cloned().collect()),
            #[cfg(feature = "gpu")]
            gpu_exit_set: Arc::new(plan.gpu_exits.iter().cloned().collect()),
            #[cfg(not(feature = "gpu"))]
            gpu_edges: &[],
            segments: &plan.segments,
            schedule_order: &plan.schedule_order,
            const_inputs: Arc::new(plan.nodes.iter().map(|n| n.const_inputs.clone()).collect()),
            backpressure: plan.backpressure.clone(),
            handler: Arc::new(handler),
            state: StateStore::default(),
            gpu_available: false,
            #[cfg(feature = "gpu")]
            gpu: None,
            #[cfg(not(feature = "gpu"))]
            gpu: None,
            queues: Arc::new(queues),
            warnings_seen: Arc::new(Mutex::new(HashSet::new())),
            telemetry: ExecutionTelemetry::default(),
            pool_size: None,
            host_bridges: None,
            const_coercers: None,
            output_packers: None,
            graph_metadata: Arc::new(plan.graph_metadata.clone()),
        }
    }

    /// Provide a shared constant coercer registry (used by dynamic plugins).
    pub fn with_const_coercers(mut self, coercers: crate::io::ConstCoercerMap) -> Self {
        self.const_coercers = Some(coercers);
        self
    }

    /// Provide a shared output packer registry (used by dynamic plugins).
    pub fn with_output_packers(mut self, packers: crate::io::OutputPackerMap) -> Self {
        self.output_packers = Some(packers);
        self
    }

    /// Inject shared state store (optional).
    pub fn with_state(mut self, state: StateStore) -> Self {
        self.state = state;
        self
    }

    /// Provide a GPU handle when available.
    #[cfg(feature = "gpu")]
    pub fn with_gpu(mut self, gpu: daedalus_gpu::GpuContextHandle) -> Self {
        self.gpu_available = true;
        self.gpu = Some(gpu.clone());
        if let Some(ref mgr) = self.host_bridges {
            mgr.attach_gpu(gpu);
        }
        self
    }

    #[cfg(not(feature = "gpu"))]
    pub fn without_gpu(mut self) -> Self {
        self.gpu_available = false;
        self
    }

    /// Override pool size when using the pool-based parallel executor.
    pub fn with_pool_size(mut self, size: Option<usize>) -> Self {
        self.pool_size = size;
        self
    }

    /// Attach a host bridge manager to enable implicit host I/O nodes.
    pub fn with_host_bridges(mut self, mgr: crate::host_bridge::HostBridgeManager) -> Self {
        #[cfg(feature = "gpu")]
        if let Some(gpu) = self.gpu.clone() {
            mgr.attach_gpu(gpu);
        }
        self.host_bridges = Some(mgr);
        self
    }

    /// Reset per-run state (queues, telemetry, warnings) so this executor can be reused.
    pub fn reset(&mut self) {
        self.telemetry = ExecutionTelemetry::default();
        if let Ok(mut warnings) = self.warnings_seen.lock() {
            warnings.clear();
        }

        for (idx, storage) in self.queues.iter().enumerate() {
            match storage {
                EdgeStorage::Locked(queue) => {
                    if let Ok(mut q) = queue.lock() {
                        *q = queue::EdgeQueue::default();
                        if let Some((_, _, _, _, policy)) = self.edges.get(idx) {
                            q.ensure_policy(policy);
                        }
                    }
                }
                #[cfg(feature = "lockfree-queues")]
                EdgeStorage::BoundedLf(queue) => while queue.pop().is_some() {},
            }
        }
    }

    /// Build a lightweight snapshot for a single run without re-planning.
    fn snapshot(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
            edges: self.edges,
            #[cfg(feature = "gpu")]
            gpu_edges: self.gpu_edges,
            #[cfg(feature = "gpu")]
            _gpu_entries: self._gpu_entries,
            #[cfg(feature = "gpu")]
            _gpu_exits: self._gpu_exits,
            #[cfg(feature = "gpu")]
            gpu_entry_set: self.gpu_entry_set.clone(),
            #[cfg(feature = "gpu")]
            gpu_exit_set: self.gpu_exit_set.clone(),
            #[cfg(not(feature = "gpu"))]
            gpu_edges: self.gpu_edges,
            segments: self.segments,
            schedule_order: self.schedule_order,
            const_inputs: self.const_inputs.clone(),
            backpressure: self.backpressure.clone(),
            handler: self.handler.clone(),
            state: self.state.clone(),
            gpu_available: self.gpu_available,
            #[cfg(feature = "gpu")]
            gpu: self.gpu.clone(),
            #[cfg(not(feature = "gpu"))]
            gpu: self.gpu,
            queues: self.queues.clone(),
            warnings_seen: self.warnings_seen.clone(),
            telemetry: ExecutionTelemetry::default(),
            pool_size: self.pool_size,
            host_bridges: self.host_bridges.clone(),
            const_coercers: self.const_coercers.clone(),
            output_packers: self.output_packers.clone(),
            graph_metadata: self.graph_metadata.clone(),
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
        self.reset();
        result
    }

    /// Execute the runtime plan allowing independent segments to run in parallel.
    pub fn run_parallel(self) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
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
    pub fn run_parallel_in_place(&mut self) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
        self.reset();
        let exec = self.snapshot();
        let result = self.run_parallel_from_snapshot(exec);
        self.reset();
        result
    }

    fn run_parallel_from_snapshot(
        &self,
        exec: Executor<'a, H>,
    ) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
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

/// Owned executor that can be reused across runs without leaking the plan.
pub struct OwnedExecutor<H: NodeHandler> {
    pub(crate) nodes: Arc<[RuntimeNode]>,
    pub(crate) edges: Arc<Vec<EdgeSpec>>,
    #[cfg(feature = "gpu")]
    #[allow(dead_code)]
    pub(crate) gpu_edges: Arc<Vec<daedalus_planner::EdgeBufferInfo>>,
    #[cfg(feature = "gpu")]
    pub(crate) _gpu_entries: Arc<Vec<usize>>,
    #[cfg(feature = "gpu")]
    pub(crate) _gpu_exits: Arc<Vec<usize>>,
    #[cfg(feature = "gpu")]
    pub(crate) gpu_entry_set: Arc<HashSet<usize>>,
    #[cfg(feature = "gpu")]
    pub(crate) gpu_exit_set: Arc<HashSet<usize>>,
    #[cfg(not(feature = "gpu"))]
    #[allow(dead_code)]
    pub(crate) gpu_edges: Arc<Vec<()>>,
    pub(crate) segments: Arc<Vec<RuntimeSegment>>,
    pub(crate) schedule_order: Arc<Vec<NodeRef>>,
    pub(crate) const_inputs: Arc<Vec<Vec<(String, daedalus_data::model::Value)>>>,
    pub(crate) backpressure: BackpressureStrategy,
    pub(crate) handler: Arc<H>,
    pub(crate) state: StateStore,
    pub(crate) gpu_available: bool,
    pub(crate) gpu: MaybeGpu,
    pub(crate) queues: Arc<Vec<EdgeStorage>>,
    pub(crate) warnings_seen: Arc<Mutex<HashSet<String>>>,
    pub(crate) telemetry: ExecutionTelemetry,
    pub(crate) pool_size: Option<usize>,
    pub(crate) host_bridges: Option<crate::host_bridge::HostBridgeManager>,
    pub(crate) const_coercers: Option<crate::io::ConstCoercerMap>,
    pub(crate) output_packers: Option<crate::io::OutputPackerMap>,
    pub(crate) graph_metadata: Arc<BTreeMap<String, daedalus_data::model::Value>>,
}

impl<H: NodeHandler> OwnedExecutor<H> {
    pub fn new(plan: Arc<RuntimePlan>, handler: H) -> Self {
        let nodes: Arc<[RuntimeNode]> = plan.nodes.clone().into();
        let queues = queue::build_queues(&plan);
        Self {
            nodes,
            edges: Arc::new(plan.edges.clone()),
            #[cfg(feature = "gpu")]
            gpu_edges: Arc::new(plan.gpu_edges.clone()),
            #[cfg(feature = "gpu")]
            _gpu_entries: Arc::new(plan.gpu_entries.clone()),
            #[cfg(feature = "gpu")]
            _gpu_exits: Arc::new(plan.gpu_exits.clone()),
            #[cfg(feature = "gpu")]
            gpu_entry_set: Arc::new(plan.gpu_entries.iter().cloned().collect()),
            #[cfg(feature = "gpu")]
            gpu_exit_set: Arc::new(plan.gpu_exits.iter().cloned().collect()),
            #[cfg(not(feature = "gpu"))]
            gpu_edges: Arc::new(Vec::new()),
            segments: Arc::new(plan.segments.clone()),
            schedule_order: Arc::new(plan.schedule_order.clone()),
            const_inputs: Arc::new(plan.nodes.iter().map(|n| n.const_inputs.clone()).collect()),
            backpressure: plan.backpressure.clone(),
            handler: Arc::new(handler),
            state: StateStore::default(),
            gpu_available: false,
            #[cfg(feature = "gpu")]
            gpu: None,
            #[cfg(not(feature = "gpu"))]
            gpu: None,
            queues: Arc::new(queues),
            warnings_seen: Arc::new(Mutex::new(HashSet::new())),
            telemetry: ExecutionTelemetry::default(),
            pool_size: None,
            host_bridges: None,
            const_coercers: None,
            output_packers: None,
            graph_metadata: Arc::new(plan.graph_metadata.clone()),
        }
    }

    /// Provide a shared constant coercer registry (used by dynamic plugins).
    pub fn with_const_coercers(mut self, coercers: crate::io::ConstCoercerMap) -> Self {
        self.const_coercers = Some(coercers);
        self
    }

    /// Provide a shared output packer registry (used by dynamic plugins).
    pub fn with_output_packers(mut self, packers: crate::io::OutputPackerMap) -> Self {
        self.output_packers = Some(packers);
        self
    }

    pub fn with_state(mut self, state: StateStore) -> Self {
        self.state = state;
        self
    }

    #[cfg(feature = "gpu")]
    pub fn with_gpu(mut self, gpu: daedalus_gpu::GpuContextHandle) -> Self {
        self.gpu_available = true;
        self.gpu = Some(gpu.clone());
        if let Some(ref mgr) = self.host_bridges {
            mgr.attach_gpu(gpu);
        }
        self
    }

    #[cfg(not(feature = "gpu"))]
    pub fn without_gpu(mut self) -> Self {
        self.gpu_available = false;
        self
    }

    pub fn with_pool_size(mut self, size: Option<usize>) -> Self {
        self.pool_size = size;
        self
    }

    pub fn with_host_bridges(mut self, mgr: crate::host_bridge::HostBridgeManager) -> Self {
        #[cfg(feature = "gpu")]
        if let Some(gpu) = self.gpu.clone() {
            mgr.attach_gpu(gpu);
        }
        self.host_bridges = Some(mgr);
        self
    }

    pub fn reset(&mut self) {
        self.telemetry = ExecutionTelemetry::default();
        if let Ok(mut warnings) = self.warnings_seen.lock() {
            warnings.clear();
        }
        for (idx, storage) in self.queues.iter().enumerate() {
            match storage {
                EdgeStorage::Locked(queue) => {
                    if let Ok(mut q) = queue.lock() {
                        *q = queue::EdgeQueue::default();
                        if let Some((_, _, _, _, policy)) = self.edges.get(idx) {
                            q.ensure_policy(policy);
                        }
                    }
                }
                #[cfg(feature = "lockfree-queues")]
                EdgeStorage::BoundedLf(queue) => while queue.pop().is_some() {},
            }
        }
    }

    fn snapshot<'a>(&'a self) -> Executor<'a, H> {
        Executor {
            nodes: self.nodes.clone(),
            edges: self.edges.as_slice(),
            #[cfg(feature = "gpu")]
            gpu_edges: self.gpu_edges.as_slice(),
            #[cfg(feature = "gpu")]
            _gpu_entries: self._gpu_entries.as_slice(),
            #[cfg(feature = "gpu")]
            _gpu_exits: self._gpu_exits.as_slice(),
            #[cfg(feature = "gpu")]
            gpu_entry_set: self.gpu_entry_set.clone(),
            #[cfg(feature = "gpu")]
            gpu_exit_set: self.gpu_exit_set.clone(),
            #[cfg(not(feature = "gpu"))]
            gpu_edges: self.gpu_edges.as_slice(),
            segments: self.segments.as_slice(),
            schedule_order: self.schedule_order.as_slice(),
            const_inputs: self.const_inputs.clone(),
            backpressure: self.backpressure.clone(),
            handler: self.handler.clone(),
            state: self.state.clone(),
            gpu_available: self.gpu_available,
            #[cfg(feature = "gpu")]
            gpu: self.gpu.clone(),
            #[cfg(not(feature = "gpu"))]
            gpu: self.gpu,
            queues: self.queues.clone(),
            warnings_seen: self.warnings_seen.clone(),
            telemetry: ExecutionTelemetry::default(),
            pool_size: self.pool_size,
            host_bridges: self.host_bridges.clone(),
            const_coercers: self.const_coercers.clone(),
            output_packers: self.output_packers.clone(),
            graph_metadata: self.graph_metadata.clone(),
        }
    }

    pub fn run_in_place(&mut self) -> Result<ExecutionTelemetry, ExecuteError> {
        self.reset();
        let exec = self.snapshot();
        let res = serial::run(exec);
        self.reset();
        res
    }

    pub fn run_parallel_in_place(&mut self) -> Result<ExecutionTelemetry, ExecuteError>
    where
        H: Send + Sync + 'static,
    {
        self.reset();
        let exec = self.snapshot();
        let res = {
            #[cfg(feature = "executor-pool")]
            {
                pool::run(exec)
            }
            #[cfg(not(feature = "executor-pool"))]
            {
                parallel::run(exec)
            }
        };
        self.reset();
        res
    }
}

/// Build adjacency maps of incoming/outgoing edge indices per node.
pub(crate) fn edge_maps(
    edges: &[(NodeRef, String, NodeRef, String, EdgePolicyKind)],
) -> (Vec<Vec<usize>>, Vec<Vec<usize>>) {
    let mut incoming: Vec<Vec<usize>> = Vec::new();
    let mut outgoing: Vec<Vec<usize>> = Vec::new();
    let grow = |v: &mut Vec<Vec<usize>>, idx: usize| {
        while v.len() <= idx {
            v.push(Vec::new());
        }
    };
    for (idx, (from, _, to, _, _)) in edges.iter().enumerate() {
        let f = from.0;
        let t = to.0;
        grow(&mut incoming, f.max(t));
        grow(&mut outgoing, f.max(t));
        outgoing[f].push(idx);
        incoming[t].push(idx);
    }
    (incoming, outgoing)
}
