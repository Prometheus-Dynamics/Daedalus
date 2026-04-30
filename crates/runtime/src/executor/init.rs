#[cfg(feature = "gpu")]
use super::collect_data_edges;
#[cfg(feature = "executor-pool")]
use super::resolve_pool_workers;
use super::{
    CompiledSchedule, DirectSlot, EdgeStorage, ExecutorBuildError, NodeMetadataStore,
    build_compiled_schedule, build_node_execution_metadata, direct_edge_set, direct_slots,
    edge_maps, normalize_runtime_nodes, queue,
};
use crate::plan::{RuntimeNode, RuntimePlan};
use std::collections::HashSet;
use std::sync::Arc;

pub(crate) struct ExecutorInit {
    pub(crate) nodes: Arc<[RuntimeNode]>,
    pub(crate) incoming_edges: Arc<Vec<Vec<usize>>>,
    pub(crate) outgoing_edges: Arc<Vec<Vec<usize>>>,
    pub(crate) schedule: Arc<CompiledSchedule>,
    pub(crate) queues: Arc<Vec<EdgeStorage>>,
    pub(crate) direct_edges: Arc<HashSet<usize>>,
    pub(crate) direct_slots: Arc<Vec<DirectSlot>>,
    pub(crate) node_metadata: NodeMetadataStore,
    #[cfg(feature = "executor-pool")]
    pub(crate) pool_workers: usize,
    #[cfg(feature = "gpu")]
    pub(crate) data_edges: Arc<HashSet<usize>>,
}

pub(crate) fn build_executor_init(plan: &RuntimePlan) -> Result<ExecutorInit, ExecutorBuildError> {
    let nodes_vec = normalize_runtime_nodes(&plan.nodes)?;
    let nodes: Arc<[RuntimeNode]> = nodes_vec.into();
    let node_metadata = build_node_execution_metadata(&nodes);
    let queues = Arc::new(queue::build_queues(plan));
    let (incoming_edges, outgoing_edges) = edge_maps(&plan.edges);
    let direct_edges = Arc::new(direct_edge_set(&plan.edges, &plan.edge_transports));
    let direct_slots = direct_slots(plan.edges.len());
    let schedule = Arc::new(build_compiled_schedule(
        &nodes,
        &plan.edges,
        &plan.segments,
        &plan.schedule_order,
    ));
    #[cfg(feature = "executor-pool")]
    let pool_workers = resolve_pool_workers(None, plan.segments.len());
    #[cfg(feature = "gpu")]
    let data_edges = Arc::new(collect_data_edges(&nodes, &plan.edges));

    Ok(ExecutorInit {
        nodes,
        incoming_edges: Arc::new(incoming_edges),
        outgoing_edges: Arc::new(outgoing_edges),
        schedule,
        queues,
        direct_edges,
        direct_slots,
        node_metadata,
        #[cfg(feature = "executor-pool")]
        pool_workers,
        #[cfg(feature = "gpu")]
        data_edges,
    })
}
