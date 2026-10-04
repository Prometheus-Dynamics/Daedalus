#[cfg(feature = "gpu")]
use super::collect_data_edges;
#[cfg(feature = "executor-pool")]
use super::resolve_pool_workers;
use super::{
    CompiledSchedule, DirectSlot, EdgeStorage, ExecutorBuildError, NodeMetadataStore,
    build_compiled_schedule, build_node_execution_metadata, direct_edge_set, direct_slots,
    edge_maps, normalize_runtime_nodes, queue,
};
use crate::plan::{NODE_REQUIRED_INPUTS_META_KEY, RuntimeEdge, RuntimeNode, RuntimePlan};
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
    /// Per node, the incoming edges into its required (not optional) inputs.
    pub(crate) required_inputs: Arc<[Box<[usize]>]>,
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
    let required_inputs = required_input_edges(&nodes, &plan.edges, &incoming_edges);
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
        required_inputs,
        #[cfg(feature = "executor-pool")]
        pool_workers,
        #[cfg(feature = "gpu")]
        data_edges,
    })
}

/// Incoming edges into the inputs the planner listed as required
/// ([`NODE_REQUIRED_INPUTS_META_KEY`]): a node runs only when each of those ports has a value.
/// Optional, fan-in and undeclared ports never block.
fn required_input_edges(
    nodes: &[RuntimeNode],
    edges: &[RuntimeEdge],
    incoming: &[Vec<usize>],
) -> Arc<[Box<[usize]>]> {
    nodes
        .iter()
        .enumerate()
        .map(|(idx, node)| {
            let required = node
                .metadata
                .get(NODE_REQUIRED_INPUTS_META_KEY)
                .and_then(|value| value.as_list())
                .unwrap_or_default();
            let is_required = |port: &str| required.iter().any(|name| name.as_str() == Some(port));
            incoming
                .get(idx)
                .into_iter()
                .flatten()
                .copied()
                .filter(|&edge| {
                    edges
                        .get(edge)
                        .is_some_and(|e| is_required(e.target_port()))
                })
                .collect()
        })
        .collect()
}
