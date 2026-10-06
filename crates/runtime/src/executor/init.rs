use crate::prelude::*;

#[cfg(feature = "gpu")]
use super::collect_data_edges;
use super::{
    CompiledSchedule, DirectSlot, EdgeStorage, ExecutorBuildError, NodeMetadataStore,
    build_compiled_schedule, build_node_execution_metadata, direct_edge_set, direct_slots,
    edge_maps, is_host_bridge_node, normalize_runtime_nodes, queue, resolve_parallel_workers,
};
use crate::handles::PortId;
use crate::plan::{NODE_REQUIRED_INPUTS_META_KEY, NodeFire, RuntimeEdge, RuntimeNode, RuntimePlan};
use crate::portable::Arc;

pub(crate) struct ExecutorInit {
    pub(crate) nodes: Arc<[RuntimeNode]>,
    pub(crate) incoming_edges: Arc<Vec<Vec<usize>>>,
    pub(crate) outgoing_edges: Arc<Vec<Vec<usize>>>,
    pub(crate) schedule: Arc<CompiledSchedule>,
    pub(crate) queues: Arc<Vec<EdgeStorage>>,
    /// Per edge, whether it hands payloads over through a direct slot.
    pub(crate) direct_edges: Arc<[bool]>,
    /// Per edge, whether it must stay a queue although its policy allows a direct slot: bounded
    /// edges under a graph-level `BackpressureStrategy`, which rejects rather than replaces.
    pub(crate) queued_edges: Option<Arc<[bool]>>,
    /// Per node, whether it is a host-bridge node (never run by the scheduler).
    pub(crate) host_bridges: Arc<[bool]>,
    pub(crate) direct_slots: Arc<Vec<DirectSlot>>,
    pub(crate) node_metadata: NodeMetadataStore,
    /// Each node's connected output port ids, handed to its `NodeIo`.
    pub(crate) output_ports: Arc<[Arc<[PortId]>]>,
    /// Per node, the incoming edges into its required (not optional) inputs.
    pub(crate) required_inputs: Arc<[RequiredInputs]>,
    pub(crate) parallel_workers: usize,
    #[cfg(feature = "gpu")]
    pub(crate) data_edges: Arc<HashSet<usize>>,
}

pub(crate) fn build_executor_init(plan: &RuntimePlan) -> Result<ExecutorInit, ExecutorBuildError> {
    let nodes_vec = normalize_runtime_nodes(&plan.nodes)?;
    let nodes: Arc<[RuntimeNode]> = nodes_vec.into();
    let node_metadata = build_node_execution_metadata(&nodes);
    let queues = Arc::new(queue::build_queues(plan));
    let (incoming_edges, outgoing_edges) = edge_maps(&plan.edges);
    let output_ports = (0..nodes.len())
        .map(|node_idx| {
            let mut ports: Vec<PortId> = Vec::new();
            for &edge_idx in outgoing_edges.get(node_idx).into_iter().flatten() {
                let port = plan.edges[edge_idx].source_port_id();
                if !ports.contains(port) {
                    ports.push(port.clone());
                }
            }
            ports.into()
        })
        .collect();
    let required_inputs = required_input_edges(&nodes, &plan.edges, &incoming_edges);
    let queued_edges = backpressure_queued_edges(plan);
    let mut direct_edges = direct_edge_set(&plan.edges, &plan.edge_transports);
    if let Some(queued) = &queued_edges {
        for (direct, queued) in direct_edges.iter_mut().zip(queued.iter()) {
            *direct &= !queued;
        }
    }
    let host_bridges = nodes.iter().map(is_host_bridge_node).collect();
    let direct_slots = direct_slots(plan.edges.len());
    let schedule = Arc::new(build_compiled_schedule(
        &nodes,
        &plan.edges,
        &plan.segments,
        &plan.schedule_order,
    ));
    let parallel_workers = resolve_parallel_workers(None, plan.segments.len());
    #[cfg(feature = "gpu")]
    let data_edges = Arc::new(collect_data_edges(&nodes, &plan.edges));

    Ok(ExecutorInit {
        nodes,
        incoming_edges: Arc::new(incoming_edges),
        outgoing_edges: Arc::new(outgoing_edges),
        schedule,
        queues,
        direct_edges: direct_edges.into(),
        queued_edges,
        host_bridges,
        direct_slots,
        node_metadata,
        output_ports,
        required_inputs,
        parallel_workers,
        #[cfg(feature = "gpu")]
        data_edges,
    })
}

/// [`ExecutorInit::queued_edges`]: `None` without a backpressure override.
fn backpressure_queued_edges(plan: &RuntimePlan) -> Option<Arc<[bool]>> {
    (plan.backpressure != crate::plan::BackpressureStrategy::None).then(|| {
        plan.edges
            .iter()
            .map(|edge| edge.policy().bounded_capacity().is_some())
            .collect()
    })
}

/// A node's incoming edges into its required inputs, and its [`NodeFire`] mode.
pub(crate) struct RequiredInputs {
    pub(crate) edges: Box<[usize]>,
    /// [`NodeFire::All`]: pop nothing until each of `edges` holds a value.
    pub(crate) wait_all: bool,
}

/// Incoming edges into the inputs the planner listed as required
/// ([`NODE_REQUIRED_INPUTS_META_KEY`]): a node runs only when each of those ports has a value.
/// Optional, fan-in and undeclared ports never block.
fn required_input_edges(
    nodes: &[RuntimeNode],
    edges: &[RuntimeEdge],
    incoming: &[Vec<usize>],
) -> Arc<[RequiredInputs]> {
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
            let edges: Box<[usize]> = incoming
                .get(idx)
                .into_iter()
                .flatten()
                .copied()
                .filter(|&edge| {
                    edges
                        .get(edge)
                        .is_some_and(|e| is_required(e.target_port()))
                })
                .collect();
            RequiredInputs {
                wait_all: !edges.is_empty()
                    && NodeFire::from_metadata(&node.metadata) == NodeFire::All,
                edges,
            }
        })
        .collect()
}
