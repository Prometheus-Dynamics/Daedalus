use crate::portable::Arc;
use crate::prelude::*;
use alloc::collections::BTreeMap;

use crate::plan::{RuntimeEdge, RuntimeNode, RuntimeSegment, direct_edge_mask_for_active_edges};
use daedalus_planner::{NodeRef, is_host_bridge_metadata};

use super::{DirectSlot, NodeMetadataStore};

#[derive(Clone, Debug)]
pub(crate) struct CompiledSegmentGraph {
    pub adjacency: Arc<Vec<Vec<usize>>>,
    pub indegree: Arc<Vec<usize>>,
    pub ready_segments: Arc<Vec<usize>>,
    /// Segments a complete run executes: initially ready ones plus those with successors.
    pub total_segments: usize,
    /// The executed segments in topological order (for cost estimates over the DAG).
    pub topo_order: Arc<Vec<usize>>,
    /// Most segments on one dependency level: an upper bound on useful parallel workers.
    pub width: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledSchedule {
    pub host_nodes: Arc<Vec<NodeRef>>,
    pub host_deferred_graph: CompiledSegmentGraph,
    pub linear_segment_flow: bool,
    /// Segment of each node (`usize::MAX` for nodes outside every segment).
    pub segment_of: Arc<[usize]>,
}

pub(crate) fn is_host_bridge_node(node: &RuntimeNode) -> bool {
    is_host_bridge_metadata(&node.metadata)
}

fn node_exec_active_for_host_deferred(
    nodes: &[RuntimeNode],
    active_nodes: Option<&[bool]>,
    idx: usize,
) -> bool {
    if active_nodes
        .and_then(|mask| mask.get(idx).copied())
        .is_some_and(|active| !active)
    {
        return false;
    }
    let Some(node) = nodes.get(idx) else {
        return false;
    };
    !is_host_bridge_node(node)
}

fn build_segment_of(nodes_len: usize, segments: &[RuntimeSegment]) -> Vec<usize> {
    let mut segment_of = vec![usize::MAX; nodes_len];
    for (segment_idx, segment) in segments.iter().enumerate() {
        for node in &segment.nodes {
            if let Some(slot) = segment_of.get_mut(node.0) {
                *slot = segment_idx;
            }
        }
    }
    segment_of
}

pub(crate) fn direct_edge_set(
    edges: &[RuntimeEdge],
    edge_transports: &[Option<crate::plan::RuntimeEdgeTransport>],
) -> Vec<bool> {
    direct_edge_mask_for_active_edges(edges, edge_transports, |_| true)
}

/// One slot per edge; a buffer-all edge's slot keeps every payload, others only the newest.
pub(crate) fn direct_slots(edges: &[RuntimeEdge]) -> Arc<Vec<DirectSlot>> {
    Arc::new(
        edges
            .iter()
            .map(|edge| {
                DirectSlot::new(matches!(
                    edge.policy().pressure,
                    daedalus_transport::PressurePolicy::BufferAll
                ))
            })
            .collect(),
    )
}

fn build_segment_rank(
    segment_of: &[usize],
    schedule_order: &[NodeRef],
    segments_len: usize,
    node_exec_active: impl Fn(usize) -> bool,
) -> Vec<usize> {
    let mut segment_rank = vec![usize::MAX; segments_len];
    for (rank, node_ref) in schedule_order.iter().enumerate() {
        if !node_exec_active(node_ref.0) {
            continue;
        }
        let Some(&segment_idx) = segment_of.get(node_ref.0) else {
            continue;
        };
        if segment_idx == usize::MAX {
            continue;
        }
        if rank < segment_rank[segment_idx] {
            segment_rank[segment_idx] = rank;
        }
    }
    segment_rank
}

fn build_segment_graph(
    nodes: &[RuntimeNode],
    edges: &[RuntimeEdge],
    segments: &[RuntimeSegment],
    segment_of: &[usize],
    segment_rank: &[usize],
    node_exec_active: impl Fn(usize) -> bool,
    skip_host_bridge_edges: bool,
) -> CompiledSegmentGraph {
    let mut segment_active = vec![false; segments.len()];
    for (segment_idx, segment) in segments.iter().enumerate() {
        segment_active[segment_idx] = segment.nodes.iter().any(|node| node_exec_active(node.0));
    }

    let mut adjacency_sets: Vec<alloc::collections::BTreeSet<usize>> =
        vec![alloc::collections::BTreeSet::new(); segments.len()];
    let mut indegree = vec![0usize; segments.len()];
    for edge in edges {
        let from = edge.from();
        let to = edge.to();
        if !node_exec_active(from.0) || !node_exec_active(to.0) {
            continue;
        }
        if skip_host_bridge_edges
            && (nodes.get(from.0).is_some_and(is_host_bridge_node)
                || nodes.get(to.0).is_some_and(is_host_bridge_node))
        {
            continue;
        }
        let Some(&from_segment) = segment_of.get(from.0) else {
            continue;
        };
        let Some(&to_segment) = segment_of.get(to.0) else {
            continue;
        };
        if from_segment == usize::MAX || to_segment == usize::MAX {
            continue;
        }
        if !segment_active[from_segment] || !segment_active[to_segment] {
            continue;
        }
        if from_segment != to_segment && adjacency_sets[from_segment].insert(to_segment) {
            indegree[to_segment] += 1;
        }
    }

    let mut adjacency: Vec<Vec<usize>> = adjacency_sets
        .into_iter()
        .map(|set| set.into_iter().collect())
        .collect();
    for next in &mut adjacency {
        next.sort_by_key(|segment_idx| {
            segment_rank
                .get(*segment_idx)
                .copied()
                .unwrap_or(usize::MAX)
        });
    }
    let mut ready_segments: Vec<usize> = indegree
        .iter()
        .enumerate()
        .filter_map(|(idx, degree)| {
            if *degree == 0 && segment_active[idx] {
                Some(idx)
            } else {
                None
            }
        })
        .collect();
    ready_segments.sort_by_key(|segment_idx| {
        segment_rank
            .get(*segment_idx)
            .copied()
            .unwrap_or(usize::MAX)
    });

    let mut counted: alloc::collections::BTreeSet<usize> = ready_segments.iter().copied().collect();
    counted.extend((0..adjacency.len()).filter(|&idx| !adjacency[idx].is_empty()));
    let (topo_order, width) = topo_order_and_width(&adjacency, &indegree, &ready_segments);
    CompiledSegmentGraph {
        total_segments: counted.len(),
        topo_order: Arc::new(topo_order),
        width,
        adjacency: Arc::new(adjacency),
        indegree: Arc::new(indegree),
        ready_segments: Arc::new(ready_segments),
    }
}

/// Kahn order from `ready`, and the size of the largest level (segments whose longest path from
/// a ready segment has the same length).
fn topo_order_and_width(
    adjacency: &[Vec<usize>],
    indegree: &[usize],
    ready: &[usize],
) -> (Vec<usize>, usize) {
    let mut remaining = indegree.to_vec();
    let mut level = vec![0usize; adjacency.len()];
    let mut order = ready.to_vec();
    let mut next = 0;
    while let Some(&segment) = order.get(next) {
        next += 1;
        for &succ in &adjacency[segment] {
            level[succ] = level[succ].max(level[segment] + 1);
            remaining[succ] -= 1;
            if remaining[succ] == 0 {
                order.push(succ);
            }
        }
    }
    let mut per_level = vec![0usize; order.len()];
    for &segment in &order {
        per_level[level[segment]] += 1;
    }
    (order, per_level.into_iter().max().unwrap_or(0))
}

pub(crate) fn build_active_host_deferred_graph(
    nodes: &[RuntimeNode],
    edges: &[RuntimeEdge],
    segments: &[RuntimeSegment],
    schedule_order: &[NodeRef],
    active_nodes: Option<&[bool]>,
    segment_of: &[usize],
) -> (Vec<usize>, CompiledSegmentGraph) {
    let segment_rank = build_segment_rank(segment_of, schedule_order, segments.len(), |idx| {
        node_exec_active_for_host_deferred(nodes, active_nodes, idx)
    });
    let graph = build_segment_graph(
        nodes,
        edges,
        segments,
        segment_of,
        &segment_rank,
        |idx| node_exec_active_for_host_deferred(nodes, active_nodes, idx),
        true,
    );
    (segment_rank, graph)
}

pub(crate) fn build_compiled_schedule(
    nodes: &[RuntimeNode],
    edges: &[RuntimeEdge],
    segments: &[RuntimeSegment],
    schedule_order: &[NodeRef],
) -> CompiledSchedule {
    let segment_of = build_segment_of(nodes.len(), segments);
    let (_, host_deferred_graph) =
        build_active_host_deferred_graph(nodes, edges, segments, schedule_order, None, &segment_of);
    let host_nodes = nodes
        .iter()
        .enumerate()
        .filter_map(|(idx, node)| is_host_bridge_node(node).then_some(NodeRef(idx)))
        .collect();

    CompiledSchedule {
        linear_segment_flow: has_only_linear_segment_flow(
            nodes.len(),
            segments,
            edges,
            Some(&segment_of),
        ),
        host_nodes: Arc::new(host_nodes),
        host_deferred_graph,
        segment_of: segment_of.into(),
    }
}

/// Workers a parallel run may use: the configured pool size (else the available parallelism),
/// at most one per segment. One without the `threads` feature, so `Parallel` and `Adaptive` run
/// serially there.
#[cfg(not(feature = "threads"))]
pub(crate) fn resolve_parallel_workers(_pool_size: Option<usize>, _segments_len: usize) -> usize {
    1
}

/// Workers a parallel run may use: the configured pool size (else the available parallelism),
/// at most one per segment.
#[cfg(feature = "threads")]
pub(crate) fn resolve_parallel_workers(pool_size: Option<usize>, segments_len: usize) -> usize {
    pool_size
        .or_else(|| std::thread::available_parallelism().map(|n| n.get()).ok())
        .unwrap_or(4)
        .max(1)
        .min(segments_len.max(1))
}

fn has_only_linear_segment_flow(
    nodes_len: usize,
    segments: &[RuntimeSegment],
    edges: &[RuntimeEdge],
    compiled_segment_of: Option<&[usize]>,
) -> bool {
    if segments.len() <= 1 {
        return true;
    }

    let owned_segment_of;
    let segment_of = if let Some(segment_of) = compiled_segment_of {
        segment_of
    } else {
        owned_segment_of = build_segment_of(nodes_len, segments);
        &owned_segment_of
    };

    let mut indegree = vec![0usize; segments.len()];
    let mut outdegree = vec![0usize; segments.len()];
    let mut seen = alloc::collections::BTreeSet::new();
    for edge in edges {
        let Some(&from_segment) = segment_of.get(edge.from().0) else {
            continue;
        };
        let Some(&to_segment) = segment_of.get(edge.to().0) else {
            continue;
        };
        if from_segment == usize::MAX || to_segment == usize::MAX || from_segment == to_segment {
            continue;
        }
        if seen.insert((from_segment, to_segment)) {
            outdegree[from_segment] += 1;
            indegree[to_segment] += 1;
            if outdegree[from_segment] > 1 || indegree[to_segment] > 1 {
                return false;
            }
        }
    }

    indegree.iter().filter(|degree| **degree == 0).count() <= 1
}

pub(crate) fn build_node_execution_metadata(nodes: &[RuntimeNode]) -> NodeMetadataStore {
    Arc::new(
        nodes
            .iter()
            .map(|node| {
                let mut metadata: BTreeMap<String, daedalus_data::model::Value> =
                    node.metadata.clone();
                if let Some(label) = &node.label {
                    metadata.entry("label".to_string()).or_insert_with(|| {
                        daedalus_data::model::Value::String(label.clone().into())
                    });
                }
                if let Some(bundle) = &node.bundle {
                    metadata.entry("bundle".to_string()).or_insert_with(|| {
                        daedalus_data::model::Value::String(bundle.clone().into())
                    });
                }
                Arc::new(metadata)
            })
            .collect(),
    )
}
