use serde::{Deserialize, Serialize};

pub use daedalus_core::policy::BackpressureStrategy;
use daedalus_planner::{
    ComputeAffinity, EdgeBufferInfo, ExecutionPlan, GpuSegment, GraphNodeSelector, NodeRef,
};
use std::collections::{BTreeSet, VecDeque};

/// Edge policy kinds; default is FIFO.
///
/// ```
/// use daedalus_runtime::EdgePolicyKind;
/// let policy = EdgePolicyKind::NewestWins;
/// assert_eq!(policy, EdgePolicyKind::NewestWins);
/// ```
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum EdgePolicyKind {
    #[default]
    Fifo,
    NewestWins,
    Broadcast,
    Bounded {
        cap: usize,
    },
}

/// Runtime node with policy hints.
///
/// ```
/// use daedalus_runtime::RuntimeNode;
/// use daedalus_planner::ComputeAffinity;
/// let node = RuntimeNode {
///     id: "demo".into(),
///     stable_id: 0,
///     bundle: None,
///     label: None,
///     compute: ComputeAffinity::CpuOnly,
///     const_inputs: vec![],
///     sync_groups: vec![],
///     metadata: Default::default(),
/// };
/// assert_eq!(node.id, "demo");
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimeNode {
    pub id: String,
    /// Stable numeric id for runtime hot paths (e.g. handler dispatch).
    ///
    /// Derived deterministically from `id` and validated for collision-free use
    /// within a graph/registry.
    #[serde(skip)]
    pub stable_id: u128,
    pub bundle: Option<String>,
    pub label: Option<String>,
    pub compute: ComputeAffinity,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub const_inputs: Vec<(String, daedalus_data::model::Value)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sync_groups: Vec<daedalus_core::sync::SyncGroup>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, daedalus_data::model::Value>,
}

/// A schedulable segment (may group GPU-required nodes).
///
/// ```
/// use daedalus_runtime::RuntimeSegment;
/// use daedalus_planner::{ComputeAffinity, NodeRef};
/// let seg = RuntimeSegment { nodes: vec![NodeRef(0)], compute: ComputeAffinity::CpuOnly };
/// assert_eq!(seg.nodes.len(), 1);
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimeSegment {
    pub nodes: Vec<NodeRef>,
    pub compute: ComputeAffinity,
}

/// Final runtime plan, derived from planner output.
///
/// ```
/// use daedalus_runtime::RuntimePlan;
/// use daedalus_planner::{ExecutionPlan, Graph};
/// let plan = RuntimePlan::from_execution(&ExecutionPlan::new(Graph::default(), vec![]));
/// assert!(plan.nodes.is_empty());
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimePlan {
    pub default_policy: EdgePolicyKind,
    pub backpressure: BackpressureStrategy,
    /// Prefer lock-free bounded edge queues when available.
    ///
    /// This is a runtime knob; it should not appear in serialized runtime plans.
    #[serde(skip)]
    pub lockfree_queues: bool,
    /// Graph-level metadata (typed values) propagated into `ExecutionContext.graph_metadata`.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub graph_metadata: std::collections::BTreeMap<String, daedalus_data::model::Value>,
    pub nodes: Vec<RuntimeNode>,
    pub edges: Vec<(NodeRef, String, NodeRef, String, EdgePolicyKind)>,
    pub gpu_segments: Vec<GpuSegment>,
    pub gpu_edges: Vec<EdgeBufferInfo>,
    /// Edges that enter a GPU segment (CPU->GPU).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gpu_entries: Vec<usize>,
    /// Edges that leave a GPU segment to CPU consumers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gpu_exits: Vec<usize>,
    pub segments: Vec<RuntimeSegment>,
    pub schedule_order: Vec<NodeRef>,
}

/// Select a subset of graph outputs to compute (demand-driven execution).
///
/// `node` matches the runtime node via index, id, or metadata.
/// If `port` is provided, only edges that feed that input port of the sink node are followed
/// when building the upstream closure (useful for `io.host_output` nodes that have multiple inputs).
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct RuntimeSink {
    #[serde(default)]
    pub node: GraphNodeSelector,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<String>,
}

impl RuntimePlan {
    /// Convert a planner execution plan into a runtime plan.
    pub fn from_execution(plan: &ExecutionPlan) -> Self {
        let (gpu_segments, gpu_edges) = plan.graph.gpu_buffers();
        let (gpu_entries, gpu_exits) = {
            let mut entries = Vec::new();
            let mut exits = Vec::new();
            for info in &gpu_edges {
                if !info.gpu_fast_path {
                    // If the source is GPU and target is CPU, it's an exit; if source CPU and target GPU, entry.
                    let from =
                        plan.graph.nodes[plan.graph.edges[info.edge_index].from.node.0].compute;
                    let to = plan.graph.nodes[plan.graph.edges[info.edge_index].to.node.0].compute;
                    let from_gpu = matches!(
                        from,
                        ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired
                    );
                    let to_gpu = matches!(
                        to,
                        ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired
                    );
                    if !from_gpu && to_gpu {
                        entries.push(info.edge_index);
                    }
                    if from_gpu && !to_gpu {
                        exits.push(info.edge_index);
                    }
                }
            }
            (entries, exits)
        };
        let nodes: Vec<RuntimeNode> = plan
            .graph
            .nodes
            .iter()
            .map(|n| RuntimeNode {
                id: n.id.0.clone(),
                stable_id: daedalus_core::stable_id::stable_id128("node", &n.id.0),
                bundle: n.bundle.clone(),
                label: n.label.clone(),
                compute: n.compute,
                const_inputs: n.const_inputs.clone(),
                sync_groups: n.sync_groups.clone(),
                metadata: n.metadata.clone(),
            })
            .collect();
        // Collision check: if this ever triggers, it indicates a stable-id hashing collision.
        // We treat it as a hard error to avoid silent handler dispatch mismatches.
        {
            let mut seen: std::collections::HashMap<u128, String> =
                std::collections::HashMap::new();
            for n in &nodes {
                if let Some(prev) = seen.insert(n.stable_id, n.id.clone())
                    && prev != n.id
                {
                    panic!(
                        "daedalus-runtime: stable_id collision: id='{}' and id='{}' map to {:x}",
                        prev, n.id, n.stable_id
                    );
                }
            }
        }
        let edges: Vec<(NodeRef, String, NodeRef, String, EdgePolicyKind)> = plan
            .graph
            .edges
            .iter()
            .map(|e| {
                (
                    e.from.node,
                    e.from.port.clone(),
                    e.to.node,
                    e.to.port.clone(),
                    EdgePolicyKind::Fifo,
                )
            })
            .collect();

        let mut order: Vec<NodeRef> = Vec::new();
        if let Some(order_str) =
            plan.graph
                .metadata
                .get("schedule_order")
                .and_then(|value| match value {
                    daedalus_data::model::Value::String(s) => Some(s.to_string()),
                    _ => None,
                })
        {
            let mut by_id: std::collections::HashMap<&str, usize> =
                std::collections::HashMap::new();
            for (idx, node) in plan.graph.nodes.iter().enumerate() {
                by_id.insert(node.id.0.as_str(), idx);
            }
            let mut seen = vec![false; plan.graph.nodes.len()];
            for id in order_str
                .split(',')
                .map(str::trim)
                .filter(|v| !v.is_empty())
            {
                if let Some(idx) = by_id.get(id).copied()
                    && !seen[idx]
                {
                    seen[idx] = true;
                    order.push(NodeRef(idx));
                }
            }
            for (idx, was_seen) in seen.iter().enumerate() {
                if !*was_seen {
                    order.push(NodeRef(idx));
                }
            }
        } else {
            order = (0..plan.graph.nodes.len()).map(NodeRef).collect();
        }
        // Runtime execution must respect data dependencies. Legacy behavior iterated nodes in
        // declaration order, which can run consumers before producers and cause widespread
        // "sync_groups + no inputs" stalls in grouped graphs.
        //
        // Host bridge nodes are executed in dedicated pre/post passes by the executor, so we
        // intentionally ignore host-node edges while deriving execution order for regular nodes.
        let host_nodes: Vec<bool> = plan
            .graph
            .nodes
            .iter()
            .map(|n| n.id.0.ends_with("io.host_bridge") || n.id.0.ends_with("io.host_output"))
            .collect();
        order = dependency_order(plan.graph.nodes.len(), &edges, &order, &host_nodes);

        // Simple segmentation: group consecutive GPU-pref/required nodes into a single segment,
        // leave CPU-only nodes as singletons. This is a placeholder until planner emits segments.
        let mut segments = Vec::new();
        let mut current_gpu: Option<RuntimeSegment> = None;
        for node_ref in &order {
            let node = &plan.graph.nodes[node_ref.0];
            match node.compute {
                ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired => {
                    if let Some(seg) = &mut current_gpu {
                        seg.nodes.push(*node_ref);
                        if matches!(node.compute, ComputeAffinity::GpuRequired) {
                            seg.compute = ComputeAffinity::GpuRequired;
                        }
                    } else {
                        current_gpu = Some(RuntimeSegment {
                            nodes: vec![*node_ref],
                            compute: node.compute,
                        });
                    }
                }
                ComputeAffinity::CpuOnly => {
                    if let Some(seg) = current_gpu.take() {
                        segments.push(seg);
                    }
                    segments.push(RuntimeSegment {
                        nodes: vec![*node_ref],
                        compute: ComputeAffinity::CpuOnly,
                    });
                }
            }
        }
        if let Some(seg) = current_gpu.take() {
            segments.push(seg);
        }

        let graph_metadata = plan.graph.metadata.clone();

        RuntimePlan {
            default_policy: EdgePolicyKind::Fifo,
            backpressure: BackpressureStrategy::None,
            lockfree_queues: false,
            graph_metadata,
            nodes,
            edges,
            gpu_segments,
            gpu_edges,
            gpu_entries,
            gpu_exits,
            segments,
            schedule_order: order,
        }
    }

    /// Compute an "active nodes" mask for demand-driven execution given a set of sinks.
    ///
    /// The returned mask can be passed to `Executor::with_active_nodes` to skip unrelated branches.
    pub fn active_nodes_for_sinks(&self, sinks: &[RuntimeSink]) -> Result<Vec<bool>, String> {
        active_nodes_mask_for_sinks(&self.nodes, &self.edges, sinks)
    }
}

fn dependency_order(
    node_count: usize,
    edges: &[(NodeRef, String, NodeRef, String, EdgePolicyKind)],
    preferred: &[NodeRef],
    host_nodes: &[bool],
) -> Vec<NodeRef> {
    if node_count == 0 {
        return Vec::new();
    }

    let mut rank = vec![usize::MAX; node_count];
    for (i, node_ref) in preferred.iter().enumerate() {
        if node_ref.0 < node_count && rank[node_ref.0] == usize::MAX {
            rank[node_ref.0] = i;
        }
    }
    let base = preferred.len();
    for (idx, r) in rank.iter_mut().enumerate() {
        if *r == usize::MAX {
            *r = base + idx;
        }
    }

    let mut indegree = vec![0usize; node_count];
    let mut outgoing: Vec<Vec<usize>> = vec![Vec::new(); node_count];
    for (from, _, to, _, _) in edges {
        if from.0 >= node_count || to.0 >= node_count {
            continue;
        }
        if host_nodes.get(from.0).copied().unwrap_or(false)
            || host_nodes.get(to.0).copied().unwrap_or(false)
        {
            continue;
        }
        indegree[to.0] = indegree[to.0].saturating_add(1);
        outgoing[from.0].push(to.0);
    }

    let mut ready: BTreeSet<(usize, usize)> = BTreeSet::new();
    for idx in 0..node_count {
        if indegree[idx] == 0 {
            ready.insert((rank[idx], idx));
        }
    }

    let mut ordered: Vec<NodeRef> = Vec::with_capacity(node_count);
    let mut visited = vec![false; node_count];
    while let Some((_, idx)) = ready.iter().next().copied() {
        ready.remove(&(rank[idx], idx));
        if visited[idx] {
            continue;
        }
        visited[idx] = true;
        ordered.push(NodeRef(idx));
        for &dst in &outgoing[idx] {
            if indegree[dst] > 0 {
                indegree[dst] -= 1;
                if indegree[dst] == 0 {
                    ready.insert((rank[dst], dst));
                }
            }
        }
    }

    if ordered.len() == node_count {
        return ordered;
    }

    // Preserve deterministic behavior for cyclic graphs by appending unresolved nodes
    // in preferred declaration order.
    let mut remaining: Vec<usize> = (0..node_count).filter(|idx| !visited[*idx]).collect();
    remaining.sort_by_key(|idx| (rank[*idx], *idx));
    for idx in remaining {
        ordered.push(NodeRef(idx));
    }
    ordered
}

/// Compute a node-activity mask for demand-driven execution given a set of sinks.
///
/// The returned mask can be passed to `Executor::with_active_nodes` to skip unrelated branches.
pub fn active_nodes_mask_for_sinks(
    nodes: &[RuntimeNode],
    edges: &[(NodeRef, String, NodeRef, String, EdgePolicyKind)],
    sinks: &[RuntimeSink],
) -> Result<Vec<bool>, String> {
    if sinks.is_empty() {
        return Ok(vec![true; nodes.len()]);
    }

    fn resolve_indices(nodes: &[RuntimeNode], selector: &GraphNodeSelector) -> Vec<usize> {
        if let Some(index) = selector.index {
            if index < nodes.len() {
                return vec![index];
            }
            return Vec::new();
        }
        if let Some(meta) = selector.metadata.as_ref() {
            let key = meta.key.trim();
            if !key.is_empty() {
                return nodes
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, node)| {
                        node.metadata
                            .get(key)
                            .filter(|value| *value == &meta.value)
                            .map(|_| idx)
                    })
                    .collect();
            }
        }
        if let Some(id) = selector.id.as_ref() {
            let trimmed = id.trim();
            if !trimmed.is_empty() {
                return nodes
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, node)| (node.id == trimmed).then_some(idx))
                    .collect();
            }
        }
        Vec::new()
    }

    let mut incoming_edges: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
    for (edge_idx, (_from, _from_port, to, _to_port, _policy)) in edges.iter().enumerate() {
        if to.0 < incoming_edges.len() {
            incoming_edges[to.0].push(edge_idx);
        }
    }

    let mut active = vec![false; nodes.len()];
    // Only apply the port filter at the sink node (initial cut). A sink node may be selected
    // through multiple input ports (for example `io.host_output` with `overlay` plus `tx/ty`),
    // so we need to preserve the union of requested ports instead of letting later entries
    // overwrite earlier ones.
    enum SinkPortFilter {
        Any,
        Ports(BTreeSet<String>),
    }
    let mut edge_port_filter: std::collections::HashMap<usize, SinkPortFilter> =
        std::collections::HashMap::new();
    let mut q: VecDeque<usize> = VecDeque::new();

    for sink in sinks {
        let indices = resolve_indices(nodes, &sink.node);
        if indices.is_empty() {
            return Err("runtime sink selector did not match any nodes".into());
        }
        for idx in indices {
            if let Some(port) = sink.port.as_ref() {
                let trimmed = port.trim();
                if !trimmed.is_empty() {
                    match edge_port_filter.entry(idx) {
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            match entry.get_mut() {
                                SinkPortFilter::Any => {}
                                SinkPortFilter::Ports(ports) => {
                                    ports.insert(trimmed.to_string());
                                }
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            let mut ports = BTreeSet::new();
                            ports.insert(trimmed.to_string());
                            entry.insert(SinkPortFilter::Ports(ports));
                        }
                    }
                }
            } else {
                edge_port_filter.insert(idx, SinkPortFilter::Any);
            }
            if !active[idx] {
                active[idx] = true;
                q.push_back(idx);
            }
        }
    }

    while let Some(node_idx) = q.pop_front() {
        let filter_ports = edge_port_filter.get(&node_idx);
        for &eidx in incoming_edges
            .get(node_idx)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
        {
            let (from, _from_port, _to, to_port, _policy) = &edges[eidx];
            if let Some(SinkPortFilter::Ports(filter_ports)) = filter_ports {
                if !filter_ports
                    .iter()
                    .any(|port| to_port.eq_ignore_ascii_case(port))
                {
                    continue;
                }
            }
            let src = from.0;
            if src < active.len() && !active[src] {
                active[src] = true;
                q.push_back(src);
            }
        }
    }

    Ok(active)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_node(id: &str) -> RuntimeNode {
        RuntimeNode {
            id: id.to_string(),
            stable_id: 0,
            bundle: None,
            label: None,
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        }
    }

    #[test]
    fn active_nodes_mask_keeps_multiple_sink_ports_on_same_node() {
        let nodes = vec![
            test_node("overlay_src"),
            test_node("value_src"),
            test_node("io.host_output"),
        ];
        let edges = vec![
            (
                NodeRef(0),
                "frame".to_string(),
                NodeRef(2),
                "overlay".to_string(),
                EdgePolicyKind::Fifo,
            ),
            (
                NodeRef(1),
                "value".to_string(),
                NodeRef(2),
                "tx".to_string(),
                EdgePolicyKind::Fifo,
            ),
        ];
        let sinks = vec![
            RuntimeSink {
                node: GraphNodeSelector {
                    index: Some(2),
                    id: None,
                    metadata: None,
                },
                port: Some("overlay".to_string()),
            },
            RuntimeSink {
                node: GraphNodeSelector {
                    index: Some(2),
                    id: None,
                    metadata: None,
                },
                port: Some("tx".to_string()),
            },
        ];

        let active = active_nodes_mask_for_sinks(&nodes, &edges, &sinks).expect("active mask");
        assert_eq!(active, vec![true, true, true]);
    }
}
