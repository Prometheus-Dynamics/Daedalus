mod demand;
mod explain;
mod policy;
mod transport_parse;
mod transports;

use serde::{Deserialize, Serialize};

use crate::handles::PortId;
use daedalus_core::metadata::{PLAN_GPU_SEGMENTS_KEY, PLAN_SCHEDULE_ORDER_KEY};
pub use daedalus_core::policy::BackpressureStrategy;
use daedalus_data::model::Value;
use daedalus_planner::{
    ComputeAffinity, EdgeBufferInfo, ExecutionPlan, GpuSegment, GraphNodeSelector, NodeRef,
    is_host_bridge_metadata,
};
pub use daedalus_registry::capability::{NODE_EXECUTION_KIND_META_KEY, NodeExecutionKind};
use daedalus_transport::PressurePolicy;
pub(crate) use demand::active_nodes_mask_for_sinks;
pub use demand::{DemandError, DemandSlice, DemandSliceEntry, DemandTelemetry};
pub use explain::{
    RuntimeBranchExplanation, RuntimeEdgeExplanation, RuntimeEdgeHandoff, RuntimeNodeExplanation,
    RuntimePlanExplanation,
};
use policy::edge_policy_from_metadata;
pub use policy::{
    EDGE_CAPACITY_KEY, EDGE_FRESHNESS_POLICY_KEY, EDGE_PRESSURE_BOUNDED, EDGE_PRESSURE_COALESCE,
    EDGE_PRESSURE_DROP_NEWEST, EDGE_PRESSURE_DROP_OLDEST, EDGE_PRESSURE_ERROR_ON_FULL,
    EDGE_PRESSURE_FIFO, EDGE_PRESSURE_LATEST_ONLY, EDGE_PRESSURE_POLICY_KEY, RuntimeEdgePolicy,
};
use std::collections::{BTreeMap, BTreeSet};

pub fn node_execution_kind_from_metadata(
    metadata: &std::collections::BTreeMap<String, Value>,
) -> NodeExecutionKind {
    if is_host_bridge_metadata(metadata) {
        return NodeExecutionKind::HostBridge;
    }
    NodeExecutionKind::from_metadata(metadata).unwrap_or_default()
}

pub fn runtime_node_execution_kind(node: &RuntimeNode) -> NodeExecutionKind {
    node_execution_kind_from_metadata(&node.metadata)
}

fn metadata_string_list(value: Option<&Value>) -> Option<Vec<String>> {
    let Value::List(items) = value? else {
        return None;
    };
    Some(
        items
            .iter()
            .filter_map(|item| match item {
                Value::String(value) => Some(value.to_string()),
                _ => None,
            })
            .collect(),
    )
}

fn metadata_string_matrix(value: Option<&Value>) -> Option<Vec<Vec<String>>> {
    let Value::List(rows) = value? else {
        return None;
    };
    Some(
        rows.iter()
            .filter_map(|row| metadata_string_list(Some(row)))
            .filter(|row| !row.is_empty())
            .collect(),
    )
}

/// Typed internal runtime edge.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEdge {
    from: NodeRef,
    source_port: PortId,
    to: NodeRef,
    target_port: PortId,
    policy: RuntimeEdgePolicy,
}

impl RuntimeEdge {
    pub fn new(
        from: NodeRef,
        from_port: impl Into<String>,
        to: NodeRef,
        to_port: impl Into<String>,
        policy: RuntimeEdgePolicy,
    ) -> Self {
        Self {
            from,
            source_port: PortId::new(from_port),
            to,
            target_port: PortId::new(to_port),
            policy,
        }
    }

    pub fn from(&self) -> NodeRef {
        self.from
    }

    pub fn source_port(&self) -> &str {
        self.source_port.as_str()
    }

    pub fn source_port_id(&self) -> &PortId {
        &self.source_port
    }

    pub fn to(&self) -> NodeRef {
        self.to
    }

    pub fn target_port(&self) -> &str {
        self.target_port.as_str()
    }

    pub fn target_port_id(&self) -> &PortId {
        &self.target_port
    }

    pub fn policy(&self) -> &RuntimeEdgePolicy {
        &self.policy
    }

    pub fn policy_mut(&mut self) -> &mut RuntimeEdgePolicy {
        &mut self.policy
    }

    pub(crate) fn source_key(&self) -> (usize, PortId) {
        (self.from.0, self.source_port.clone())
    }

    pub(crate) fn target_key(&self) -> (usize, PortId) {
        (self.to.0, self.target_port.clone())
    }
}

pub(crate) fn direct_edge_mask_for_active_edges(
    edges: &[RuntimeEdge],
    edge_transports: &[Option<RuntimeEdgeTransport>],
    mut edge_active: impl FnMut(usize) -> bool,
) -> Vec<bool> {
    let mut source_port_counts: BTreeMap<(usize, PortId), usize> = BTreeMap::new();
    let mut target_port_counts: BTreeMap<(usize, PortId), usize> = BTreeMap::new();
    for (idx, edge) in edges.iter().enumerate() {
        if !edge_active(idx) {
            continue;
        }
        *source_port_counts.entry(edge.source_key()).or_default() += 1;
        *target_port_counts.entry(edge.target_key()).or_default() += 1;
    }

    edges
        .iter()
        .enumerate()
        .map(|(idx, edge)| {
            if !edge_active(idx) {
                return false;
            }
            let adapter_steps_empty = edge_transports
                .get(idx)
                .and_then(Option::as_ref)
                .map(|transport| transport.adapter_steps.is_empty())
                .unwrap_or(true);
            let direct_policy = matches!(
                edge.policy().pressure,
                PressurePolicy::LatestOnly | PressurePolicy::Coalesce { .. }
            );
            adapter_steps_empty
                && direct_policy
                && source_port_counts
                    .get(&edge.source_key())
                    .copied()
                    .unwrap_or(0)
                    == 1
                && target_port_counts
                    .get(&edge.target_key())
                    .copied()
                    .unwrap_or(0)
                    == 1
        })
        .collect()
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
    pub default_policy: RuntimeEdgePolicy,
    pub backpressure: BackpressureStrategy,
    /// Graph-level metadata (typed values) propagated into `ExecutionContext.graph_metadata`.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub graph_metadata: std::collections::BTreeMap<String, daedalus_data::model::Value>,
    pub nodes: Vec<RuntimeNode>,
    pub edges: Vec<RuntimeEdge>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub edge_transports: Vec<Option<RuntimeEdgeTransport>>,
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
    /// Compile-time demand slices for every host output sink.
    #[serde(default, skip)]
    pub demand_slices: Vec<DemandSliceEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RuntimePlanError {
    #[error("stable id collision: id='{previous}' and id='{current}' map to {stable_id:x}")]
    StableIdCollision {
        previous: String,
        current: String,
        stable_id: u128,
    },
    #[error("unknown edge pressure policy '{policy}' on edge {edge_index}")]
    UnknownEdgePressurePolicy { edge_index: usize, policy: String },
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
    pub port: Option<PortId>,
}

impl PartialEq for RuntimeSink {
    fn eq(&self, other: &Self) -> bool {
        self.port == other.port
            && self.node.index == other.node.index
            && self.node.id == other.node.id
            && match (self.node.metadata.as_ref(), other.node.metadata.as_ref()) {
                (Some(a), Some(b)) => a.key == b.key && a.value == b.value,
                (None, None) => true,
                _ => false,
            }
    }
}

impl Eq for RuntimeSink {}

impl RuntimeSink {
    pub fn node_id(id: impl Into<String>) -> Self {
        Self {
            node: GraphNodeSelector {
                id: Some(id.into()),
                ..GraphNodeSelector::default()
            },
            port: None,
        }
    }

    pub fn node_index(index: usize) -> Self {
        Self {
            node: GraphNodeSelector {
                index: Some(index),
                ..GraphNodeSelector::default()
            },
            port: None,
        }
    }

    pub fn port(mut self, port: impl Into<String>) -> Self {
        self.port = Some(PortId::new(port));
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEdgeTransport {
    pub from_type: daedalus_data::model::TypeExpr,
    pub to_type: daedalus_data::model::TypeExpr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_transport: Option<daedalus_transport::TypeKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_transport: Option<daedalus_transport::TypeKey>,
    #[serde(default)]
    pub target_access: daedalus_transport::AccessMode,
    #[serde(default, skip_serializing_if = "is_false")]
    pub target_exclusive: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_residency: Option<daedalus_transport::Residency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_target: Option<daedalus_transport::TypeKey>,
    pub adapter_steps: Vec<daedalus_transport::AdapterId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub adapter_path: Vec<daedalus_registry::capability::AdapterPathStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_adapter_cost: Option<u64>,
}

impl RuntimePlan {
    /// Convert a planner execution plan into a runtime plan.
    pub fn from_execution(plan: &ExecutionPlan) -> Self {
        Self::try_from_execution(plan).unwrap_or_else(|err| panic!("{err}"))
    }

    /// Fallible conversion from a planner execution plan into a runtime plan.
    pub fn try_from_execution(plan: &ExecutionPlan) -> Result<Self, RuntimePlanError> {
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
        ensure_unique_stable_ids(&nodes)?;
        let edges: Vec<RuntimeEdge> = plan
            .graph
            .edges
            .iter()
            .enumerate()
            .map(|(edge_index, e)| {
                let policy = edge_policy_from_metadata(&e.metadata).map_err(|err| match err {
                    policy::EdgePolicyMetadataError::UnknownPressurePolicy { policy } => {
                        RuntimePlanError::UnknownEdgePressurePolicy { edge_index, policy }
                    }
                })?;
                Ok(RuntimeEdge::new(
                    e.from.node,
                    e.from.port.clone(),
                    e.to.node,
                    e.to.port.clone(),
                    policy,
                ))
            })
            .collect::<Result<_, RuntimePlanError>>()?;
        let edge_transports = transports::runtime_edge_transports(plan, &edges);

        let mut order: Vec<NodeRef> = Vec::new();
        if let Some(order_ids) =
            metadata_string_list(plan.graph.metadata.get(PLAN_SCHEDULE_ORDER_KEY))
        {
            let mut by_id: std::collections::HashMap<&str, usize> =
                std::collections::HashMap::new();
            for (idx, node) in plan.graph.nodes.iter().enumerate() {
                by_id.insert(node.id.0.as_str(), idx);
            }
            let mut seen = vec![false; plan.graph.nodes.len()];
            for id in order_ids {
                if let Some(idx) = by_id.get(id.as_str()).copied()
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
        // Runtime execution must respect data dependencies. Declaration order can run consumers
        // before producers and cause widespread "sync_groups + no inputs" stalls in grouped graphs.
        //
        // Host bridge nodes are executed in dedicated pre/post passes by the executor, so we
        // intentionally ignore host-node edges while deriving execution order for regular nodes.
        let host_nodes: Vec<bool> = plan
            .graph
            .nodes
            .iter()
            .map(|n| is_host_bridge_metadata(&n.metadata))
            .collect();
        order = dependency_order(plan.graph.nodes.len(), &edges, &order, &host_nodes);

        let segments = runtime_segments_from_planner(plan, &order);

        let graph_metadata = plan.graph.metadata.clone();

        let mut runtime = RuntimePlan {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: BackpressureStrategy::None,
            graph_metadata,
            nodes,
            edges,
            edge_transports,
            gpu_segments,
            gpu_edges,
            gpu_entries,
            gpu_exits,
            segments,
            schedule_order: order,
            demand_slices: Vec::new(),
        };
        runtime.demand_slices = demand::build_host_output_demand_slices(&runtime);
        Ok(runtime)
    }
}

fn ensure_unique_stable_ids(nodes: &[RuntimeNode]) -> Result<(), RuntimePlanError> {
    let mut seen: std::collections::HashMap<u128, String> = std::collections::HashMap::new();
    for node in nodes {
        if let Some(previous) = seen.insert(node.stable_id, node.id.clone())
            && previous != node.id
        {
            return Err(RuntimePlanError::StableIdCollision {
                previous,
                current: node.id.clone(),
                stable_id: node.stable_id,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "plan/tests.rs"]
mod tests;

fn is_gpu_compute(compute: ComputeAffinity) -> bool {
    matches!(
        compute,
        ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired
    )
}

fn runtime_segments_from_planner(plan: &ExecutionPlan, order: &[NodeRef]) -> Vec<RuntimeSegment> {
    let mut by_id: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for (idx, node) in plan.graph.nodes.iter().enumerate() {
        by_id.insert(node.id.0.as_str(), idx);
    }

    let mut order_rank = vec![usize::MAX; plan.graph.nodes.len()];
    for (rank, node_ref) in order.iter().enumerate() {
        if let Some(slot) = order_rank.get_mut(node_ref.0) {
            *slot = rank;
        }
    }

    let mut node_to_planner_segment: Vec<Option<usize>> = vec![None; plan.graph.nodes.len()];
    let mut planner_segments: Vec<RuntimeSegment> = Vec::new();
    let planner_segment_ids =
        metadata_string_matrix(plan.graph.metadata.get(PLAN_GPU_SEGMENTS_KEY))
            .unwrap_or_else(|| dependency_gpu_segment_ids(plan));
    for ids in planner_segment_ids {
        let mut refs: Vec<NodeRef> = Vec::new();
        for id in ids {
            let Some(idx) = by_id.get(id.as_str()).copied() else {
                continue;
            };
            if !is_gpu_compute(plan.graph.nodes[idx].compute)
                || node_to_planner_segment[idx].is_some()
            {
                continue;
            }
            refs.push(NodeRef(idx));
        }
        refs.sort_by_key(|node_ref| {
            order_rank
                .get(node_ref.0)
                .copied()
                .unwrap_or(usize::MAX)
                .min(usize::MAX - 1)
        });
        refs.dedup();
        if refs.is_empty() {
            continue;
        }
        let compute = if refs.iter().any(|node_ref| {
            matches!(
                plan.graph.nodes[node_ref.0].compute,
                ComputeAffinity::GpuRequired
            )
        }) {
            ComputeAffinity::GpuRequired
        } else {
            ComputeAffinity::GpuPreferred
        };
        let segment_idx = planner_segments.len();
        for node_ref in &refs {
            node_to_planner_segment[node_ref.0] = Some(segment_idx);
        }
        planner_segments.push(RuntimeSegment {
            nodes: refs,
            compute,
        });
    }

    let mut emitted_planner_segments = vec![false; planner_segments.len()];
    let mut segments = Vec::new();
    for node_ref in order {
        let Some(node) = plan.graph.nodes.get(node_ref.0) else {
            continue;
        };
        if !is_gpu_compute(node.compute) {
            segments.push(RuntimeSegment {
                nodes: vec![*node_ref],
                compute: ComputeAffinity::CpuOnly,
            });
            continue;
        }
        if let Some(segment_idx) = node_to_planner_segment[node_ref.0] {
            if !emitted_planner_segments[segment_idx] {
                emitted_planner_segments[segment_idx] = true;
                segments.push(planner_segments[segment_idx].clone());
            }
        } else {
            segments.push(RuntimeSegment {
                nodes: vec![*node_ref],
                compute: node.compute,
            });
        }
    }
    segments
}

#[derive(Clone, Debug)]
struct SegmentDsu {
    parent: Vec<usize>,
}

impl SegmentDsu {
    fn new(len: usize) -> Self {
        Self {
            parent: (0..len).collect(),
        }
    }

    fn find(&mut self, idx: usize) -> usize {
        if self.parent[idx] != idx {
            self.parent[idx] = self.find(self.parent[idx]);
        }
        self.parent[idx]
    }

    fn union(&mut self, a: usize, b: usize) {
        let root_a = self.find(a);
        let root_b = self.find(b);
        if root_a != root_b {
            self.parent[root_b] = root_a;
        }
    }
}

fn dependency_gpu_segment_ids(plan: &ExecutionPlan) -> Vec<Vec<String>> {
    let mut dsu = SegmentDsu::new(plan.graph.nodes.len());
    for edge in &plan.graph.edges {
        let from = edge.from.node.0;
        let to = edge.to.node.0;
        let Some(from_node) = plan.graph.nodes.get(from) else {
            continue;
        };
        let Some(to_node) = plan.graph.nodes.get(to) else {
            continue;
        };
        if is_gpu_compute(from_node.compute) && is_gpu_compute(to_node.compute) {
            dsu.union(from, to);
        }
    }

    let mut by_root: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for idx in 0..plan.graph.nodes.len() {
        if is_gpu_compute(plan.graph.nodes[idx].compute) {
            let root = dsu.find(idx);
            by_root.entry(root).or_default().push(idx);
        }
    }

    by_root
        .into_values()
        .map(|indices| {
            indices
                .into_iter()
                .map(|idx| plan.graph.nodes[idx].id.0.clone())
                .collect()
        })
        .collect()
}

fn dependency_order(
    node_count: usize,
    edges: &[RuntimeEdge],
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
    for edge in edges {
        let from = edge.from();
        let to = edge.to();
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

fn is_false(value: &bool) -> bool {
    !*value
}
