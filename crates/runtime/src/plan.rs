use serde::{Deserialize, Serialize};

pub use daedalus_core::policy::BackpressureStrategy;
use daedalus_planner::{ComputeAffinity, EdgeBufferInfo, ExecutionPlan, GpuSegment, NodeRef};

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
        let nodes = plan
            .graph
            .nodes
            .iter()
            .map(|n| RuntimeNode {
                id: n.id.0.clone(),
                bundle: n.bundle.clone(),
                label: n.label.clone(),
                compute: n.compute,
                const_inputs: n.const_inputs.clone(),
                sync_groups: n.sync_groups.clone(),
                metadata: n.metadata.clone(),
            })
            .collect();
        let edges = plan
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
        if let Some(order_str) = plan.graph.metadata.get("schedule_order") {
            let mut by_id: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
            for (idx, node) in plan.graph.nodes.iter().enumerate() {
                by_id.insert(node.id.0.as_str(), idx);
            }
            let mut seen = vec![false; plan.graph.nodes.len()];
            for id in order_str.split(',').map(str::trim).filter(|v| !v.is_empty()) {
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

        let graph_metadata = plan.graph.metadata_values.clone();

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
}
