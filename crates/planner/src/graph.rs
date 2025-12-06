use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::diagnostics::Diagnostic;

/// Default execution-plan version for deterministic serde/goldens.
pub const DEFAULT_PLAN_VERSION: &str = "0.1";

/// Compute affinity hint for scheduling/GPU pass.
pub use daedalus_core::compute::ComputeAffinity;
/// Sync grouping metadata.
#[allow(unused_imports)]
pub use daedalus_core::sync::{SyncGroup, SyncPolicy};

/// Stable hash helper used for goldens; simple FNV-1a for determinism.
///
/// ```
/// use daedalus_planner::StableHash;
/// let hash = StableHash::from_bytes(b"demo");
/// assert_ne!(hash.0, 0);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StableHash(pub u64);

impl StableHash {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;
        let mut hash = FNV_OFFSET;
        for b in bytes {
            hash ^= *b as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        StableHash(hash)
    }
}

/// Node reference within a graph (index-based for compactness).
///
/// ```
/// use daedalus_planner::NodeRef;
/// let node = NodeRef(3);
/// assert_eq!(node.0, 3);
/// ```
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeRef(pub usize);

/// Port reference by name within a node.
///
/// ```
/// use daedalus_planner::{NodeRef, PortRef};
/// let port = PortRef { node: NodeRef(0), port: "out".into() };
/// assert_eq!(port.port, "out");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PortRef {
    pub node: NodeRef,
    pub port: String,
}

/// Edge from one node/port to another.
///
/// ```
/// use daedalus_planner::{Edge, NodeRef, PortRef};
/// let edge = Edge {
///     from: PortRef { node: NodeRef(0), port: "out".into() },
///     to: PortRef { node: NodeRef(1), port: "in".into() },
///     metadata: Default::default(),
/// };
/// assert_eq!(edge.from.port, "out");
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub from: PortRef,
    pub to: PortRef,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, daedalus_data::model::Value>,
}

/// An instantiated node, identified by registry id.
///
/// ```
/// use daedalus_planner::{ComputeAffinity, NodeInstance};
/// use daedalus_registry::ids::NodeId;
/// let node = NodeInstance {
///     id: NodeId::new("demo.node"),
///     bundle: None,
///     label: None,
///     inputs: vec![],
///     outputs: vec![],
///     compute: ComputeAffinity::CpuOnly,
///     const_inputs: vec![],
///     sync_groups: vec![],
///     metadata: Default::default(),
/// };
/// assert_eq!(node.id.0, "demo.node");
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeInstance {
    pub id: daedalus_registry::ids::NodeId,
    pub bundle: Option<String>,
    pub label: Option<String>,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    #[serde(default)]
    pub compute: ComputeAffinity,
    #[serde(default)]
    pub const_inputs: Vec<(String, daedalus_data::model::Value)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sync_groups: Vec<SyncGroup>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, daedalus_data::model::Value>,
}

/// Planner input graph (pre-pass).
///
/// ```
/// use daedalus_planner::Graph;
/// let graph = Graph::default();
/// assert!(graph.nodes.is_empty());
/// ```
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Graph {
    pub nodes: Vec<NodeInstance>,
    pub edges: Vec<Edge>,
    /// Arbitrary metadata retained for goldens/debug.
    pub metadata: BTreeMap<String, String>,
    /// Graph-level metadata (typed values) that should be visible to nodes at runtime.
    ///
    /// This is distinct from `metadata` (string-only planner debug/golden output). Planner passes
    /// may write to `metadata`; graph authors should write to `metadata_values`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata_values: BTreeMap<String, daedalus_data::model::Value>,
}

/// Contiguous GPU segment metadata.
///
/// ```
/// use daedalus_planner::{GpuSegment, NodeRef};
/// let seg = GpuSegment { buffer_id: 0, nodes: vec![NodeRef(0)] };
/// assert_eq!(seg.nodes.len(), 1);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuSegment {
    pub buffer_id: usize,
    pub nodes: Vec<NodeRef>,
}

/// Edge buffer hints used by the GPU pass.
///
/// ```
/// use daedalus_planner::EdgeBufferInfo;
/// let info = EdgeBufferInfo { edge_index: 0, gpu_fast_path: false, buffer_id: None };
/// assert_eq!(info.edge_index, 0);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeBufferInfo {
    /// Index into `Graph::edges`.
    pub edge_index: usize,
    /// True when both endpoints are GPU-capable, meaning the edge can reuse a GPU buffer.
    pub gpu_fast_path: bool,
    /// Buffer id used when `gpu_fast_path` is true.
    pub buffer_id: Option<usize>,
}

impl Graph {
    /// Identify contiguous GPU-to-GPU chains and assign them shared buffer ids, along with
    /// edge annotations that mark where GPU fast paths can be used.
    ///
    /// ```
    /// use daedalus_planner::{Graph, NodeInstance, ComputeAffinity, Edge, PortRef, NodeRef};
    /// use daedalus_registry::ids::NodeId;
    ///
    /// let mut graph = Graph::default();
    /// graph.nodes.push(NodeInstance {
    ///     id: NodeId::new("a"),
    ///     bundle: None,
    ///     label: None,
    ///     inputs: vec![],
    ///     outputs: vec!["out".into()],
    ///     compute: ComputeAffinity::GpuPreferred,
    ///     const_inputs: vec![],
    ///     sync_groups: vec![],
    ///     metadata: Default::default(),
    /// });
    /// graph.nodes.push(NodeInstance {
    ///     id: NodeId::new("b"),
    ///     bundle: None,
    ///     label: None,
    ///     inputs: vec!["in".into()],
    ///     outputs: vec![],
    ///     compute: ComputeAffinity::GpuPreferred,
    ///     const_inputs: vec![],
    ///     sync_groups: vec![],
    ///     metadata: Default::default(),
    /// });
    /// graph.edges.push(Edge {
    ///     from: PortRef { node: NodeRef(0), port: "out".into() },
    ///     to: PortRef { node: NodeRef(1), port: "in".into() },
    ///     metadata: Default::default(),
    /// });
    /// let (segments, edges) = graph.gpu_buffers();
    /// assert_eq!(edges.len(), 1);
    /// assert_eq!(segments.len(), 1);
    /// ```
    pub fn gpu_buffers(&self) -> (Vec<GpuSegment>, Vec<EdgeBufferInfo>) {
        #[derive(Clone)]
        struct Dsu {
            parent: Vec<usize>,
        }
        impl Dsu {
            fn new(n: usize) -> Self {
                Self {
                    parent: (0..n).collect(),
                }
            }
            fn find(&mut self, x: usize) -> usize {
                if self.parent[x] != x {
                    let p = self.parent[x];
                    self.parent[x] = self.find(p);
                }
                self.parent[x]
            }
            fn union(&mut self, a: usize, b: usize) {
                let ra = self.find(a);
                let rb = self.find(b);
                if ra != rb {
                    self.parent[rb] = ra;
                }
            }
        }

        let mut dsu = Dsu::new(self.nodes.len());
        for e in &self.edges {
            let from = &self.nodes[e.from.node.0];
            let to = &self.nodes[e.to.node.0];
            let gpu_gpu = matches!(
                from.compute,
                ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired
            ) && matches!(
                to.compute,
                ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired
            );
            if gpu_gpu {
                dsu.union(e.from.node.0, e.to.node.0);
            }
        }

        let mut root_to_buf = BTreeMap::new();
        let mut node_buf: Vec<Option<usize>> = vec![None; self.nodes.len()];
        for (idx, n) in self.nodes.iter().enumerate() {
            if matches!(
                n.compute,
                ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired
            ) {
                let root = dsu.find(idx);
                let buf_id = match root_to_buf.get(&root) {
                    Some(id) => *id,
                    None => {
                        let next = root_to_buf.len();
                        root_to_buf.insert(root, next);
                        next
                    }
                };
                node_buf[idx] = Some(buf_id);
            }
        }

        let mut segments = Vec::new();
        for (root, buf_id) in root_to_buf {
            let mut members: Vec<NodeRef> = self
                .nodes
                .iter()
                .enumerate()
                .filter(|(i, _)| dsu.find(*i) == root)
                .map(|(i, _)| NodeRef(i))
                .collect();
            members.sort_by_key(|nr| nr.0);
            segments.push(GpuSegment {
                buffer_id: buf_id,
                nodes: members,
            });
        }
        segments.sort_by_key(|s| s.buffer_id);

        let mut edges = Vec::new();
        for (i, e) in self.edges.iter().enumerate() {
            let from = &self.nodes[e.from.node.0];
            let to = &self.nodes[e.to.node.0];
            let gpu_gpu = matches!(
                from.compute,
                ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired
            ) && matches!(
                to.compute,
                ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired
            );
            let buffer_id = if gpu_gpu {
                node_buf[e.from.node.0]
            } else {
                None
            };
            edges.push(EdgeBufferInfo {
                edge_index: i,
                gpu_fast_path: gpu_gpu,
                buffer_id,
            });
        }

        (segments, edges)
    }
}

/// Final execution plan with diagnostics and stable hash for goldens.
///
/// ```
/// use daedalus_planner::{ExecutionPlan, Graph};
/// let plan = ExecutionPlan::new(Graph::default(), vec![]);
/// assert_eq!(plan.graph.nodes.len(), 0);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub version: String,
    pub graph: Graph,
    pub diagnostics: Vec<Diagnostic>,
    pub hash: StableHash,
}

impl ExecutionPlan {
    /// Build a plan and compute its stable hash.
    pub fn new(graph: Graph, diagnostics: Vec<Diagnostic>) -> Self {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(graph.nodes.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&(graph.edges.len() as u64).to_le_bytes());
        for n in &graph.nodes {
            bytes.extend_from_slice(n.id.0.as_bytes());
            bytes.push(match n.compute {
                ComputeAffinity::CpuOnly => 0,
                ComputeAffinity::GpuPreferred => 1,
                ComputeAffinity::GpuRequired => 2,
            });
        }
        for e in &graph.edges {
            bytes.extend_from_slice(&(e.from.node.0 as u64).to_le_bytes());
            bytes.extend_from_slice(&(e.to.node.0 as u64).to_le_bytes());
            bytes.extend_from_slice(e.from.port.as_bytes());
            bytes.extend_from_slice(e.to.port.as_bytes());
        }
        let hash = StableHash::from_bytes(&bytes);
        Self {
            version: DEFAULT_PLAN_VERSION.to_string(),
            graph,
            diagnostics,
            hash,
        }
    }
}
