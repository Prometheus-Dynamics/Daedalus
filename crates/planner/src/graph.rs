use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::diagnostics::Diagnostic;

/// Default execution-plan version for deterministic serde/goldens.
pub const DEFAULT_PLAN_VERSION: &str = "0.1";

/// Compute affinity hint for scheduling/GPU pass.
pub use daedalus_core::compute::ComputeAffinity;
/// Sync grouping metadata.
pub use daedalus_core::sync::SyncGroup;

/// Stable hash helper used for goldens; simple FNV-1a for determinism.
///
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StableHash(pub u64);

impl StableHash {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        StableHash(daedalus_core::stable_id::fnv1a64(bytes))
    }
}

pub(crate) fn stable_hash_serialized<T: Serialize + ?Sized>(domain: &str, value: &T) -> StableHash {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(domain.as_bytes());
    bytes.push(0);
    match serde_json::to_vec(value) {
        Ok(serialized) => bytes.extend_from_slice(&serialized),
        Err(error) => {
            bytes.extend_from_slice(b"serde_error");
            bytes.extend_from_slice(error.to_string().as_bytes());
        }
    }
    StableHash::from_bytes(&bytes)
}

/// Node reference within a graph (index-based for compactness).
///
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeRef(pub usize);

/// Port reference by name within a node.
///
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortRef {
    pub node: NodeRef,
    pub port: String,
}

/// Edge from one node/port to another.
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Edge {
    pub from: PortRef,
    pub to: PortRef,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, daedalus_data::model::Value>,
}

/// An instantiated node, identified by registry id.
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Graph {
    pub nodes: Vec<NodeInstance>,
    pub edges: Vec<Edge>,
    /// Graph-level metadata (typed values) that should be visible to nodes at runtime.
    ///
    /// Stored as plain JSON in persisted graphs (no tagged `type/value` wrappers).
    #[serde(default, with = "graph_metadata_serde")]
    pub metadata: BTreeMap<String, daedalus_data::model::Value>,
}

mod graph_metadata_serde {
    use super::*;
    use daedalus_data::json::{from_plain_json, to_plain_json};
    use daedalus_data::model::Value;
    use serde::{Deserializer, Serializer};
    use serde_json::Value as JsonValue;

    pub fn serialize<S>(value: &BTreeMap<String, Value>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value
            .iter()
            .map(|(k, v)| (k.clone(), to_plain_json(v)))
            .collect::<serde_json::Map<_, _>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<String, Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        BTreeMap::<String, JsonValue>::deserialize(deserializer)?
            .into_iter()
            .map(|(k, v)| Ok((k, from_plain_json(&v).map_err(serde::de::Error::custom)?)))
            .collect()
    }
}

/// Contiguous GPU segment metadata.
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuSegment {
    pub buffer_id: usize,
    pub nodes: Vec<NodeRef>,
}

/// Edge buffer hints used by the GPU pass.
///
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
    pub fn stable_hash(&self) -> StableHash {
        stable_hash_serialized("daedalus_planner::Graph", self)
    }

    /// Identify contiguous GPU-to-GPU chains and assign them shared buffer ids, along with
    /// edge annotations that mark where GPU fast paths can be used.
    ///
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
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub version: String,
    pub graph: Graph,
    pub diagnostics: Vec<Diagnostic>,
    pub hash: StableHash,
}

#[derive(Serialize)]
struct ExecutionPlanHashInput<'a> {
    version: &'a str,
    graph: &'a Graph,
    diagnostics: &'a [Diagnostic],
}

impl ExecutionPlan {
    /// Build a plan and compute its stable hash.
    pub fn new(graph: Graph, diagnostics: Vec<Diagnostic>) -> Self {
        let version = DEFAULT_PLAN_VERSION.to_string();
        let hash = stable_hash_serialized(
            "daedalus_planner::ExecutionPlan",
            &ExecutionPlanHashInput {
                version: &version,
                graph: &graph,
                diagnostics: &diagnostics,
            },
        );
        Self {
            version,
            graph,
            diagnostics,
            hash,
        }
    }
}
