use crate::graph::Graph;
use daedalus_data::model::Value as DaedalusValue;
use daedalus_registry::ids::NodeId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct GraphPatch {
    #[serde(default = "default_patch_version")]
    pub version: u32,
    #[serde(default)]
    pub ops: Vec<GraphPatchOp>,
}

fn default_patch_version() -> u32 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GraphPatchOp {
    SetNodeConst {
        node: GraphNodeSelector,
        port: String,
        #[serde(default)]
        value: Option<DaedalusValue>,
    },
    ReplaceNodeId {
        node: GraphNodeSelector,
        new_id: String,
    },
    DeleteNodes {
        node: GraphNodeSelector,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct GraphNodeSelector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<GraphMetadataSelector>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GraphMetadataSelector {
    pub key: String,
    pub value: DaedalusValue,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PatchReport {
    pub applied_ops: usize,
    pub skipped_ops: usize,
    pub matched_nodes: usize,
}

impl GraphPatch {
    pub fn apply_to_graph(&self, graph: &mut Graph) -> PatchReport {
        let mut report = PatchReport::default();
        for op in &self.ops {
            match op {
                GraphPatchOp::SetNodeConst { node, port, value } => {
                    let indices = resolve_graph_indices(graph, node);
                    if indices.is_empty() {
                        report.skipped_ops += 1;
                        continue;
                    }
                    let normalized_port = normalize_port(port);
                    for idx in indices {
                        if let Some(node) = graph.nodes.get_mut(idx) {
                            apply_const_override(
                                &mut node.const_inputs,
                                &normalized_port,
                                port,
                                value,
                            );
                            report.matched_nodes += 1;
                        }
                    }
                    report.applied_ops += 1;
                }
                GraphPatchOp::ReplaceNodeId { node, new_id } => {
                    let indices = resolve_graph_indices(graph, node);
                    if indices.is_empty() {
                        report.skipped_ops += 1;
                        continue;
                    }
                    let trimmed = new_id.trim();
                    if trimmed.is_empty() {
                        report.skipped_ops += 1;
                        continue;
                    }
                    for idx in indices {
                        if let Some(node) = graph.nodes.get_mut(idx) {
                            node.id = NodeId::new(trimmed.to_string());
                            report.matched_nodes += 1;
                        }
                    }
                    report.applied_ops += 1;
                }
                GraphPatchOp::DeleteNodes { node } => {
                    let indices = resolve_graph_indices(graph, node);
                    if indices.is_empty() {
                        report.skipped_ops += 1;
                        continue;
                    }
                    let n = graph.nodes.len();
                    let mut remove = vec![false; n];
                    for idx in indices {
                        if idx < n && !remove[idx] {
                            remove[idx] = true;
                            report.matched_nodes += 1;
                        }
                    }
                    let mut remap: Vec<Option<usize>> = vec![None; n];
                    let mut new_nodes = Vec::with_capacity(n.saturating_sub(report.matched_nodes));
                    for (old_idx, node) in graph.nodes.iter().enumerate() {
                        if remove[old_idx] {
                            continue;
                        }
                        let new_idx = new_nodes.len();
                        new_nodes.push(node.clone());
                        remap[old_idx] = Some(new_idx);
                    }
                    let mut new_edges = Vec::with_capacity(graph.edges.len());
                    for edge in &graph.edges {
                        let Some(from) = remap.get(edge.from.node.0).and_then(|v| *v) else {
                            continue;
                        };
                        let Some(to) = remap.get(edge.to.node.0).and_then(|v| *v) else {
                            continue;
                        };
                        let mut cloned = edge.clone();
                        cloned.from.node.0 = from;
                        cloned.to.node.0 = to;
                        new_edges.push(cloned);
                    }
                    graph.nodes = new_nodes;
                    graph.edges = new_edges;
                    report.applied_ops += 1;
                }
            }
        }
        report
    }
}

fn resolve_graph_indices(graph: &Graph, selector: &GraphNodeSelector) -> Vec<usize> {
    if let Some(index) = selector.index {
        if index < graph.nodes.len() {
            return vec![index];
        }
        return Vec::new();
    }

    if let Some(meta) = selector.metadata.as_ref() {
        let key = meta.key.trim();
        if !key.is_empty() {
            return graph
                .nodes
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
            return graph
                .nodes
                .iter()
                .enumerate()
                .filter_map(|(idx, node)| (node.id.0 == trimmed).then_some(idx))
                .collect();
        }
    }

    Vec::new()
}

fn normalize_port(port: &str) -> String {
    port.trim().to_ascii_lowercase()
}

fn apply_const_override(
    const_inputs: &mut Vec<(String, DaedalusValue)>,
    normalized_port: &str,
    port: &str,
    value: &Option<DaedalusValue>,
) {
    let mut matched = None;
    for (idx, (name, _)) in const_inputs.iter().enumerate() {
        if normalize_port(name) == normalized_port {
            matched = Some(idx);
            break;
        }
    }

    match (matched, value) {
        (Some(idx), Some(next)) => {
            const_inputs[idx] = (const_inputs[idx].0.clone(), next.clone());
        }
        (Some(idx), None) => {
            const_inputs.remove(idx);
        }
        (None, Some(next)) => {
            let key = if port.trim().is_empty() {
                normalized_port.to_string()
            } else {
                port.trim().to_string()
            };
            const_inputs.push((key, next.clone()));
        }
        (None, None) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{ComputeAffinity, NodeInstance};

    #[test]
    fn apply_patch_sets_const_by_metadata() {
        let mut graph = Graph::default();
        let mut node = NodeInstance {
            id: NodeId::new("demo.node"),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec![],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        };
        node.metadata.insert(
            "helios.ui.node_id".to_string(),
            DaedalusValue::String("node-1".into()),
        );
        graph.nodes.push(node);

        let patch = GraphPatch {
            version: 1,
            ops: vec![GraphPatchOp::SetNodeConst {
                node: GraphNodeSelector {
                    metadata: Some(GraphMetadataSelector {
                        key: "helios.ui.node_id".to_string(),
                        value: DaedalusValue::String("node-1".into()),
                    }),
                    ..Default::default()
                },
                port: "threshold".to_string(),
                value: Some(DaedalusValue::Int(5)),
            }],
        };

        let report = patch.apply_to_graph(&mut graph);
        assert_eq!(report.applied_ops, 1);
        assert_eq!(graph.nodes.len(), 1);
        assert_eq!(graph.nodes[0].const_inputs.len(), 1);
        assert_eq!(graph.nodes[0].const_inputs[0].0, "threshold");
    }

    #[test]
    fn apply_patch_replaces_node_id() {
        let mut graph = Graph::default();
        graph.nodes.push(NodeInstance {
            id: NodeId::new("demo.old"),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec![],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });

        let patch = GraphPatch {
            version: 1,
            ops: vec![GraphPatchOp::ReplaceNodeId {
                node: GraphNodeSelector {
                    id: Some("demo.old".to_string()),
                    ..Default::default()
                },
                new_id: "demo.new".to_string(),
            }],
        };

        let report = patch.apply_to_graph(&mut graph);
        assert_eq!(report.applied_ops, 1);
        assert_eq!(graph.nodes[0].id.0, "demo.new");
    }

    #[test]
    fn apply_patch_deletes_nodes_and_remaps_edges() {
        let mut graph = Graph::default();
        graph.nodes.push(NodeInstance {
            id: NodeId::new("a"),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec!["out".into()],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
        graph.nodes.push(NodeInstance {
            id: NodeId::new("b"),
            bundle: None,
            label: None,
            inputs: vec!["in".into()],
            outputs: vec![],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
        graph.edges.push(crate::graph::Edge {
            from: crate::graph::PortRef {
                node: crate::graph::NodeRef(0),
                port: "out".into(),
            },
            to: crate::graph::PortRef {
                node: crate::graph::NodeRef(1),
                port: "in".into(),
            },
            metadata: Default::default(),
        });

        let patch = GraphPatch {
            version: 1,
            ops: vec![GraphPatchOp::DeleteNodes {
                node: GraphNodeSelector {
                    id: Some("a".to_string()),
                    ..Default::default()
                },
            }],
        };

        let report = patch.apply_to_graph(&mut graph);
        assert_eq!(report.applied_ops, 1);
        assert_eq!(report.matched_nodes, 1);
        assert_eq!(graph.nodes.len(), 1);
        assert_eq!(graph.nodes[0].id.0, "b");
        assert_eq!(graph.edges.len(), 0);
    }
}
