use daedalus_core::metadata::{EMBEDDED_GRAPH_KEY, EMBEDDED_HOST_KEY};
use daedalus_data::model::Value;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::diagnostics::{Diagnostic, DiagnosticCode};
use crate::graph::{Edge, Graph, NodeInstance, NodeRef, PortRef};
use crate::metadata::GroupMetadata;

use super::{
    PlannerCatalog, PlannerInput, diagnostic_node_id, is_host_bridge, latest_node,
    node_metadata_value,
};

pub(super) fn expand_embedded_graphs(
    input: &mut PlannerInput,
    catalog: &PlannerCatalog,
    diags: &mut Vec<Diagnostic>,
) {
    let trace = std::env::var_os("DAEDALUS_TRACE_EMBEDDED_EXPAND").is_some();
    #[derive(Clone)]
    struct EmbeddedSpec {
        graph: Graph,
        group_id: Option<String>,
        group_label: Option<String>,
        host_label: Option<String>,
    }

    fn parse_embedded(raw: &str, node_id: &str, diags: &mut Vec<Diagnostic>) -> Option<Graph> {
        match serde_json::from_str::<Graph>(raw) {
            Ok(graph) => Some(graph),
            Err(err) => {
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::NodeMissing,
                        format!("embedded graph parse failed: {err}"),
                    )
                    .in_pass("expand_embedded")
                    .at_node(node_id.to_string()),
                );
                None
            }
        }
    }

    let mut embedded_graphs: HashMap<usize, EmbeddedSpec> = HashMap::new();
    for (idx, node) in input.graph.nodes.iter().enumerate() {
        let Some(desc) = latest_node(catalog, &node.id) else {
            continue;
        };
        if let Some(Value::String(raw)) = node_metadata_value(desc, EMBEDDED_GRAPH_KEY) {
            if let Some(graph) = parse_embedded(raw.as_ref(), &diagnostic_node_id(node), diags) {
                embedded_graphs.insert(
                    idx,
                    EmbeddedSpec {
                        graph,
                        group_id: None,
                        group_label: None,
                        host_label: None,
                    },
                );
            }
            continue;
        }
    }

    if embedded_graphs.is_empty() {
        return;
    }

    let mut connected_inputs: HashMap<usize, HashSet<String>> = HashMap::new();
    for edge in &input.graph.edges {
        if embedded_graphs.contains_key(&edge.to.node.0) {
            connected_inputs
                .entry(edge.to.node.0)
                .or_default()
                .insert(edge.to.port.clone());
        }
    }

    #[derive(Clone, Debug)]
    struct EmbeddedMap {
        inputs: BTreeMap<String, Vec<PortRef>>,
        outputs: BTreeMap<String, Vec<PortRef>>,
    }

    let mut new_nodes: Vec<NodeInstance> = Vec::new();
    let mut embedded_internal_edges: Vec<Edge> = Vec::new();
    let mut remap: Vec<Option<usize>> = vec![None; input.graph.nodes.len()];
    let mut embedded_maps: HashMap<usize, EmbeddedMap> = HashMap::new();

    for (idx, node) in input.graph.nodes.iter().enumerate() {
        let Some(spec) = embedded_graphs.get(&idx) else {
            let new_idx = new_nodes.len();
            new_nodes.push(node.clone());
            remap[idx] = Some(new_idx);
            continue;
        };
        let graph = &spec.graph;

        let host_index = graph.nodes.iter().position(is_host_bridge).or_else(|| {
            let host_label = latest_node(catalog, &node.id)
                .and_then(|desc| node_metadata_value(desc, EMBEDDED_HOST_KEY))
                .and_then(|val| match val {
                    Value::String(s) => {
                        let trimmed = s.trim();
                        if trimmed.is_empty() {
                            None
                        } else {
                            Some(trimmed.to_string())
                        }
                    }
                    _ => None,
                });
            let host_label = host_label.or_else(|| spec.host_label.clone());
            host_label.and_then(|label| {
                graph
                    .nodes
                    .iter()
                    .position(|n| n.label.as_deref() == Some(label.as_str()))
            })
        });

        let Some(host_index) = host_index else {
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::NodeMissing,
                    "embedded graph missing host bridge".to_string(),
                )
                .in_pass("expand_embedded")
                .at_node(diagnostic_node_id(node)),
            );
            let new_idx = new_nodes.len();
            new_nodes.push(node.clone());
            remap[idx] = Some(new_idx);
            continue;
        };

        let group_label = node
            .label
            .clone()
            .or_else(|| latest_node(catalog, &node.id).and_then(|desc| desc.label.clone()))
            .or_else(|| spec.group_label.clone())
            .unwrap_or_else(|| node.id.0.clone());
        let group_id = spec.group_id.clone().unwrap_or_else(|| group_label.clone());
        let prefix = format!("{group_label}::");
        let mut index_map: Vec<Option<usize>> = vec![None; graph.nodes.len()];

        for (g_idx, g_node) in graph.nodes.iter().enumerate() {
            if g_idx == host_index {
                continue;
            }
            let mut cloned = g_node.clone();
            let base_label = cloned.label.clone().unwrap_or_else(|| cloned.id.0.clone());
            cloned.label = Some(format!("{prefix}{base_label}"));
            GroupMetadata {
                id: Some(group_id.clone()),
                label: Some(group_label.clone()),
                embedded_group: Some(group_label.clone()),
            }
            .write_to_node_metadata(&mut cloned.metadata);
            let new_idx = new_nodes.len();
            new_nodes.push(cloned);
            index_map[g_idx] = Some(new_idx);
        }

        let mut inputs: BTreeMap<String, Vec<PortRef>> = BTreeMap::new();
        let mut outputs: BTreeMap<String, Vec<PortRef>> = BTreeMap::new();

        for edge in &graph.edges {
            let from_is_host = edge.from.node.0 == host_index;
            let to_is_host = edge.to.node.0 == host_index;

            match (from_is_host, to_is_host) {
                (true, false) => {
                    if let Some(target_idx) = index_map[edge.to.node.0] {
                        inputs
                            .entry(edge.from.port.clone())
                            .or_default()
                            .push(PortRef {
                                node: NodeRef(target_idx),
                                port: edge.to.port.clone(),
                            });
                    }
                }
                (false, true) => {
                    if let Some(source_idx) = index_map[edge.from.node.0] {
                        outputs
                            .entry(edge.to.port.clone())
                            .or_default()
                            .push(PortRef {
                                node: NodeRef(source_idx),
                                port: edge.from.port.clone(),
                            });
                    }
                }
                (false, false) => {
                    let Some(from_idx) = index_map[edge.from.node.0] else {
                        continue;
                    };
                    let Some(to_idx) = index_map[edge.to.node.0] else {
                        continue;
                    };
                    embedded_internal_edges.push(Edge {
                        from: PortRef {
                            node: NodeRef(from_idx),
                            port: edge.from.port.clone(),
                        },
                        to: PortRef {
                            node: NodeRef(to_idx),
                            port: edge.to.port.clone(),
                        },
                        metadata: edge.metadata.clone(),
                    });
                }
                (true, true) => {}
            }
        }

        embedded_maps.insert(idx, EmbeddedMap { inputs, outputs });

        if trace {
            let mut in_keys: Vec<String> = embedded_maps
                .get(&idx)
                .map(|m| m.inputs.keys().cloned().collect())
                .unwrap_or_default();
            let mut out_keys: Vec<String> = embedded_maps
                .get(&idx)
                .map(|m| m.outputs.keys().cloned().collect())
                .unwrap_or_default();
            in_keys.sort();
            out_keys.sort();
            tracing::debug!(
                target: "daedalus_planner::passes",
                node_idx = idx,
                node_id = %node.id.0,
                group_label = %group_label,
                embedded_inputs = ?in_keys,
                embedded_outputs = ?out_keys,
                "embedded graph expanded"
            );
        }
    }

    let mut new_edges: Vec<Edge> = Vec::new();
    for edge in &input.graph.edges {
        let from_map = embedded_maps.get(&edge.from.node.0);
        let to_map = embedded_maps.get(&edge.to.node.0);

        match (from_map, to_map) {
            (None, None) => {
                let Some(from_idx) = remap[edge.from.node.0] else {
                    continue;
                };
                let Some(to_idx) = remap[edge.to.node.0] else {
                    continue;
                };
                new_edges.push(Edge {
                    from: PortRef {
                        node: NodeRef(from_idx),
                        port: edge.from.port.clone(),
                    },
                    to: PortRef {
                        node: NodeRef(to_idx),
                        port: edge.to.port.clone(),
                    },
                    metadata: edge.metadata.clone(),
                });
            }
            (None, Some(to)) => {
                let Some(from_idx) = remap[edge.from.node.0] else {
                    continue;
                };
                if let Some(targets) = to.inputs.get(&edge.to.port) {
                    for target in targets {
                        new_edges.push(Edge {
                            from: PortRef {
                                node: NodeRef(from_idx),
                                port: edge.from.port.clone(),
                            },
                            to: target.clone(),
                            metadata: edge.metadata.clone(),
                        });
                    }
                } else {
                    // The outer edge targets an embedded-node input port that isn't wired to the host bridge.
                    // This previously dropped the edge silently and later manifested as "missing <port>" at runtime.
                    if let Some(node) = input.graph.nodes.get(edge.to.node.0) {
                        diags.push(
                            Diagnostic::new(
                                DiagnosticCode::PortMissing,
                                format!(
                                    "edge targets embedded node {} input port `{}`, but the embedded graph does not expose/wire that input",
                                    node.id.0, edge.to.port
                                ),
                            )
                            .in_pass("expand_embedded")
                            .at_node(diagnostic_node_id(node))
                            .at_port(edge.to.port.clone())
                            .with_meta(
                                "missing_port",
                                Value::String(std::borrow::Cow::Owned(edge.to.port.clone())),
                            )
                            .with_meta(
                                "missing_port_direction",
                                Value::String(std::borrow::Cow::Borrowed("input")),
                            ),
                        );
                    }
                    if trace {
                        let keys: Vec<&String> = to.inputs.keys().collect();
                        tracing::debug!(
                            target: "daedalus_planner::passes",
                            to_node_idx = edge.to.node.0,
                            to_port = %edge.to.port,
                            available_inputs = ?keys,
                            "embedded edge dropped because target input is not mapped"
                        );
                    }
                }
            }
            (Some(from), None) => {
                let Some(to_idx) = remap[edge.to.node.0] else {
                    continue;
                };
                if let Some(sources) = from.outputs.get(&edge.from.port) {
                    for source in sources {
                        new_edges.push(Edge {
                            from: source.clone(),
                            to: PortRef {
                                node: NodeRef(to_idx),
                                port: edge.to.port.clone(),
                            },
                            metadata: edge.metadata.clone(),
                        });
                    }
                } else {
                    // The outer edge references an embedded-node output port that isn't wired from the host bridge.
                    if let Some(node) = input.graph.nodes.get(edge.from.node.0) {
                        diags.push(
                            Diagnostic::new(
                                DiagnosticCode::PortMissing,
                                format!(
                                    "edge sources embedded node {} output port `{}`, but the embedded graph does not expose/wire that output",
                                    node.id.0, edge.from.port
                                ),
                            )
                            .in_pass("expand_embedded")
                            .at_node(diagnostic_node_id(node))
                            .at_port(edge.from.port.clone())
                            .with_meta(
                                "missing_port",
                                Value::String(std::borrow::Cow::Owned(edge.from.port.clone())),
                            )
                            .with_meta(
                                "missing_port_direction",
                                Value::String(std::borrow::Cow::Borrowed("output")),
                            ),
                        );
                    }
                    if trace {
                        let keys: Vec<&String> = from.outputs.keys().collect();
                        tracing::debug!(
                            target: "daedalus_planner::passes",
                            from_node_idx = edge.from.node.0,
                            from_port = %edge.from.port,
                            available_outputs = ?keys,
                            "embedded edge dropped because source output is not mapped"
                        );
                    }
                }
            }
            (Some(from), Some(to)) => {
                let sources = from.outputs.get(&edge.from.port);
                let targets = to.inputs.get(&edge.to.port);
                if let (Some(sources), Some(targets)) = (sources, targets) {
                    for source in sources {
                        for target in targets {
                            new_edges.push(Edge {
                                from: source.clone(),
                                to: target.clone(),
                                metadata: edge.metadata.clone(),
                            });
                        }
                    }
                } else if trace {
                    let out_keys: Vec<&String> = from.outputs.keys().collect();
                    let in_keys: Vec<&String> = to.inputs.keys().collect();
                    tracing::debug!(
                        target: "daedalus_planner::passes",
                        from_node_idx = edge.from.node.0,
                        from_port = %edge.from.port,
                        available_outputs = ?out_keys,
                        to_node_idx = edge.to.node.0,
                        to_port = %edge.to.port,
                        available_inputs = ?in_keys,
                        "embedded edge dropped because one endpoint is not mapped"
                    );
                }
            }
        }
    }

    // Apply const inputs from embedded nodes when there is no incoming edge.
    for (idx, node) in input.graph.nodes.iter().enumerate() {
        let Some(map) = embedded_maps.get(&idx) else {
            continue;
        };
        let connected = connected_inputs.get(&idx);
        for (port, value) in &node.const_inputs {
            if connected.map(|set| set.contains(port)).unwrap_or(false) {
                continue;
            }
            if let Some(targets) = map.inputs.get(port) {
                for target in targets {
                    if let Some(inner) = new_nodes.get_mut(target.node.0) {
                        inner.const_inputs.retain(|(name, _)| name != &target.port);
                        inner
                            .const_inputs
                            .push((target.port.clone(), value.clone()));
                    }
                }
            }
        }
    }

    new_edges.extend(embedded_internal_edges);

    input.graph.nodes = new_nodes;
    input.graph.edges = new_edges;
}
