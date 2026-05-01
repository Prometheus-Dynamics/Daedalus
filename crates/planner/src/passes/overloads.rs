use daedalus_core::metadata::NODE_OVERLOADS_KEY;
use daedalus_data::model::{StructFieldValue, TypeExpr, Value};
use daedalus_registry::capability::NodeDecl;
use std::collections::BTreeMap;

use crate::diagnostics::{Diagnostic, DiagnosticCode};
use crate::graph::Graph;
use crate::metadata::DynamicPortMetadata;

use super::{
    NodeOverloadResolution, OverloadPortResolution, PlannerCatalog, PlannerConfig,
    adapt_request_for_input, diagnostic_node_id, input_access_for, input_ty_for, latest_node,
    port_type, resolve_edge_adapter_request,
};

#[derive(Clone, Debug)]
struct ParsedNodeOverload {
    id: String,
    label: Option<String>,
    inputs: BTreeMap<String, TypeExpr>,
}

fn struct_field<'a>(fields: &'a [StructFieldValue], name: &str) -> Option<&'a Value> {
    fields
        .iter()
        .find(|field| field.name == name)
        .map(|field| &field.value)
}

fn value_to_string_map(value: &Value) -> Option<BTreeMap<String, Value>> {
    let Value::Map(entries) = value else {
        return None;
    };
    let mut map = BTreeMap::new();
    for (key, value) in entries {
        let Value::String(key) = key else {
            return None;
        };
        map.insert(key.to_string(), value.clone());
    }
    Some(map)
}

fn value_to_typeexpr(value: &Value) -> Option<TypeExpr> {
    match value {
        Value::String(json) => serde_json::from_str::<TypeExpr>(json).ok(),
        _ => None,
    }
}

fn parse_node_overloads(desc: &NodeDecl) -> Vec<ParsedNodeOverload> {
    let Some(Value::List(entries)) = super::node_metadata_value(desc, NODE_OVERLOADS_KEY) else {
        return Vec::new();
    };

    let mut overloads = Vec::new();
    for entry in entries {
        let Value::Struct(fields) = entry else {
            continue;
        };
        let Some(Value::String(id)) = struct_field(&fields, "id") else {
            continue;
        };
        let label = struct_field(&fields, "label").and_then(|value| match value {
            Value::String(value) => Some(value.to_string()),
            _ => None,
        });
        let Some(inputs_value) = struct_field(&fields, "inputs") else {
            continue;
        };
        let Some(raw_inputs) = value_to_string_map(inputs_value) else {
            continue;
        };
        let mut inputs = BTreeMap::new();
        let mut valid = true;
        for (port, raw_ty) in raw_inputs {
            let Some(ty) = value_to_typeexpr(&raw_ty) else {
                valid = false;
                break;
            };
            inputs.insert(port, ty);
        }
        if !valid {
            continue;
        }
        overloads.push(ParsedNodeOverload {
            id: id.to_string(),
            label,
            inputs,
        });
    }
    overloads.sort_by(|a, b| a.id.cmp(&b.id));
    overloads
}

pub(super) fn resolve_node_overloads(
    graph: &mut Graph,
    catalog: &PlannerCatalog,
    config: &PlannerConfig,
    diags: &mut Vec<Diagnostic>,
) -> Vec<NodeOverloadResolution> {
    let mut resolutions = Vec::new();
    let active_features = config.active_features.clone();
    let allow_gpu = config.enable_gpu;

    for node_idx in 0..graph.nodes.len() {
        let Some(desc) = latest_node(catalog, &graph.nodes[node_idx].id) else {
            continue;
        };
        let overloads = parse_node_overloads(desc);
        if overloads.is_empty() {
            continue;
        }

        let node = graph.nodes[node_idx].clone();
        let incoming_edges = graph
            .edges
            .iter()
            .filter(|edge| edge.to.node.0 == node_idx)
            .cloned()
            .collect::<Vec<_>>();

        let mut best: Option<(u64, String, ParsedNodeOverload, Vec<OverloadPortResolution>)> = None;
        for overload in overloads {
            let mut total_cost = 0u64;
            let mut port_resolutions = Vec::new();
            let mut valid = true;

            for edge in &incoming_edges {
                let Some(from_node) = graph.nodes.get(edge.from.node.0) else {
                    valid = false;
                    break;
                };
                let Some(from_desc) = latest_node(catalog, &from_node.id) else {
                    valid = false;
                    break;
                };
                let Some(from_ty) = port_type(from_node, from_desc, &edge.from.port, false) else {
                    valid = false;
                    break;
                };
                let Some(to_ty) = overload
                    .inputs
                    .get(&edge.to.port)
                    .cloned()
                    .or_else(|| port_type(&node, desc, &edge.to.port, true))
                else {
                    valid = false;
                    break;
                };
                let request =
                    adapt_request_for_input(input_access_for(desc, &edge.to.port), &to_ty);

                let Some(resolved) = resolve_edge_adapter_request(
                    config.transport_capabilities.as_ref(),
                    &from_ty,
                    &to_ty,
                    request,
                    &active_features,
                    allow_gpu,
                ) else {
                    valid = false;
                    break;
                };

                total_cost = total_cost.saturating_add(resolved.total_cost);
                port_resolutions.push(OverloadPortResolution {
                    port: edge.to.port.clone(),
                    from_node: from_node.id.0.clone(),
                    from_port: edge.from.port.clone(),
                    from_type: from_ty,
                    to_type: to_ty,
                    resolution_kind: resolved.resolution_kind,
                    adapter_mode: resolved.adapter_mode,
                    total_cost: resolved.total_cost,
                    converter_steps: resolved.converter_steps,
                });
            }

            if !valid {
                continue;
            }

            port_resolutions.sort_by(|a, b| {
                a.port
                    .cmp(&b.port)
                    .then_with(|| a.from_node.cmp(&b.from_node))
                    .then_with(|| a.from_port.cmp(&b.from_port))
            });
            let sort_key = (total_cost, overload.id.clone());
            match &best {
                Some((best_cost, best_id, _, _))
                    if (&sort_key.0, &sort_key.1) >= (best_cost, best_id) => {}
                _ => best = Some((sort_key.0, sort_key.1.clone(), overload, port_resolutions)),
            }
        }

        let Some((total_cost, _, overload, port_resolutions)) = best else {
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::ConverterMissing,
                    format!(
                        "no overload on node {} could satisfy the connected input types",
                        node.id.0
                    ),
                )
                .in_pass("resolve_overloads")
                .at_node(diagnostic_node_id(&node)),
            );
            continue;
        };

        let mut dynamic_metadata =
            DynamicPortMetadata::from_node_metadata(&graph.nodes[node_idx].metadata);
        for (port, ty) in &overload.inputs {
            if input_ty_for(desc, port).is_none() {
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::PortExtra,
                        format!(
                            "overload {} on node {} references unknown input port `{}`",
                            overload.id, node.id.0, port
                        ),
                    )
                    .in_pass("resolve_overloads")
                    .at_node(diagnostic_node_id(&node))
                    .at_port(port.clone()),
                );
                continue;
            }
            dynamic_metadata.set_resolved_type(true, port, ty.clone());
        }
        for (port, ty) in &overload.inputs {
            dynamic_metadata.set_label(true, port, format!("{ty:?}"));
        }
        dynamic_metadata.write_to_node_metadata(&mut graph.nodes[node_idx].metadata);

        resolutions.push(NodeOverloadResolution {
            node: node.id.0.clone(),
            overload_id: overload.id,
            overload_label: overload.label,
            total_cost,
            ports: port_resolutions,
        });
    }

    resolutions.sort_by(|a, b| {
        a.node
            .cmp(&b.node)
            .then_with(|| a.overload_id.cmp(&b.overload_id))
    });
    resolutions
}
