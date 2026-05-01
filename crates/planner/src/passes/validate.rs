use daedalus_data::model::Value;
use daedalus_registry::capability::NodeDecl;
use std::collections::HashSet;

use crate::diagnostics::{Diagnostic, DiagnosticCode};
use crate::graph::Graph;

use super::{PlannerCatalog, diagnostic_node_id, input_ty_for, latest_node};

pub(super) fn validate_port_declarations(
    graph: &Graph,
    catalog: &PlannerCatalog,
    diags: &mut Vec<Diagnostic>,
    strict_port_declarations: bool,
) {
    fn is_dynamic(desc: &NodeDecl, is_input: bool) -> bool {
        crate::metadata::descriptor_dynamic_port_type(desc, is_input).is_some()
    }

    fn fanin_hints(desc: &NodeDecl) -> Vec<String> {
        desc.fanin_inputs
            .iter()
            .map(|spec| format!("{}{}+", spec.prefix, spec.start))
            .collect()
    }

    fn available_inputs(desc: &NodeDecl) -> Vec<Value> {
        let mut out: Vec<Value> = desc
            .inputs
            .iter()
            .map(|p| Value::String(std::borrow::Cow::Owned(p.name.clone())))
            .collect();
        for hint in fanin_hints(desc) {
            out.push(Value::String(std::borrow::Cow::Owned(hint)));
        }
        out
    }

    fn available_outputs(desc: &NodeDecl) -> Vec<Value> {
        desc.outputs
            .iter()
            .map(|p| Value::String(std::borrow::Cow::Owned(p.name.clone())))
            .collect()
    }

    for node in &graph.nodes {
        let Some(desc) = latest_node(catalog, &node.id) else {
            continue;
        };

        let node_label = diagnostic_node_id(node);

        // Inputs: stale graph can carry extra/missing port entries even when there are no edges.
        let dynamic_inputs = is_dynamic(desc, true);
        let mut seen_inputs: HashSet<String> = HashSet::new();
        for port in &node.inputs {
            let port_lc = port.trim().to_ascii_lowercase();
            if port_lc.is_empty() {
                continue;
            }
            if !seen_inputs.insert(port_lc.clone()) {
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::PortDuplicate,
                        format!(
                            "graph declares duplicate input port `{}` on node {}",
                            port, node.id.0
                        ),
                    )
                    .in_pass("validate_ports")
                    .at_node(node_label.clone())
                    .at_port(port.clone())
                    .with_meta(
                        "extra_port",
                        Value::String(std::borrow::Cow::Owned(port.clone())),
                    )
                    .with_meta(
                        "extra_port_direction",
                        Value::String(std::borrow::Cow::Borrowed("input")),
                    )
                    .with_meta("available_ports", Value::List(available_inputs(desc))),
                );
                continue;
            }

            if dynamic_inputs {
                continue;
            }
            if input_ty_for(desc, port).is_some() {
                continue;
            }

            diags.push(
                Diagnostic::new(
                    DiagnosticCode::PortExtra,
                    format!(
                        "graph declares input port `{}` on node {}, but the registry descriptor does not provide that port",
                        port, node.id.0
                    ),
                )
                .in_pass("validate_ports")
                .at_node(node_label.clone())
                .at_port(port.clone())
                .with_meta(
                    "extra_port",
                    Value::String(std::borrow::Cow::Owned(port.clone())),
                )
                .with_meta(
                    "extra_port_direction",
                    Value::String(std::borrow::Cow::Borrowed("input")),
                )
                .with_meta("available_ports", Value::List(available_inputs(desc))),
            );
        }

        // Validate missing ports when the graph declares port lists (normal UI-persisted graphs),
        // or when strict mode is enabled.
        if !dynamic_inputs && (strict_port_declarations || !node.inputs.is_empty()) {
            let node_inputs_lc: HashSet<String> = node
                .inputs
                .iter()
                .map(|p| p.trim().to_ascii_lowercase())
                .filter(|p| !p.is_empty())
                .collect();
            for port in &desc.inputs {
                let port_lc = port.name.trim().to_ascii_lowercase();
                if port_lc.is_empty() {
                    continue;
                }
                if node_inputs_lc.contains(&port_lc) {
                    continue;
                }
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::PortMissing,
                        format!(
                            "graph is missing input port `{}` on node {} (graph is stale; regenerate ports from registry)",
                            port.name, node.id.0
                        ),
                    )
                    .in_pass("validate_ports")
                    .at_node(node_label.clone())
                    .at_port(port.name.clone())
                    .with_meta(
                        "missing_port",
                        Value::String(std::borrow::Cow::Owned(port.name.clone())),
                    )
                    .with_meta(
                        "missing_port_direction",
                        Value::String(std::borrow::Cow::Borrowed("input")),
                    )
                    .with_meta("available_ports", Value::List(available_inputs(desc))),
                );
            }
        }

        // Outputs: same story.
        let dynamic_outputs = is_dynamic(desc, false);
        let mut seen_outputs: HashSet<String> = HashSet::new();
        for port in &node.outputs {
            let port_lc = port.trim().to_ascii_lowercase();
            if port_lc.is_empty() {
                continue;
            }
            if !seen_outputs.insert(port_lc.clone()) {
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::PortDuplicate,
                        format!(
                            "graph declares duplicate output port `{}` on node {}",
                            port, node.id.0
                        ),
                    )
                    .in_pass("validate_ports")
                    .at_node(node_label.clone())
                    .at_port(port.clone())
                    .with_meta(
                        "extra_port",
                        Value::String(std::borrow::Cow::Owned(port.clone())),
                    )
                    .with_meta(
                        "extra_port_direction",
                        Value::String(std::borrow::Cow::Borrowed("output")),
                    )
                    .with_meta("available_ports", Value::List(available_outputs(desc))),
                );
                continue;
            }

            if dynamic_outputs {
                continue;
            }
            if desc.outputs.iter().any(|p| p.name == *port) {
                continue;
            }

            diags.push(
                Diagnostic::new(
                    DiagnosticCode::PortExtra,
                    format!(
                        "graph declares output port `{}` on node {}, but the registry descriptor does not provide that port",
                        port, node.id.0
                    ),
                )
                .in_pass("validate_ports")
                .at_node(node_label.clone())
                .at_port(port.clone())
                .with_meta(
                    "extra_port",
                    Value::String(std::borrow::Cow::Owned(port.clone())),
                )
                .with_meta(
                    "extra_port_direction",
                    Value::String(std::borrow::Cow::Borrowed("output")),
                )
                .with_meta("available_ports", Value::List(available_outputs(desc))),
            );
        }

        if !dynamic_outputs && (strict_port_declarations || !node.outputs.is_empty()) {
            let node_outputs_lc: HashSet<String> = node
                .outputs
                .iter()
                .map(|p| p.trim().to_ascii_lowercase())
                .filter(|p| !p.is_empty())
                .collect();
            for port in &desc.outputs {
                let port_lc = port.name.trim().to_ascii_lowercase();
                if port_lc.is_empty() {
                    continue;
                }
                if node_outputs_lc.contains(&port_lc) {
                    continue;
                }
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::PortMissing,
                        format!(
                            "graph is missing output port `{}` on node {} (graph is stale; regenerate ports from registry)",
                            port.name, node.id.0
                        ),
                    )
                    .in_pass("validate_ports")
                    .at_node(node_label.clone())
                    .at_port(port.name.clone())
                    .with_meta(
                        "missing_port",
                        Value::String(std::borrow::Cow::Owned(port.name.clone())),
                    )
                    .with_meta(
                        "missing_port_direction",
                        Value::String(std::borrow::Cow::Borrowed("output")),
                    )
                    .with_meta("available_ports", Value::List(available_outputs(desc))),
                );
            }
        }
    }

    // Validate edge references against registry ports, even when the graph doesn't declare port
    // lists (or when the lists are stale). This catches the common "node updated, edge still
    // points at removed port" failure mode.
    for edge in &graph.edges {
        let Some(from_node) = graph.nodes.get(edge.from.node.0) else {
            continue;
        };
        let Some(to_node) = graph.nodes.get(edge.to.node.0) else {
            continue;
        };
        let Some(from_desc) = latest_node(catalog, &from_node.id) else {
            continue;
        };
        let Some(to_desc) = latest_node(catalog, &to_node.id) else {
            continue;
        };

        let from_dynamic_outputs = is_dynamic(from_desc, false);
        if !from_dynamic_outputs {
            let port = edge.from.port.trim();
            if !port.is_empty()
                && !from_desc
                    .outputs
                    .iter()
                    .any(|p| p.name.eq_ignore_ascii_case(port))
            {
                let available = Value::List(available_outputs(from_desc));
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::PortMissing,
                        format!(
                            "edge references output port `{}` on node {}, but the registry descriptor does not provide that port",
                            edge.from.port, from_node.id.0
                        ),
                    )
                    .in_pass("validate_ports")
                    .at_node(diagnostic_node_id(from_node))
                    .at_port(edge.from.port.clone())
                    .with_meta(
                        "missing_port",
                        Value::String(std::borrow::Cow::Owned(edge.from.port.clone())),
                    )
                    .with_meta(
                        "missing_port_direction",
                        Value::String(std::borrow::Cow::Borrowed("output")),
                    )
                    .with_meta("available_ports", available),
                );
            }
        }

        let to_dynamic_inputs = is_dynamic(to_desc, true);
        if !to_dynamic_inputs {
            let port = edge.to.port.trim();
            if !port.is_empty() && input_ty_for(to_desc, port).is_none() {
                let available = Value::List(available_inputs(to_desc));
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::PortMissing,
                        format!(
                            "edge references input port `{}` on node {}, but the registry descriptor does not provide that port",
                            edge.to.port, to_node.id.0
                        ),
                    )
                    .in_pass("validate_ports")
                    .at_node(diagnostic_node_id(to_node))
                    .at_port(edge.to.port.clone())
                    .with_meta(
                        "missing_port",
                        Value::String(std::borrow::Cow::Owned(edge.to.port.clone())),
                    )
                    .with_meta(
                        "missing_port_direction",
                        Value::String(std::borrow::Cow::Borrowed("input")),
                    )
                    .with_meta("available_ports", available),
                );
            }
        }
    }
}
