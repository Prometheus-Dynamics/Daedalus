use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;

use daedalus_registry::capability::{NODE_FIRE_META_KEY, NodeDecl, NodeFire};

use crate::diagnostics::{Diagnostic, DiagnosticCode};
use crate::metadata::descriptor_metadata_value;

use super::{
    PlannerCatalog, PlannerConfig, PlannerInput, adapt_request_for_input, diagnostic_node_id,
    input_access_for, is_host_bridge, latest_node, port_type, resolve_edge_adapter_request,
    target_residency_for_node,
};

pub(super) fn lint(
    input: &PlannerInput,
    catalog: &PlannerCatalog,
    config: &PlannerConfig,
    diags: &mut Vec<Diagnostic>,
) {
    let n = input.graph.nodes.len();
    let mut incoming: Vec<usize> = vec![0; n];
    let mut outgoing: Vec<usize> = vec![0; n];
    for e in &input.graph.edges {
        if e.from.node.0 < n {
            outgoing[e.from.node.0] += 1;
        }
        if e.to.node.0 < n {
            incoming[e.to.node.0] += 1;
        }
    }

    // Enforce exclusivity for ports that declare `Move`/`Modify` access.
    // This is the planner-level guardrail that makes in-place / COW transforms predictable:
    // if a producer output is fanned out, a downstream node cannot claim exclusive access.
    let mut fanout: BTreeMap<(usize, String), usize> = BTreeMap::new();
    for e in &input.graph.edges {
        *fanout
            .entry((e.from.node.0, e.from.port.clone()))
            .or_insert(0) += 1;
    }
    for e in &input.graph.edges {
        let Some(to_node) = input.graph.nodes.get(e.to.node.0) else {
            continue;
        };
        let Some(desc) = latest_node(catalog, &to_node.id) else {
            continue;
        };
        let access = input_access_for(desc, &e.to.port);
        if matches!(
            access,
            daedalus_transport::AccessMode::Move | daedalus_transport::AccessMode::Modify
        ) {
            let count = fanout
                .get(&(e.from.node.0, e.from.port.clone()))
                .copied()
                .unwrap_or(0);
            if count > 1 {
                let Some(from_node) = input.graph.nodes.get(e.from.node.0) else {
                    continue;
                };
                let from_ty = latest_node(catalog, &from_node.id)
                    .and_then(|desc| port_type(from_node, desc, &e.from.port, false));
                let to_ty = port_type(to_node, desc, &e.to.port, true);
                if let (Some(out_ty), Some(in_ty)) = (from_ty, to_ty) {
                    let mut request = adapt_request_for_input(access, &in_ty);
                    request.exclusive = true;
                    request.residency = target_residency_for_node(to_node, config);
                    let features = config.active_features.clone();
                    let resolved = resolve_edge_adapter_request(
                        config.transport_capabilities.as_ref(),
                        &out_ty,
                        &in_ty,
                        request,
                        &features,
                        config.enable_gpu,
                    );
                    if resolved
                        .as_ref()
                        .is_some_and(|resolved| resolved.uses_adapter())
                    {
                        continue;
                    }
                }
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::AccessViolation,
                        format!(
                            "input {}:{} requires exclusive access ({access:?}), but source {}:{} is fanned out to {} consumers",
                            diagnostic_node_id(to_node),
                            e.to.port,
                            diagnostic_node_id(from_node),
                            e.from.port,
                            count
                        ),
                    )
                    .in_pass("lint")
                    .at_node(diagnostic_node_id(to_node))
                    .at_port(e.to.port.clone()),
                );
            }
        }
    }

    for (idx, node) in input.graph.nodes.iter().enumerate() {
        // Optional inputs are meant to be left unconnected.
        let desc = latest_node(catalog, &node.id);
        let required: Vec<&str> = node
            .inputs
            .iter()
            .map(String::as_str)
            .filter(|name| {
                !desc.is_some_and(|desc| {
                    desc.inputs
                        .iter()
                        .any(|port| port.optional && port.name.eq_ignore_ascii_case(name))
                })
            })
            .collect();
        if incoming[idx] == 0 && !required.is_empty() {
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::LintWarning,
                    format!(
                        "node {} has unconnected inputs: {}",
                        node.id.0,
                        required.join(",")
                    ),
                )
                .in_pass("lint")
                .at_node(diagnostic_node_id(node)),
            );
        }
        if outgoing[idx] == 0 && !node.outputs.is_empty() {
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::LintWarning,
                    format!(
                        "node {} has unused outputs: {}",
                        node.id.0,
                        node.outputs.join(",")
                    ),
                )
                .in_pass("lint")
                .at_node(diagnostic_node_id(node)),
            );
        }
    }

    lint_fire_modes(input, catalog, diags);
}

/// `fire = "all"` nodes: warn about an unknown fire mode, and about required inputs whose
/// producer may not produce (best effort): a conditional output (`outputs.<port>.conditional`,
/// an `Option` return), or a node that can itself be skipped because one of its required inputs
/// comes from such a producer. The join then holds its other inputs until that producer emits.
fn lint_fire_modes(input: &PlannerInput, catalog: &PlannerCatalog, diags: &mut Vec<Diagnostic>) {
    let graph = &input.graph;
    let descs: Vec<Option<&NodeDecl>> = graph
        .nodes
        .iter()
        .map(|node| latest_node(catalog, &node.id))
        .collect();
    let is_required = |node: usize, port: &str| {
        descs[node].is_some_and(|desc| {
            desc.inputs
                .iter()
                .any(|p| !p.optional && p.name.eq_ignore_ascii_case(port))
        })
    };
    let conditional = |node: usize, port: &str| {
        descs[node].is_some_and(|desc| {
            let key = alloc::format!("outputs.{port}.conditional");
            matches!(
                descriptor_metadata_value(desc, &key),
                Some(daedalus_data::model::Value::Bool(true))
            )
        })
    };
    // Fixed point: a node may skip when a required input comes from an output that may be empty.
    let mut may_skip = vec![false; graph.nodes.len()];
    let may_be_empty = |may_skip: &[bool], node: usize, port: &str| {
        !is_host_bridge(&graph.nodes[node]) && (may_skip[node] || conditional(node, port))
    };
    let mut changed = true;
    while changed {
        changed = false;
        for edge in &graph.edges {
            let (from, to) = (edge.from.node.0, edge.to.node.0);
            if from < may_skip.len()
                && to < may_skip.len()
                && !may_skip[to]
                && is_required(to, &edge.to.port)
                && may_be_empty(&may_skip, from, &edge.from.port)
            {
                may_skip[to] = true;
                changed = true;
            }
        }
    }

    for (idx, node) in graph.nodes.iter().enumerate() {
        let Some(raw) = node.metadata.get(NODE_FIRE_META_KEY) else {
            continue;
        };
        let fire = raw.as_str().and_then(NodeFire::parse);
        if fire.is_none() {
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::LintWarning,
                    alloc::format!(
                        "node {} has unknown {NODE_FIRE_META_KEY} {raw:?}; expected \"any\" or \"all\" (using \"any\")",
                        node.id.0
                    ),
                )
                .in_pass("lint")
                .at_node(diagnostic_node_id(node)),
            );
        }
        if fire != Some(NodeFire::All) {
            continue;
        }
        for edge in graph.edges.iter().filter(|edge| edge.to.node.0 == idx) {
            let from = edge.from.node.0;
            if from >= graph.nodes.len()
                || !is_required(idx, &edge.to.port)
                || !may_be_empty(&may_skip, from, &edge.from.port)
            {
                continue;
            }
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::LintWarning,
                    alloc::format!(
                        "node {} fires on all inputs, but `{}` comes from {}:{}, which may not produce; the node holds its other inputs until it does",
                        diagnostic_node_id(node),
                        edge.to.port,
                        diagnostic_node_id(&graph.nodes[from]),
                        edge.from.port,
                    ),
                )
                .in_pass("lint")
                .at_node(diagnostic_node_id(node))
                .at_port(edge.to.port.clone()),
            );
        }
    }
}
