use std::collections::HashMap;

use crate::diagnostics::{Diagnostic, DiagnosticCode};

use super::{
    PlannerCatalog, PlannerConfig, PlannerInput, adapt_request_for_input, diagnostic_node_id,
    input_access_for, latest_node, port_type, resolve_edge_adapter_request,
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

    // Enforce exclusivity for ports that declare `Owned`/`MutBorrowed` access.
    // This is the planner-level guardrail that makes in-place / COW transforms predictable:
    // if a producer output is fanned out, a downstream node cannot claim exclusive access.
    let mut fanout: HashMap<(usize, String), usize> = HashMap::new();
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
        if incoming[idx] == 0 && !node.inputs.is_empty() {
            diags.push(
                Diagnostic::new(
                    DiagnosticCode::LintWarning,
                    format!(
                        "node {} has unconnected inputs: {}",
                        node.id.0,
                        node.inputs.join(",")
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
}
