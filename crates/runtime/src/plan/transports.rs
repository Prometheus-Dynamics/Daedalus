use daedalus_core::metadata::PLAN_EDGE_EXPLANATIONS_KEY;
use daedalus_planner::ExecutionPlan;

use super::transport_parse::{
    access_field, adapter_path_field, adapter_steps_field, bool_field, residency_field,
    string_field, struct_fields, typeexpr_field, typeexpr_transport_key, u64_field,
};
use super::{RuntimeEdge, RuntimeEdgeTransport};

pub(super) fn runtime_edge_transports(
    plan: &ExecutionPlan,
    edges: &[RuntimeEdge],
) -> Vec<Option<RuntimeEdgeTransport>> {
    let mut transports = vec![None; edges.len()];
    let Some(daedalus_data::model::Value::List(entries)) =
        plan.graph.metadata.get(PLAN_EDGE_EXPLANATIONS_KEY)
    else {
        return Vec::new();
    };

    for entry in entries {
        let Some(fields) = struct_fields(entry) else {
            continue;
        };
        if string_field(fields, "resolution_kind") != Some("conversion") {
            continue;
        }
        let steps = adapter_steps_field(fields, "converter_steps");
        if steps.is_empty() {
            continue;
        }
        let Some(from_node) = string_field(fields, "from_node") else {
            continue;
        };
        let Some(from_port) = string_field(fields, "from_port") else {
            continue;
        };
        let Some(to_node) = string_field(fields, "to_node") else {
            continue;
        };
        let Some(to_port) = string_field(fields, "to_port") else {
            continue;
        };
        let Some(from_type) = typeexpr_field(fields, "from_type") else {
            continue;
        };
        let Some(to_type) = typeexpr_field(fields, "to_type") else {
            continue;
        };

        for (idx, edge) in edges.iter().enumerate() {
            let Some(edge_from_node) = plan.graph.nodes.get(edge.from().0) else {
                continue;
            };
            let Some(edge_to_node) = plan.graph.nodes.get(edge.to().0) else {
                continue;
            };
            if edge_from_node.id.0 == from_node
                && edge.source_port() == from_port
                && edge_to_node.id.0 == to_node
                && edge.target_port() == to_port
            {
                let source_transport = Some(typeexpr_transport_key(&from_type));
                let target_transport = string_field(fields, "transport_target")
                    .map(daedalus_transport::TypeKey::new)
                    .or_else(|| Some(typeexpr_transport_key(&to_type)));
                transports[idx] = Some(RuntimeEdgeTransport {
                    from_type: from_type.clone(),
                    to_type: to_type.clone(),
                    source_transport,
                    target_transport,
                    target_access: access_field(fields, "target_access")
                        .unwrap_or(daedalus_transport::AccessMode::Read),
                    target_exclusive: bool_field(fields, "target_exclusive").unwrap_or(false),
                    target_residency: residency_field(fields, "target_residency"),
                    transport_target: string_field(fields, "transport_target")
                        .map(daedalus_transport::TypeKey::new),
                    adapter_steps: steps.clone(),
                    adapter_path: adapter_path_field(fields, "adapter_path"),
                    expected_adapter_cost: u64_field(fields, "total_cost"),
                });
                break;
            }
        }
    }

    if transports.iter().all(Option::is_none) {
        Vec::new()
    } else {
        transports
    }
}
