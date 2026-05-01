use daedalus_data::model::Value;

use crate::graph::Graph;

use super::{
    PLAN_APPLIED_LOWERINGS_KEY, PLAN_CONVERTER_METADATA_PREFIX, PLAN_EDGE_EXPLANATIONS_KEY,
    PLAN_OVERLOAD_RESOLUTIONS_KEY, PlannerCatalog, latest_node,
};

pub(super) fn apply_descriptor_defaults(graph: &mut Graph, catalog: &PlannerCatalog) {
    for node in &mut graph.nodes {
        let Some(desc) = latest_node(catalog, &node.id) else {
            continue;
        };
        desc.execution_kind
            .write_default_to_metadata(&mut node.metadata);
        for port in &desc.inputs {
            let Some(raw) = &port.const_value_json else {
                continue;
            };
            let Ok(value) = serde_json::from_str::<Value>(raw) else {
                continue;
            };
            if node.const_inputs.iter().any(|(name, _)| name == &port.name) {
                continue;
            }
            node.const_inputs.push((port.name.clone(), value));
        }
    }
}

pub(super) fn clear_planner_owned_graph_metadata(graph: &mut Graph) {
    graph.metadata.retain(|key, _| {
        !key.starts_with(PLAN_CONVERTER_METADATA_PREFIX)
            && key != PLAN_APPLIED_LOWERINGS_KEY
            && key != PLAN_EDGE_EXPLANATIONS_KEY
            && key != PLAN_OVERLOAD_RESOLUTIONS_KEY
    });
}
