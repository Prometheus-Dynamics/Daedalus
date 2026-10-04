use alloc::vec::Vec;
use daedalus_data::model::Value;
use daedalus_registry::capability::{NODE_FIRE_META_KEY, NODE_REQUIRED_INPUTS_META_KEY};

use crate::graph::Graph;
use crate::metadata::descriptor_metadata_value;

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
        // The declaration's fire mode is the default; graph node metadata overrides it.
        if !node.metadata.contains_key(NODE_FIRE_META_KEY)
            && let Some(fire) = descriptor_metadata_value(desc, NODE_FIRE_META_KEY)
        {
            node.metadata.insert(NODE_FIRE_META_KEY.into(), fire);
        }
        let required: Vec<Value> = desc
            .inputs
            .iter()
            .filter(|port| !port.optional)
            .map(|port| Value::String(port.name.clone().into()))
            .collect();
        if required.is_empty() {
            node.metadata.remove(NODE_REQUIRED_INPUTS_META_KEY);
        } else {
            node.metadata
                .insert(NODE_REQUIRED_INPUTS_META_KEY.into(), Value::List(required));
        }
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
