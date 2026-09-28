use super::*;
use crate::graph::Graph;
use std::collections::BTreeMap;

#[test]
fn build_plan_strips_planner_owned_dynamic_metadata() {
    let mut metadata = BTreeMap::new();
    for key in [
        DYNAMIC_INPUT_TYPES_KEY,
        DYNAMIC_OUTPUT_TYPES_KEY,
        DYNAMIC_INPUT_LABELS_KEY,
        DYNAMIC_OUTPUT_LABELS_KEY,
        DYNAMIC_INPUTS_KEY,
        DYNAMIC_OUTPUTS_KEY,
    ] {
        metadata.insert(key.to_string(), Value::String("client".into()));
    }

    let graph = Graph {
        nodes: vec![NodeInstance {
            id: NodeId::new("demo.node"),
            bundle: None,
            label: None,
            inputs: Vec::new(),
            outputs: Vec::new(),
            compute: crate::graph::ComputeAffinity::CpuOnly,
            const_inputs: Vec::new(),
            sync_groups: Vec::new(),
            metadata,
        }],
        ..Graph::default()
    };

    let out = build_plan(PlannerInput { graph }, PlannerConfig::default());
    let node_metadata = &out.plan.graph.nodes[0].metadata;
    for key in [
        DYNAMIC_INPUT_TYPES_KEY,
        DYNAMIC_OUTPUT_TYPES_KEY,
        DYNAMIC_INPUT_LABELS_KEY,
        DYNAMIC_OUTPUT_LABELS_KEY,
        DYNAMIC_INPUTS_KEY,
        DYNAMIC_OUTPUTS_KEY,
    ] {
        assert!(!node_metadata.contains_key(key), "{key} was not stripped");
    }
}
