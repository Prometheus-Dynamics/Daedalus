use daedalus_data::model::Value;
use daedalus_planner::{
    ComputeAffinity, Graph, NodeInstance, PlannerConfig, PlannerInput, build_plan,
};
use daedalus_registry::capability::{
    CapabilityRegistry, NODE_EXECUTION_KIND_META_KEY, NodeDecl, NodeExecutionKind,
};

#[test]
fn planner_applies_node_execution_kind_from_declaration() {
    let mut capabilities = CapabilityRegistry::new();
    capabilities
        .register_node(NodeDecl::new("demo.external").execution_kind(NodeExecutionKind::External))
        .unwrap();

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("demo.external"),
        bundle: None,
        label: None,
        inputs: Vec::new(),
        outputs: Vec::new(),
        compute: ComputeAffinity::CpuOnly,
        const_inputs: Vec::new(),
        sync_groups: Vec::new(),
        metadata: Default::default(),
    });

    let output = build_plan(
        PlannerInput { graph },
        PlannerConfig {
            transport_capabilities: Some(capabilities),
            ..PlannerConfig::default()
        },
    );

    assert_eq!(
        output.plan.graph.nodes[0]
            .metadata
            .get(NODE_EXECUTION_KIND_META_KEY),
        Some(&Value::String("external".into()))
    );
}
