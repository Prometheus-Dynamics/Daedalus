use daedalus_data::model::{TypeExpr, ValueType};
use daedalus_planner::{
    ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PlannerConfig, PlannerInput, PortRef,
    build_plan,
};
use daedalus_registry::store::{NodeDescriptorBuilder, Registry};

#[test]
fn fanin_inputs_accept_indexed_ports() {
    let mut registry = Registry::new();
    let list_int = TypeExpr::List(Box::new(TypeExpr::Scalar(ValueType::Int)));

    registry
        .register_node(
            NodeDescriptorBuilder::new("src")
                .output("out", list_int.clone())
                .build()
                .unwrap(),
        )
        .unwrap();

    registry
        .register_node(
            NodeDescriptorBuilder::new("merge")
                .fanin_input("ins", 0, list_int.clone())
                .output("out", list_int.clone())
                .build()
                .unwrap(),
        )
        .unwrap();

    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("src"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("merge"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "ins2".into(),
        },
        metadata: Default::default(),
    });

    let out = build_plan(
        PlannerInput {
            graph,
            registry: &registry,
        },
        PlannerConfig::default(),
    );

    assert!(
        out.plan.diagnostics.is_empty(),
        "expected no diagnostics, got: {:#?}",
        out.plan.diagnostics
    );
}
