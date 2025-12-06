use daedalus_planner::{
    ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PlannerConfig, PlannerInput, PortRef,
    build_plan,
};

#[test]
fn schedule_order_respects_topology() {
    // n0 -> n2, n1 -> n2; expect n0/n1 before n2 in schedule_order
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("n0"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec!["out".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("n1"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec!["out".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("n2"),
        bundle: None,
        label: None,
        inputs: vec!["in".into()],
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
            node: NodeRef(2),
            port: "in".into(),
        },
        metadata: Default::default(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(1),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(2),
            port: "in".into(),
        },
        metadata: Default::default(),
    });

    let registry = daedalus_registry::store::Registry::new();
    let planner_input = PlannerInput {
        graph,
        registry: &registry,
    };
    let output = build_plan(planner_input, PlannerConfig::default());

    let order_str = output
        .plan
        .graph
        .metadata
        .get("schedule_order")
        .cloned()
        .unwrap_or_default();
    let parts: Vec<&str> = order_str.split(',').collect();
    let pos = |id: &str| parts.iter().position(|p| *p == id).unwrap_or(usize::MAX);
    assert!(pos("n0") < pos("n2"));
    assert!(pos("n1") < pos("n2"));
}
