use daedalus_planner::{ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PortRef};
use daedalus_runtime::{EdgePolicyKind, SchedulerConfig, build_runtime, debug};

#[test]
fn runtime_plan_inherits_nodes_and_edges() {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("a"),
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
        id: daedalus_registry::ids::NodeId::new("b"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::GpuRequired,
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
            port: "in".into(),
        },
        metadata: Default::default(),
    });

    let exec = daedalus_planner::ExecutionPlan::new(graph, vec![]);
    let runtime = build_runtime(&exec, &SchedulerConfig::default());

    assert_eq!(runtime.nodes.len(), 2);
    assert_eq!(runtime.edges.len(), 1);
    assert!(matches!(runtime.edges[0].4, EdgePolicyKind::Fifo));
    // Segments group GPU nodes consecutively.
    assert_eq!(runtime.segments.len(), 2);
    assert_eq!(runtime.segments[1].compute, ComputeAffinity::GpuRequired);

    // Ensure serde round-trip works.
    let json = debug::to_pretty_json(&runtime);
    let round = debug::from_json(&json).expect("round-trip");
    assert_eq!(round.nodes.len(), runtime.nodes.len());
}
