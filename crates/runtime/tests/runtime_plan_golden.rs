use daedalus_planner::{
    ComputeAffinity, Edge, ExecutionPlan, Graph, NodeInstance, NodeRef, PortRef,
};
use daedalus_runtime::{EdgePolicyKind, SchedulerConfig, build_runtime, debug};
use serde_json::Value;

#[test]
fn runtime_plan_cpu_golden() {
    // Planner graph: a(out) -> b(in), both CPU
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
            port: "in".into(),
        },
        metadata: Default::default(),
    });

    let exec = ExecutionPlan::new(graph, vec![]);
    let runtime = build_runtime(&exec, &SchedulerConfig::default());
    // sanity
    assert!(matches!(runtime.edges[0].4, EdgePolicyKind::Fifo));
    assert_eq!(runtime.schedule_order, vec![NodeRef(0), NodeRef(1)]);

    let actual: Value = serde_json::from_str(&debug::to_pretty_json(&runtime)).unwrap();
    let expected: Value =
        serde_json::from_str(include_str!("goldens/runtime_plan_cpu.json")).unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn runtime_plan_gpu_segment_golden() {
    // Planner graph: cpu -> gpu1 -> gpu2 -> cpu
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("cpu0"),
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
        id: daedalus_registry::ids::NodeId::new("gpu1"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::GpuRequired,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("gpu2"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::GpuPreferred,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("cpu1"),
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
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(2),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(3),
            port: "in".into(),
        },
        metadata: Default::default(),
    });

    let exec = ExecutionPlan::new(graph, vec![]);
    let runtime = build_runtime(&exec, &SchedulerConfig::default());
    // segments should group contiguous GPU nodes (gpu1 + gpu2)
    assert_eq!(runtime.segments.len(), 3);
    assert_eq!(runtime.segments[1].nodes, vec![NodeRef(1), NodeRef(2)]);
    assert_eq!(
        runtime.schedule_order,
        vec![NodeRef(1), NodeRef(2), NodeRef(0), NodeRef(3)]
    );

    let actual: Value = serde_json::from_str(&debug::to_pretty_json(&runtime)).unwrap();
    let expected: Value =
        serde_json::from_str(include_str!("goldens/runtime_plan_gpu_segment.json")).unwrap();
    assert_eq!(actual, expected);
}
