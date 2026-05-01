use daedalus_data::model::Value;
use daedalus_planner::{ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PortRef};
use daedalus_runtime::host_bridge::HOST_BRIDGE_META_KEY;
use daedalus_runtime::{RuntimeEdgePolicy, RuntimePlan, SchedulerConfig, build_runtime, debug};
use std::collections::BTreeMap;

fn node(id: &str, compute: ComputeAffinity) -> NodeInstance {
    NodeInstance {
        id: daedalus_registry::ids::NodeId::new(id),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    }
}

fn edge(from: usize, to: usize) -> Edge {
    Edge {
        from: PortRef {
            node: NodeRef(from),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(to),
            port: "in".into(),
        },
        metadata: Default::default(),
    }
}

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
    assert_eq!(runtime.edges[0].policy(), &RuntimeEdgePolicy::default());
    // Segments group GPU nodes consecutively.
    assert_eq!(runtime.segments.len(), 2);
    assert_eq!(runtime.segments[1].compute, ComputeAffinity::GpuRequired);

    // Ensure serde round-trip works.
    let json = debug::to_pretty_json(&runtime);
    let round = debug::from_json(&json).expect("round-trip");
    assert_eq!(round.nodes.len(), runtime.nodes.len());
}

#[test]
fn runtime_plan_splits_independent_gpu_fanout_segments() {
    let mut graph = Graph::default();
    graph.nodes.push(node("cpu-root", ComputeAffinity::CpuOnly));
    graph
        .nodes
        .push(node("gpu-a", ComputeAffinity::GpuRequired));
    graph
        .nodes
        .push(node("gpu-b", ComputeAffinity::GpuPreferred));
    graph.edges.push(edge(0, 1));
    graph.edges.push(edge(0, 2));

    let exec = daedalus_planner::ExecutionPlan::new(graph, vec![]);
    let runtime = RuntimePlan::from_execution(&exec);

    assert_eq!(
        runtime.schedule_order,
        vec![NodeRef(0), NodeRef(1), NodeRef(2)]
    );
    assert_eq!(runtime.segments.len(), 3);
    assert_eq!(runtime.segments[0].nodes, vec![NodeRef(0)]);
    assert_eq!(runtime.segments[1].nodes, vec![NodeRef(1)]);
    assert_eq!(runtime.segments[2].nodes, vec![NodeRef(2)]);
}

#[test]
fn runtime_plan_groups_dependent_gpu_chain_segment() {
    let mut graph = Graph::default();
    graph.nodes.push(node("cpu-root", ComputeAffinity::CpuOnly));
    graph
        .nodes
        .push(node("gpu-a", ComputeAffinity::GpuRequired));
    graph
        .nodes
        .push(node("gpu-b", ComputeAffinity::GpuPreferred));
    graph.nodes.push(node("cpu-tail", ComputeAffinity::CpuOnly));
    graph.edges.push(edge(0, 1));
    graph.edges.push(edge(1, 2));
    graph.edges.push(edge(2, 3));

    let exec = daedalus_planner::ExecutionPlan::new(graph, vec![]);
    let runtime = RuntimePlan::from_execution(&exec);

    assert_eq!(runtime.segments.len(), 3);
    assert_eq!(runtime.segments[0].nodes, vec![NodeRef(0)]);
    assert_eq!(runtime.segments[1].nodes, vec![NodeRef(1), NodeRef(2)]);
    assert_eq!(runtime.segments[1].compute, ComputeAffinity::GpuRequired);
    assert_eq!(runtime.segments[2].nodes, vec![NodeRef(3)]);
}

#[test]
fn runtime_plan_uses_host_bridge_metadata_not_node_id_suffix() {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("consumer"),
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
        id: daedalus_registry::ids::NodeId::new("custom.host.gateway"),
        bundle: None,
        label: Some("renamed-host".to_string()),
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: BTreeMap::from([(HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true))]),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(1),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(0),
            port: "in".into(),
        },
        metadata: Default::default(),
    });

    let exec = daedalus_planner::ExecutionPlan::new(graph, vec![]);
    let runtime = RuntimePlan::from_execution(&exec);

    assert_eq!(runtime.schedule_order, vec![NodeRef(0), NodeRef(1)]);
}
