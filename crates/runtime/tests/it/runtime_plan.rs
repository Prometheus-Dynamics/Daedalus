use daedalus_data::model::Value;
use daedalus_planner::{ComputeAffinity, Edge, Graph, NodeInstance, NodeRef};
use daedalus_runtime::host_bridge::HOST_BRIDGE_META_KEY;
use daedalus_runtime::{RuntimeEdgePolicy, RuntimePlan, SchedulerConfig, build_runtime, debug};

fn node(id: &str, compute: ComputeAffinity) -> NodeInstance {
    NodeInstance::new(id).with_compute(compute)
}

fn edge(from: usize, to: usize) -> Edge {
    Edge::new(from, "out", to, "in")
}

#[test]
fn runtime_plan_inherits_nodes_and_edges() {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance::new("a"));
    graph
        .nodes
        .push(NodeInstance::new("b").with_compute(ComputeAffinity::GpuRequired));
    graph.edges.push(Edge::new(0, "out", 1, "in"));

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
    graph.nodes.push(NodeInstance::new("consumer"));
    graph.nodes.push(
        NodeInstance::new("custom.host.gateway")
            .with_label("renamed-host")
            .with_metadata(HOST_BRIDGE_META_KEY, Value::Bool(true)),
    );
    graph.edges.push(Edge::new(1, "out", 0, "in"));

    let exec = daedalus_planner::ExecutionPlan::new(graph, vec![]);
    let runtime = RuntimePlan::from_execution(&exec);

    assert_eq!(runtime.schedule_order, vec![NodeRef(0), NodeRef(1)]);
}
