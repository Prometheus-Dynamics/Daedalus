use daedalus_planner::{ComputeAffinity, Edge, ExecutionPlan, Graph, NodeInstance, NodeRef};
use daedalus_runtime::{RuntimeEdgePolicy, SchedulerConfig, build_runtime, debug};
use serde_json::Value;

#[test]
fn runtime_plan_cpu_golden() {
    // Planner graph: a(out) -> b(in), both CPU
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance::new("a"));
    graph.nodes.push(NodeInstance::new("b"));
    graph.edges.push(Edge::new(0, "out", 1, "in"));

    let exec = ExecutionPlan::new(graph, vec![]);
    let runtime = build_runtime(&exec, &SchedulerConfig::default());
    // sanity
    assert_eq!(runtime.edges[0].policy(), &RuntimeEdgePolicy::default());
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
    graph.nodes.push(NodeInstance::new("cpu0"));
    graph
        .nodes
        .push(NodeInstance::new("gpu1").with_compute(ComputeAffinity::GpuRequired));
    graph
        .nodes
        .push(NodeInstance::new("gpu2").with_compute(ComputeAffinity::GpuPreferred));
    graph.nodes.push(NodeInstance::new("cpu1"));

    graph.edges.push(Edge::new(0, "out", 1, "in"));
    graph.edges.push(Edge::new(1, "out", 2, "in"));
    graph.edges.push(Edge::new(2, "out", 3, "in"));

    let exec = ExecutionPlan::new(graph, vec![]);
    let runtime = build_runtime(&exec, &SchedulerConfig::default());
    // segments should group contiguous GPU nodes (gpu1 + gpu2)
    assert_eq!(runtime.segments.len(), 3);
    assert_eq!(runtime.segments[1].nodes, vec![NodeRef(1), NodeRef(2)]);
    assert_eq!(
        runtime.schedule_order,
        vec![NodeRef(0), NodeRef(1), NodeRef(2), NodeRef(3)]
    );

    let actual: Value = serde_json::from_str(&debug::to_pretty_json(&runtime)).unwrap();
    let expected: Value =
        serde_json::from_str(include_str!("goldens/runtime_plan_gpu_segment.json")).unwrap();
    assert_eq!(actual, expected);
}
