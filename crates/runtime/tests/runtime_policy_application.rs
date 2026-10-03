use daedalus_planner::{ExecutionPlan, Graph, NodeInstance};
use daedalus_runtime::{RuntimeEdgePolicy, SchedulerConfig, build_runtime};

#[test]
fn default_policy_applied_to_edges() {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance::new("a"));
    let exec = ExecutionPlan::new(graph, vec![]);

    let rt = build_runtime(
        &exec,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::latest_only(),
            backpressure: daedalus_runtime::BackpressureStrategy::BoundedQueues,
        },
    );
    assert!(rt.edges.is_empty());
    assert_eq!(rt.default_policy, RuntimeEdgePolicy::latest_only());
    assert!(matches!(
        rt.backpressure,
        daedalus_runtime::BackpressureStrategy::BoundedQueues
    ));
}
