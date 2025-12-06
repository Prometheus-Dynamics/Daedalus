use daedalus_planner::{ComputeAffinity, ExecutionPlan, Graph, NodeInstance};
use daedalus_runtime::{
    BackpressureStrategy, EdgePolicyKind, ExecuteError, Executor, NodeError, NodeHandler,
    RuntimeNode, SchedulerConfig, build_runtime,
};

struct FailingHandler;

impl NodeHandler for FailingHandler {
    fn run(
        &self,
        _node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        Err(NodeError::InvalidInput("missing required port".into()))
    }
}

#[test]
fn handler_error_bubbles_with_context() {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("fail"),
        bundle: None,
        label: None,
        inputs: vec![],
        outputs: vec![],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: Default::default(),
    });
    let exec = ExecutionPlan::new(graph, vec![]);
    let rt = build_runtime(
        &exec,
        &SchedulerConfig {
            default_policy: EdgePolicyKind::Fifo,
            backpressure: BackpressureStrategy::None,
            lockfree_queues: false,
        },
    );
    let err = Executor::new(&rt, FailingHandler).run().unwrap_err();
    match err {
        ExecuteError::HandlerFailed { node, error } => {
            assert_eq!(node, "fail");
            assert!(matches!(error, NodeError::InvalidInput(_)));
        }
        other => panic!("unexpected error: {:?}", other),
    }
}
