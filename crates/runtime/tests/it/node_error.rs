use daedalus_data::model::Value;
use daedalus_planner::{ExecutionPlan, Graph, NodeInstance};
use daedalus_runtime::{
    BackpressureStrategy, ExecuteError, Executor, NODE_EXECUTION_KIND_META_KEY, NodeError,
    NodeHandler, RuntimeEdgePolicy, RuntimeNode, SchedulerConfig, build_runtime,
    handler_registry::HandlerRegistry,
};

fn single_node_plan(id: &str, execution_kind: Option<&str>) -> daedalus_runtime::RuntimePlan {
    let mut graph = Graph::default();
    let mut metadata = std::collections::BTreeMap::new();
    if let Some(kind) = execution_kind {
        metadata.insert(
            NODE_EXECUTION_KIND_META_KEY.to_string(),
            Value::String(kind.to_string().into()),
        );
    }
    graph.nodes.push(NodeInstance {
        metadata,
        ..NodeInstance::new(id)
    });
    let exec = ExecutionPlan::new(graph, vec![]);
    build_runtime(
        &exec,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: BackpressureStrategy::None,
        },
    )
}

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

/// The default contains a missing handler: the run succeeds and the failure is recorded.
#[test]
fn missing_handler_is_contained_by_default() {
    let rt = single_node_plan("unregistered", None);
    let telemetry = Executor::new(&rt, HandlerRegistry::new())
        .run()
        .expect("the default contains the failure");
    assert_eq!(telemetry.errors[0].node_id, "unregistered");
    assert_eq!(telemetry.errors[0].code, "missing_handler");
}

/// With fail-fast the missing handler ends the run, with the node and error.
#[test]
fn missing_handler_bubbles_with_context() {
    let rt = single_node_plan("unregistered", None);
    let err = Executor::new(&rt, HandlerRegistry::new())
        .with_fail_fast(true)
        .run()
        .unwrap_err();
    match err {
        ExecuteError::HandlerFailed { node, error } => {
            assert_eq!(node, "unregistered");
            assert!(matches!(
                error,
                NodeError::MissingHandler { ref node, .. } if node == "unregistered"
            ));
            assert_eq!(error.code(), "missing_handler");
        }
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn explicit_no_op_node_can_run_without_handler() {
    let rt = single_node_plan("noop", Some("no_op"));

    let telemetry = Executor::new(&rt, HandlerRegistry::new())
        .run()
        .expect("explicit no-op node should not require a handler");

    assert_eq!(telemetry.nodes_executed, 1);
}

#[test]
fn external_handler_failure_is_contained_by_default() {
    let rt = single_node_plan("external", Some("external"));
    let telemetry = Executor::new(&rt, HandlerRegistry::new())
        .run()
        .expect("the default contains the failure");
    assert_eq!(telemetry.errors[0].node_id, "external");
    assert_eq!(telemetry.errors[0].code, "external_handler_unavailable");
}

#[test]
fn explicit_external_node_reports_external_handler_error() {
    let rt = single_node_plan("external", Some("external"));

    let err = Executor::new(&rt, HandlerRegistry::new())
        .with_fail_fast(true)
        .run()
        .unwrap_err();

    match err {
        ExecuteError::HandlerFailed { node, error } => {
            assert_eq!(node, "external");
            assert!(matches!(
                error,
                NodeError::ExternalHandlerUnavailable { ref node, .. } if node == "external"
            ));
            assert_eq!(error.code(), "external_handler_unavailable");
        }
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn invalid_execution_kind_is_contained_by_default() {
    let rt = single_node_plan("invalid_kind", Some("definitely_not_a_kind"));
    let telemetry = Executor::new(&rt, HandlerRegistry::new())
        .run()
        .expect("the default contains the failure");
    assert_eq!(telemetry.errors[0].node_id, "invalid_kind");
    assert_eq!(telemetry.errors[0].code, "missing_handler");
}

#[test]
fn invalid_execution_kind_metadata_defaults_to_handler_required() {
    let rt = single_node_plan("invalid_kind", Some("definitely_not_a_kind"));

    let err = Executor::new(&rt, HandlerRegistry::new())
        .with_fail_fast(true)
        .run()
        .unwrap_err();

    match err {
        ExecuteError::HandlerFailed { node, error } => {
            assert_eq!(node, "invalid_kind");
            assert!(matches!(
                error,
                NodeError::MissingHandler { ref node, .. } if node == "invalid_kind"
            ));
        }
        other => panic!("unexpected error: {:?}", other),
    }
}

#[test]
fn handler_error_is_contained_by_default() {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance::new("fail"));
    let exec = ExecutionPlan::new(graph, vec![]);
    let rt = build_runtime(
        &exec,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: BackpressureStrategy::None,
        },
    );
    let telemetry = Executor::new(&rt, FailingHandler)
        .run()
        .expect("the default contains the failure");
    assert_eq!(telemetry.errors[0].node_id, "fail");
    assert_eq!(
        telemetry.errors[0].code,
        NodeError::InvalidInput(String::new()).code()
    );
    assert_eq!(telemetry.recovery().skipped_frames, 1);
}

#[test]
fn handler_error_bubbles_with_context() {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance::new("fail"));
    let exec = ExecutionPlan::new(graph, vec![]);
    let rt = build_runtime(
        &exec,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: BackpressureStrategy::None,
        },
    );
    let err = Executor::new(&rt, FailingHandler)
        .with_fail_fast(true)
        .run()
        .unwrap_err();
    match err {
        ExecuteError::HandlerFailed { node, error } => {
            assert_eq!(node, "fail");
            assert!(matches!(error, NodeError::InvalidInput(_)));
        }
        other => panic!("unexpected error: {:?}", other),
    }
}
