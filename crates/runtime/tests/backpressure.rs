use std::sync::{Arc, Mutex};

use daedalus_planner::{
    ComputeAffinity, Edge, ExecutionPlan, Graph, NodeInstance, NodeRef, PortRef,
};
use daedalus_runtime::{
    BackpressureStrategy, ExecuteError, Executor, NodeHandler, RuntimeEdgePolicy, RuntimeNode,
    SchedulerConfig, build_runtime,
};

struct Harness {
    seen: Arc<Mutex<Vec<String>>>,
}

impl NodeHandler for Harness {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), daedalus_runtime::NodeError> {
        match node.id.as_str() {
            "prod" => {
                io.push_payload(
                    "out",
                    daedalus_transport::Payload::bytes(Arc::from(&b"one"[..])),
                );
                io.push_payload(
                    "out",
                    daedalus_transport::Payload::bytes(Arc::from(&b"two"[..])),
                );
            }
            "cons" => {
                let mut guard = self.seen.lock().unwrap();
                for payload in io.inputs_for("in") {
                    if let Some(bytes) = payload.inner.get_bytes() {
                        guard.push(String::from_utf8_lossy(&bytes).into());
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("prod"),
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
        id: daedalus_registry::ids::NodeId::new("cons"),
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
            node: NodeRef(1),
            port: "in".into(),
        },
        metadata: Default::default(),
    });
    ExecutionPlan::new(graph, vec![])
}

#[test]
fn fifo_drains_all_inputs() {
    let exec = plan();
    let rt = build_runtime(
        &exec,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: BackpressureStrategy::None,
        },
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = Harness { seen: seen.clone() };
    let telemetry = Executor::new(&rt, handler).run().expect("run");
    assert_eq!(telemetry.backpressure_events, 0);
    assert_eq!(
        seen.lock().unwrap().clone(),
        vec!["one".to_string(), "two".to_string()]
    );
}

#[test]
fn bounded_backpressure_warns_and_preserves_queue() {
    let exec = plan();
    let rt = build_runtime(
        &exec,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::bounded(1),
            backpressure: BackpressureStrategy::BoundedQueues,
        },
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = Harness { seen: seen.clone() };
    let telemetry = Executor::new(&rt, handler).run().expect("run");
    assert_eq!(telemetry.backpressure_events, 1);
    // Second payload should be rejected, first retained.
    assert_eq!(seen.lock().unwrap().clone(), vec!["one".to_string()]);
    assert_eq!(telemetry.warnings.len(), 1);
}

#[test]
fn bounded_backpressure_error_fails_fast() {
    let exec = plan();
    let rt = build_runtime(
        &exec,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::bounded(1),
            backpressure: BackpressureStrategy::ErrorOnOverflow,
        },
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = Harness { seen: seen.clone() };
    let err = Executor::new(&rt, handler)
        .run()
        .expect_err("overflow fails");
    assert!(matches!(
        err,
        ExecuteError::HandlerFailed {
            error: daedalus_runtime::NodeError::BackpressureDrop(_),
            ..
        }
    ));
    assert!(seen.lock().unwrap().is_empty());
}
