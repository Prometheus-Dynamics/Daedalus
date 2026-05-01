use std::sync::{Arc, Mutex};

use daedalus_planner::{
    ComputeAffinity, Edge, ExecutionPlan, Graph, NodeInstance, NodeRef, PortRef,
};
use daedalus_runtime::{
    BackpressureStrategy, ExecuteError, Executor, NodeError, NodeHandler, RuntimeEdgePolicy,
    RuntimeNode, SchedulerConfig, build_runtime,
};

#[derive(Clone)]
struct LogHandler {
    log: Arc<Mutex<Vec<String>>>,
}

impl NodeHandler for LogHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), daedalus_runtime::NodeError> {
        self.log.lock().unwrap().push(node.id.clone());
        Ok(())
    }
}

#[derive(Clone)]
struct PayloadBranchHandler {
    seen: Arc<Mutex<Vec<i32>>>,
}

impl NodeHandler for PayloadBranchHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), daedalus_runtime::NodeError> {
        match node.id.as_str() {
            "source" => {
                io.push_payload(
                    "out",
                    daedalus_transport::Payload::owned("test:i32", 40_i32),
                );
            }
            "left" => {
                let value = io
                    .get_typed::<i32>("in")
                    .ok_or_else(|| daedalus_runtime::NodeError::InvalidInput("left".into()))?;
                io.push_payload(
                    "out",
                    daedalus_transport::Payload::owned("test:i32", value + 1),
                );
            }
            "right" => {
                let value = io
                    .get_typed::<i32>("in")
                    .ok_or_else(|| daedalus_runtime::NodeError::InvalidInput("right".into()))?;
                io.push_payload(
                    "out",
                    daedalus_transport::Payload::owned("test:i32", value + 2),
                );
            }
            "sink" => {
                let mut values = io
                    .inputs()
                    .iter()
                    .filter_map(|(_, payload)| payload.inner.get_ref::<i32>().copied())
                    .collect::<Vec<_>>();
                values.sort_unstable();
                self.seen.lock().unwrap().extend(values);
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone)]
struct BoundedFanoutHandler {
    seen: Arc<Mutex<Vec<i32>>>,
}

impl NodeHandler for BoundedFanoutHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "source" => {
                io.push_payload(
                    "bounded",
                    daedalus_transport::Payload::owned("test:i32", 1_i32),
                );
                io.push_payload(
                    "bounded",
                    daedalus_transport::Payload::owned("test:i32", 2_i32),
                );
                io.push_payload(
                    "side",
                    daedalus_transport::Payload::owned("test:i32", 9_i32),
                );
            }
            "bounded_sink" | "side_sink" => {
                let mut values = io
                    .inputs()
                    .iter()
                    .filter_map(|(_, payload)| payload.inner.get_ref::<i32>().copied())
                    .collect::<Vec<_>>();
                values.sort_unstable();
                self.seen.lock().unwrap().extend(values);
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone)]
struct DirectChainHandler {
    seen: Arc<Mutex<Vec<i32>>>,
}

impl NodeHandler for DirectChainHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "n0" => {
                io.push_payload("out", daedalus_transport::Payload::owned("test:i32", 7_i32));
            }
            "n1" => {
                if let Some(value) = io.get_typed::<i32>("in") {
                    self.seen.lock().unwrap().push(value);
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone)]
struct FailingParallelHandler {
    seen: Arc<Mutex<Vec<String>>>,
}

impl NodeHandler for FailingParallelHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        self.seen.lock().unwrap().push(node.id.clone());
        if node.id.starts_with("bad") {
            return Err(NodeError::InvalidInput(
                "intentional parallel failure".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct PanicParallelHandler;

impl NodeHandler for PanicParallelHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        if node.id == "bad" {
            panic!("intentional parallel panic");
        }
        Ok(())
    }
}

fn chain_plan() -> ExecutionPlan {
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

fn branch_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    for (id, inputs, outputs) in [
        ("source", vec![], vec!["out"]),
        ("left", vec!["in"], vec!["out"]),
        ("right", vec!["in"], vec!["out"]),
        ("sink", vec!["left", "right"], vec![]),
    ] {
        graph.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new(id),
            bundle: None,
            label: None,
            inputs: inputs.into_iter().map(str::to_string).collect(),
            outputs: outputs.into_iter().map(str::to_string).collect(),
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
    }
    for (from, from_port, to, to_port) in [
        (0, "out", 1, "in"),
        (0, "out", 2, "in"),
        (1, "out", 3, "left"),
        (2, "out", 3, "right"),
    ] {
        graph.edges.push(Edge {
            from: PortRef {
                node: NodeRef(from),
                port: from_port.into(),
            },
            to: PortRef {
                node: NodeRef(to),
                port: to_port.into(),
            },
            metadata: Default::default(),
        });
    }
    ExecutionPlan::new(graph, vec![])
}

fn bounded_fanout_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    for (id, inputs, outputs) in [
        ("source", vec![], vec!["bounded", "side"]),
        ("bounded_sink", vec!["in"], vec![]),
        ("side_sink", vec!["in"], vec![]),
    ] {
        graph.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new(id),
            bundle: None,
            label: None,
            inputs: inputs.into_iter().map(str::to_string).collect(),
            outputs: outputs.into_iter().map(str::to_string).collect(),
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
    }
    for (from_port, to, to_port) in [("bounded", 1, "in"), ("side", 2, "in")] {
        graph.edges.push(Edge {
            from: PortRef {
                node: NodeRef(0),
                port: from_port.into(),
            },
            to: PortRef {
                node: NodeRef(to),
                port: to_port.into(),
            },
            metadata: Default::default(),
        });
    }
    ExecutionPlan::new(graph, vec![])
}

fn independent_failure_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    for id in ["good", "bad"] {
        graph.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new(id),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec![],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
    }
    ExecutionPlan::new(graph, vec![])
}

fn fail_first_independent_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    for id in ["bad", "good"] {
        graph.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new(id),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec![],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
    }
    ExecutionPlan::new(graph, vec![])
}

fn independent_multi_failure_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    for id in ["good", "bad_one", "bad_two"] {
        graph.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new(id),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec![],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
    }
    ExecutionPlan::new(graph, vec![])
}

#[test]
fn serial_and_parallel_scope_align() {
    let plan = chain_plan();
    let rt = build_runtime(
        &plan,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: daedalus_runtime::BackpressureStrategy::None,
        },
    );

    let log1 = Arc::new(Mutex::new(Vec::new()));
    let h1 = LogHandler { log: log1.clone() };
    let telem1 = Executor::new(&rt, h1).run().expect("serial run");
    assert_eq!(telem1.nodes_executed, 2);

    let log2 = Arc::new(Mutex::new(Vec::new()));
    let h2 = LogHandler { log: log2.clone() };
    let telem2 = Executor::new(&rt, h2).run_parallel().expect("parallel run");
    assert_eq!(telem2.nodes_executed, 2);

    assert_eq!(*log1.lock().unwrap(), *log2.lock().unwrap());
}

#[test]
fn parallel_payload_branch_merges_outputs() {
    let plan = branch_plan();
    let rt = build_runtime(
        &plan,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: daedalus_runtime::BackpressureStrategy::None,
        },
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = PayloadBranchHandler { seen: seen.clone() };
    let telemetry = Executor::new(&rt, handler)
        .with_pool_size(Some(2))
        .run_parallel()
        .expect("parallel payload run");

    assert_eq!(telemetry.nodes_executed, 4);
    assert_eq!(*seen.lock().unwrap(), vec![41, 42]);
}

#[cfg(feature = "executor-pool")]
#[test]
fn direct_runtime_parallel_path_can_prewarm_worker_pool() {
    let plan = branch_plan();
    let rt = build_runtime(
        &plan,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: daedalus_runtime::BackpressureStrategy::None,
        },
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = PayloadBranchHandler { seen: seen.clone() };
    let mut executor = Executor::new(&rt, handler).with_pool_size(Some(2));

    executor
        .prewarm_worker_pool()
        .expect("worker pool prewarm succeeds");
    let telemetry = executor
        .run_parallel_in_place()
        .expect("pooled parallel run succeeds");

    assert_eq!(telemetry.nodes_executed, 4);
    assert_eq!(*seen.lock().unwrap(), vec![41, 42]);
}

#[test]
fn serial_run_in_place_resets_direct_slots_between_ticks() {
    let plan = branch_plan();
    let rt = build_runtime(
        &plan,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: daedalus_runtime::BackpressureStrategy::None,
        },
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = PayloadBranchHandler { seen: seen.clone() };
    let mut executor = Executor::new(&rt, handler);

    let first = executor.run_in_place().expect("first serial in-place run");
    let second = executor.run_in_place().expect("second serial in-place run");

    assert_eq!(first.nodes_executed, 4);
    assert_eq!(second.nodes_executed, 4);
    assert_eq!(*seen.lock().unwrap(), vec![41, 42, 41, 42]);
}

#[test]
fn parallel_run_in_place_uses_locked_direct_slots_between_ticks() {
    let plan = branch_plan();
    let rt = build_runtime(
        &plan,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: daedalus_runtime::BackpressureStrategy::None,
        },
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = PayloadBranchHandler { seen: seen.clone() };
    let mut executor = Executor::new(&rt, handler).with_pool_size(Some(2));

    let first = executor
        .run_parallel_in_place()
        .expect("first parallel in-place run");
    let second = executor
        .run_parallel_in_place()
        .expect("second parallel in-place run");

    assert_eq!(first.nodes_executed, 4);
    assert_eq!(second.nodes_executed, 4);
    assert_eq!(*seen.lock().unwrap(), vec![41, 42, 41, 42]);
}

#[test]
fn parallel_latest_only_direct_slot_transfers_payloads_between_ticks() {
    let plan = chain_plan();
    let rt = build_runtime(
        &plan,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::latest_only(),
            backpressure: daedalus_runtime::BackpressureStrategy::None,
        },
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = DirectChainHandler { seen: seen.clone() };
    let mut executor = Executor::new(&rt, handler).with_pool_size(Some(2));

    let first = executor
        .run_parallel_in_place()
        .expect("first direct-slot parallel run");
    let second = executor
        .run_parallel_in_place()
        .expect("second direct-slot parallel run");

    assert_eq!(first.nodes_executed, 2);
    assert_eq!(second.nodes_executed, 2);
    assert_eq!(*seen.lock().unwrap(), vec![7, 7]);
}

#[test]
fn retained_executor_can_switch_direct_slots_between_serial_and_parallel_ticks() {
    let plan = branch_plan();
    let rt = build_runtime(
        &plan,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: BackpressureStrategy::None,
        },
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = PayloadBranchHandler { seen: seen.clone() };
    let mut executor = Executor::new(&rt, handler).with_pool_size(Some(2));

    for _ in 0..16 {
        let serial = executor.run_in_place().expect("serial retained tick");
        let parallel = executor
            .run_parallel_in_place()
            .expect("parallel retained tick");
        assert_eq!(serial.nodes_executed, 4);
        assert_eq!(parallel.nodes_executed, 4);
    }

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 64);
    for pair in seen.chunks_exact(2) {
        assert_eq!(pair, [41, 42]);
    }
}

#[test]
fn parallel_bounded_queue_reports_backpressure_without_blocking_independent_branch() {
    let plan = bounded_fanout_plan();
    let rt = build_runtime(
        &plan,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::bounded(1),
            backpressure: BackpressureStrategy::BoundedQueues,
        },
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = BoundedFanoutHandler { seen: seen.clone() };
    let telemetry = Executor::new(&rt, handler)
        .with_pool_size(Some(2))
        .run_parallel()
        .expect("parallel bounded queue run");

    assert_eq!(telemetry.nodes_executed, 3);
    assert_eq!(telemetry.backpressure_events, 1);
    let mut seen_values = seen.lock().unwrap().clone();
    seen_values.sort_unstable();
    assert_eq!(seen_values, vec![1, 9]);
}

#[test]
fn parallel_bounded_error_overflow_fails_segment() {
    let plan = bounded_fanout_plan();
    let rt = build_runtime(
        &plan,
        &SchedulerConfig {
            default_policy: RuntimeEdgePolicy::bounded(1),
            backpressure: BackpressureStrategy::ErrorOnOverflow,
        },
    );

    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = BoundedFanoutHandler { seen: seen.clone() };
    let err = Executor::new(&rt, handler)
        .with_pool_size(Some(2))
        .run_parallel()
        .expect_err("parallel bounded overflow should fail");

    assert!(matches!(
        err,
        ExecuteError::HandlerFailed {
            error: NodeError::BackpressureDrop(_),
            ..
        }
    ));
    assert!(seen.lock().unwrap().is_empty());
}

#[test]
fn parallel_non_fail_fast_records_segment_errors_and_completes_ready_work() {
    let plan = independent_multi_failure_plan();
    let rt = build_runtime(&plan, &SchedulerConfig::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = FailingParallelHandler { seen: seen.clone() };

    let telemetry = Executor::new(&rt, handler)
        .with_pool_size(Some(2))
        .with_fail_fast(false)
        .run_parallel()
        .expect("non-fail-fast parallel run");

    assert_eq!(telemetry.nodes_executed, 3);
    assert_eq!(telemetry.errors.len(), 2);
    assert!(
        telemetry
            .errors
            .iter()
            .all(|error| error.message.contains("intentional parallel failure"))
    );
    assert!(
        telemetry
            .errors
            .iter()
            .any(|error| error.node_id.contains("bad_one"))
    );
    assert!(
        telemetry
            .errors
            .iter()
            .any(|error| error.node_id.contains("bad_two"))
    );
    let mut seen = seen.lock().unwrap().clone();
    seen.sort();
    assert_eq!(
        seen,
        vec![
            "bad_one".to_string(),
            "bad_two".to_string(),
            "good".to_string()
        ]
    );
}

#[test]
fn parallel_fail_fast_returns_first_segment_error() {
    let plan = independent_failure_plan();
    let rt = build_runtime(&plan, &SchedulerConfig::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = FailingParallelHandler { seen };

    let err = Executor::new(&rt, handler)
        .with_pool_size(Some(2))
        .run_parallel()
        .expect_err("fail-fast parallel run should fail");

    match err {
        ExecuteError::HandlerFailed { node, error } => {
            assert_eq!(node, "bad");
            assert!(error.to_string().contains("intentional parallel failure"));
        }
        other => panic!("unexpected parallel error: {other:?}"),
    }
}

#[test]
fn parallel_fail_fast_stops_scheduling_new_segments_after_error() {
    let plan = fail_first_independent_plan();
    let rt = build_runtime(&plan, &SchedulerConfig::default());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = FailingParallelHandler { seen: seen.clone() };

    let err = Executor::new(&rt, handler)
        .with_pool_size(Some(1))
        .run_parallel()
        .expect_err("fail-fast parallel run should fail");

    match err {
        ExecuteError::HandlerFailed { node, error } => {
            assert_eq!(node, "bad");
            assert!(error.to_string().contains("intentional parallel failure"));
        }
        other => panic!("unexpected parallel error: {other:?}"),
    }
    assert_eq!(*seen.lock().unwrap(), vec!["bad".to_string()]);
}

#[test]
fn parallel_worker_panic_returns_typed_error_without_stranding_scheduler() {
    let plan = independent_failure_plan();
    let rt = build_runtime(&plan, &SchedulerConfig::default());

    let err = Executor::new(&rt, PanicParallelHandler)
        .with_pool_size(Some(2))
        .run_parallel()
        .expect_err("panic should become executor error");

    match err {
        ExecuteError::HandlerPanicked { node, message } => {
            assert!(node.starts_with("segment_"));
            assert!(message.contains("intentional parallel panic"));
        }
        other => panic!("unexpected parallel error: {other:?}"),
    }
}
