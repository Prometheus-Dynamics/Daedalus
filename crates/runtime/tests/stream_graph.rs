use std::collections::BTreeMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use daedalus_data::model::Value;
use daedalus_planner::{
    ComputeAffinity, Edge, ExecutionPlan, Graph, NodeInstance, NodeRef, PortRef,
};
use daedalus_runtime::HostBridgeManager;
use daedalus_runtime::executor::OwnedExecutor;
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY};
use daedalus_runtime::{
    HostBridgeConfig, NodeError, NodeHandler, RuntimeEdgePolicy, RuntimeNode, SchedulerConfig,
    SharedStreamGraph, StreamExecutionMode, StreamGraph, StreamGraphState, StreamWorkerState,
    StreamWorkerStopError, build_runtime,
};
use daedalus_transport::{FeedOutcome, FreshnessPolicy, Payload, PressurePolicy};

struct EchoHandler;

impl NodeHandler for EchoHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        if node.id == "echo" {
            let inputs: Vec<_> = io
                .inputs_for("in")
                .map(|payload| payload.inner.clone())
                .collect();
            for payload in inputs {
                io.push_payload("out", payload);
            }
        }
        Ok(())
    }
}

struct SlowHandler {
    started: Mutex<Option<mpsc::Sender<()>>>,
    finished: Mutex<Option<mpsc::Sender<()>>>,
    sleep: Duration,
}

impl NodeHandler for SlowHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        if node.id == "echo" {
            if let Some(tx) = self.started.lock().expect("started lock").take() {
                let _ = tx.send(());
            }
            std::thread::sleep(self.sleep);
            if let Some(tx) = self.finished.lock().expect("finished lock").take() {
                let _ = tx.send(());
            }
        }
        Ok(())
    }
}

fn stream_echo_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new(HOST_BRIDGE_ID),
        bundle: None,
        label: Some("host".into()),
        inputs: vec!["out".into()],
        outputs: vec!["in".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: BTreeMap::from([
            (HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true)),
            (
                "dynamic_inputs".to_string(),
                Value::String(std::borrow::Cow::Borrowed("generic")),
            ),
            (
                "dynamic_outputs".to_string(),
                Value::String(std::borrow::Cow::Borrowed("generic")),
            ),
        ]),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("echo"),
        bundle: None,
        label: None,
        inputs: vec!["in".into()],
        outputs: vec!["out".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: BTreeMap::new(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "in".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "in".into(),
        },
        metadata: BTreeMap::new(),
    });
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(1),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(0),
            port: "out".into(),
        },
        metadata: BTreeMap::new(),
    });
    ExecutionPlan::new(graph, vec![])
}

fn two_input_stream_echo_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new(HOST_BRIDGE_ID),
        bundle: None,
        label: Some("host".into()),
        inputs: vec!["out".into()],
        outputs: vec!["left".into(), "right".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: BTreeMap::from([
            (HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true)),
            (
                "dynamic_inputs".to_string(),
                Value::String(std::borrow::Cow::Borrowed("generic")),
            ),
            (
                "dynamic_outputs".to_string(),
                Value::String(std::borrow::Cow::Borrowed("generic")),
            ),
        ]),
    });
    graph.nodes.push(NodeInstance {
        id: daedalus_registry::ids::NodeId::new("echo"),
        bundle: None,
        label: None,
        inputs: vec!["in".into()],
        outputs: vec!["out".into()],
        compute: ComputeAffinity::CpuOnly,
        const_inputs: vec![],
        sync_groups: vec![],
        metadata: BTreeMap::new(),
    });
    for port in ["left", "right"] {
        graph.edges.push(Edge {
            from: PortRef {
                node: NodeRef(0),
                port: port.into(),
            },
            to: PortRef {
                node: NodeRef(1),
                port: "in".into(),
            },
            metadata: BTreeMap::new(),
        });
    }
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(1),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(0),
            port: "out".into(),
        },
        metadata: BTreeMap::new(),
    });
    ExecutionPlan::new(graph, vec![])
}

fn recv_u32(output: &daedalus_runtime::GraphOutput) -> u32 {
    output
        .recv_timeout(Duration::from_secs(2))
        .expect("receive should not fail")
        .expect("payload should arrive before timeout")
        .get_ref::<u32>()
        .copied()
        .expect("payload should be u32")
}

#[test]
fn graph_input_close_is_scoped_to_that_input_port() {
    let runtime = Arc::new(build_runtime(
        &two_input_stream_echo_plan(),
        &SchedulerConfig::default(),
    ));
    let mut graph = StreamGraph::new(runtime, EchoHandler);
    let left = graph.input("left").expect("left input handle");
    let right = graph.input("right").expect("right input handle");
    let output = graph.output("out").expect("output handle");

    graph.start().expect("start");
    left.close().expect("close left input");
    assert!(left.stats().closed);
    assert!(!right.stats().closed);

    assert!(matches!(
        left.feed(Payload::owned("demo:u32", 10u32))
            .expect("left feed should return an outcome"),
        FeedOutcome::Dropped { .. }
    ));
    assert!(matches!(
        right
            .feed(Payload::owned("demo:u32", 22u32))
            .expect("right feed should return an outcome"),
        FeedOutcome::Accepted { .. }
    ));

    graph.drain().expect("drain");
    assert_eq!(recv_u32(&output), 22);
}

#[test]
fn stream_graph_diagnostics_report_retained_serial_execution() {
    let runtime = Arc::new(build_runtime(
        &stream_echo_plan(),
        &SchedulerConfig::default(),
    ));
    let graph = StreamGraph::new(runtime, EchoHandler);

    assert_eq!(
        graph.diagnostics().execution_mode,
        StreamExecutionMode::RetainedSerial
    );
}

#[test]
fn stream_graph_diagnostics_include_applied_host_config() {
    let runtime = Arc::new(build_runtime(
        &stream_echo_plan(),
        &SchedulerConfig::default(),
    ));
    let graph = StreamGraph::new(runtime, EchoHandler);
    let config = HostBridgeConfig::default()
        .with_default_input_policy(RuntimeEdgePolicy::bounded(4))
        .with_default_output_policy(RuntimeEdgePolicy::bounded(8))
        .with_event_recording(false)
        .with_event_limit(Some(3));

    graph.apply_host_config(&config).expect("host config");

    assert_eq!(graph.diagnostics().host_config, config);
}

#[test]
fn inactive_host_input_port_remains_queued() {
    let runtime = Arc::new(build_runtime(
        &two_input_stream_echo_plan(),
        &SchedulerConfig::default(),
    ));
    let bridges = HostBridgeManager::new();
    bridges.populate_from_plan(&runtime);
    let host = bridges.ensure_handle("host");
    let mut executor = OwnedExecutor::new(runtime, EchoHandler)
        .with_host_bridges(bridges)
        .try_with_active_edges_mask(Some(Arc::new(vec![false, true, true])))
        .expect("active edge mask");

    assert!(matches!(
        host.feed_payload("left", Payload::owned("demo:u32", 10u32)),
        FeedOutcome::Accepted { .. }
    ));
    assert!(matches!(
        host.feed_payload("right", Payload::owned("demo:u32", 22u32)),
        FeedOutcome::Accepted { .. }
    ));

    executor.run_in_place().expect("run active right side");

    assert_eq!(host.pending_inbound(), 1);
    let retained = host
        .try_pop_payload("out")
        .expect("right output")
        .get_ref::<u32>()
        .copied();
    assert_eq!(retained, Some(22));
}

#[test]
fn continuous_worker_handles_pause_resume_and_shutdown_under_pressure() {
    let runtime = Arc::new(build_runtime(
        &stream_echo_plan(),
        &SchedulerConfig::default(),
    ));
    let graph: SharedStreamGraph<EchoHandler> =
        Arc::new(Mutex::new(StreamGraph::new(runtime, EchoHandler)));

    let (input, output) = {
        let graph = graph.lock().expect("stream graph lock should be available");
        let input = graph.input("in").expect("input handle");
        let output = graph.output("out").expect("output handle");
        (input, output)
    };
    input
        .set_policy(PressurePolicy::BufferAll, FreshnessPolicy::PreserveAll)
        .expect("input policy");
    output
        .set_policy(PressurePolicy::BufferAll, FreshnessPolicy::PreserveAll)
        .expect("output policy");

    graph
        .lock()
        .expect("stream graph lock")
        .start()
        .expect("start");
    assert_eq!(
        graph
            .lock()
            .expect("stream graph lock")
            .diagnostics()
            .worker_state,
        StreamWorkerState::Idle
    );
    let worker = StreamGraph::spawn_continuous(Arc::clone(&graph), Duration::from_millis(1));

    for value in 0..32u32 {
        input
            .feed(Payload::owned("demo:u32", value))
            .expect("feed should succeed");
    }
    assert!(matches!(
        graph
            .lock()
            .expect("stream graph lock")
            .diagnostics()
            .worker_state,
        StreamWorkerState::Running | StreamWorkerState::BlockedInExecution
    ));
    let first_batch: Vec<_> = (0..32).map(|_| recv_u32(&output)).collect();
    assert_eq!(first_batch, (0..32u32).collect::<Vec<_>>());

    {
        let mut graph = graph.lock().expect("stream graph lock");
        graph.pause().expect("pause");
        assert_eq!(graph.state(), StreamGraphState::Paused);
        assert_eq!(graph.diagnostics().worker_state, StreamWorkerState::Paused);
    }
    for value in 32..64u32 {
        input
            .feed(Payload::owned("demo:u32", value))
            .expect("feed while paused should succeed");
    }
    std::thread::sleep(Duration::from_millis(20));
    assert!(
        output
            .try_recv()
            .expect("try_recv while paused should not fail")
            .is_none()
    );

    {
        let mut graph = graph.lock().expect("stream graph lock");
        graph.resume().expect("resume");
        assert_eq!(graph.state(), StreamGraphState::Running);
    }
    let second_batch: Vec<_> = (0..32).map(|_| recv_u32(&output)).collect();
    assert_eq!(second_batch, (32..64u32).collect::<Vec<_>>());

    graph
        .lock()
        .expect("stream graph lock")
        .close()
        .expect("close");
    assert_eq!(
        graph
            .lock()
            .expect("stream graph lock")
            .diagnostics()
            .worker_state,
        StreamWorkerState::Closed
    );
    assert!(worker.stop().is_none());
}

#[test]
fn continuous_worker_releases_graph_lock_while_handler_runs() {
    let runtime = Arc::new(build_runtime(
        &stream_echo_plan(),
        &SchedulerConfig::default(),
    ));
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let graph: SharedStreamGraph<SlowHandler> = Arc::new(Mutex::new(StreamGraph::new(
        runtime,
        SlowHandler {
            started: Mutex::new(Some(started_tx)),
            finished: Mutex::new(Some(finished_tx)),
            sleep: Duration::from_millis(150),
        },
    )));

    let input = {
        let graph = graph.lock().expect("stream graph lock should be available");
        graph.input("in").expect("input handle")
    };

    graph
        .lock()
        .expect("stream graph lock")
        .start()
        .expect("start");
    let worker = StreamGraph::spawn_continuous(Arc::clone(&graph), Duration::from_millis(1));
    input
        .feed(Payload::owned("demo:u32", 1u32))
        .expect("feed should succeed");
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("handler should start");
    let diagnostics = graph.lock().expect("stream graph lock").diagnostics();
    assert_eq!(
        diagnostics.worker_state,
        StreamWorkerState::BlockedInExecution
    );
    assert!(diagnostics.current_execution_elapsed.is_some());

    let (paused_tx, paused_rx) = mpsc::channel();
    let pause_graph = Arc::clone(&graph);
    let pause_thread = std::thread::spawn(move || {
        pause_graph
            .lock()
            .expect("stream graph lock")
            .pause()
            .expect("pause");
        paused_tx.send(()).expect("pause notification");
    });

    paused_rx
        .recv_timeout(Duration::from_millis(50))
        .expect("pause should acquire graph lock while handler is still running");
    pause_thread.join().expect("pause thread");
    finished_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("handler should eventually finish");

    graph
        .lock()
        .expect("stream graph lock")
        .close()
        .expect("close");
    let deadline = Instant::now() + Duration::from_secs(2);
    while graph
        .lock()
        .expect("stream graph lock")
        .diagnostics()
        .last_execution_duration
        .is_none()
    {
        assert!(
            Instant::now() < deadline,
            "worker should publish last execution duration"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(worker.stop().is_none());
}

#[test]
fn continuous_worker_stop_timeout_reports_slow_handler_without_deadlocking() {
    let runtime = Arc::new(build_runtime(
        &stream_echo_plan(),
        &SchedulerConfig::default(),
    ));
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let graph: SharedStreamGraph<SlowHandler> = Arc::new(Mutex::new(StreamGraph::new(
        runtime,
        SlowHandler {
            started: Mutex::new(Some(started_tx)),
            finished: Mutex::new(Some(finished_tx)),
            sleep: Duration::from_millis(120),
        },
    )));

    let input = {
        let graph = graph.lock().expect("stream graph lock should be available");
        graph.input("in").expect("input handle")
    };

    graph
        .lock()
        .expect("stream graph lock")
        .start()
        .expect("start");
    let mut worker = StreamGraph::spawn_continuous(Arc::clone(&graph), Duration::from_millis(1));
    input
        .feed(Payload::owned("demo:u32", 1u32))
        .expect("feed should succeed");
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("handler should start");

    let timeout = Duration::from_millis(10);
    assert_eq!(
        worker.stop_timeout(timeout),
        Err(StreamWorkerStopError::Timeout { timeout })
    );
    let diagnostics = worker.diagnostics();
    assert!(diagnostics.stop_requested);
    assert!(!diagnostics.worker_finished);
    assert!(diagnostics.shutdown_pending);
    assert!(diagnostics.stop_requested_elapsed.is_some());
    assert_eq!(diagnostics.last_error, None);

    finished_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("handler should eventually finish");
    assert_eq!(worker.stop_timeout(Duration::from_secs(1)), Ok(None));
    let diagnostics = worker.diagnostics();
    assert!(diagnostics.stop_requested);
    assert!(diagnostics.worker_finished);
    assert!(!diagnostics.shutdown_pending);
}

#[test]
fn continuous_worker_drop_requests_stop_without_waiting_for_slow_handler() {
    let runtime = Arc::new(build_runtime(
        &stream_echo_plan(),
        &SchedulerConfig::default(),
    ));
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let graph: SharedStreamGraph<SlowHandler> = Arc::new(Mutex::new(StreamGraph::new(
        runtime,
        SlowHandler {
            started: Mutex::new(Some(started_tx)),
            finished: Mutex::new(Some(finished_tx)),
            sleep: Duration::from_millis(200),
        },
    )));

    let input = {
        let graph = graph.lock().expect("stream graph lock should be available");
        graph.input("in").expect("input handle")
    };

    graph
        .lock()
        .expect("stream graph lock")
        .start()
        .expect("start");
    let worker = StreamGraph::spawn_continuous(Arc::clone(&graph), Duration::from_millis(1));
    input
        .feed(Payload::owned("demo:u32", 1u32))
        .expect("feed should succeed");
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("handler should start");

    let drop_started = Instant::now();
    drop(worker);
    assert!(
        drop_started.elapsed() < Duration::from_millis(50),
        "dropping a worker should not wait for an in-flight handler"
    );
    finished_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("handler should still finish after drop requests stop");
}
