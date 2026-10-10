//! Stream graphs driven by continuous worker threads.
#![cfg(feature = "threads")]

use daedalus_runtime::sync::Mutex;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use daedalus_data::model::Value;
use daedalus_planner::{Edge, ExecutionPlan, Graph, NodeInstance};
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

/// Bound on every wait in this file. Each wait ends on the event it names (a message, a state
/// transition), so this only keeps a broken build from hanging the run; no test asserts that
/// something happened within a duration.
const SAFETY_TIMEOUT: Duration = Duration::from_secs(30);

struct SlowHandler {
    started: Mutex<Option<mpsc::Sender<()>>>,
    finished: Mutex<Option<mpsc::Sender<()>>>,
    /// The handler parks here until the test sends on (or drops) the matching sender, so the test
    /// decides exactly when the handler is in flight and when it returns.
    release: Mutex<Option<mpsc::Receiver<()>>>,
}

impl NodeHandler for SlowHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        if node.id == "echo" {
            if let Some(tx) = self.started.lock().take() {
                let _ = tx.send(());
            }
            let release = self.release.lock().take();
            if let Some(release) = release {
                let _ = release.recv_timeout(SAFETY_TIMEOUT);
            }
            if let Some(tx) = self.finished.lock().take() {
                let _ = tx.send(());
            }
        }
        Ok(())
    }
}

/// A [`SlowHandler`] and the channels that drive it.
struct GatedHandler {
    handler: SlowHandler,
    /// Fires when the handler is in flight.
    started: mpsc::Receiver<()>,
    /// Fires when the handler has returned.
    finished: mpsc::Receiver<()>,
    /// Lets the handler return.
    release: mpsc::Sender<()>,
}

fn gated_handler() -> GatedHandler {
    let (started_tx, started) = mpsc::channel();
    let (finished_tx, finished) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    GatedHandler {
        handler: SlowHandler {
            started: Mutex::new(Some(started_tx)),
            finished: Mutex::new(Some(finished_tx)),
            release: Mutex::new(Some(release_rx)),
        },
        started,
        finished,
        release,
    }
}

fn stream_echo_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(
        NodeInstance::new(HOST_BRIDGE_ID)
            .with_label("host")
            .with_inputs(["out"])
            .with_outputs(["in"])
            .with_metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
            .with_metadata(
                "dynamic_inputs",
                Value::String(std::borrow::Cow::Borrowed("generic")),
            )
            .with_metadata(
                "dynamic_outputs",
                Value::String(std::borrow::Cow::Borrowed("generic")),
            ),
    );
    graph.nodes.push(
        NodeInstance::new("echo")
            .with_inputs(["in"])
            .with_outputs(["out"]),
    );
    graph.edges.push(Edge::new(0, "in", 1, "in"));
    graph.edges.push(Edge::new(1, "out", 0, "out"));
    ExecutionPlan::new(graph, vec![])
}

fn two_input_stream_echo_plan() -> ExecutionPlan {
    let mut graph = Graph::default();
    graph.nodes.push(
        NodeInstance::new(HOST_BRIDGE_ID)
            .with_label("host")
            .with_inputs(["out"])
            .with_outputs(["left", "right"])
            .with_metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
            .with_metadata(
                "dynamic_inputs",
                Value::String(std::borrow::Cow::Borrowed("generic")),
            )
            .with_metadata(
                "dynamic_outputs",
                Value::String(std::borrow::Cow::Borrowed("generic")),
            ),
    );
    graph.nodes.push(
        NodeInstance::new("echo")
            .with_inputs(["in"])
            .with_outputs(["out"]),
    );
    for port in ["left", "right"] {
        graph.edges.push(Edge::new(0, port, 1, "in"));
    }
    graph.edges.push(Edge::new(1, "out", 0, "out"));
    ExecutionPlan::new(graph, vec![])
}

fn recv_u32(output: &daedalus_runtime::GraphOutput) -> u32 {
    output
        .recv_timeout(SAFETY_TIMEOUT)
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
        let graph = graph.lock();
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

    graph.lock().start().expect("start");
    assert_eq!(
        graph.lock().diagnostics().worker_state,
        StreamWorkerState::Idle
    );
    let worker = StreamGraph::spawn_continuous(Arc::clone(&graph), Duration::from_millis(1));

    {
        // Holding the graph keeps the worker from draining the inputs before the check.
        let graph = graph.lock();
        for value in 0..32u32 {
            input
                .feed(Payload::owned("demo:u32", value))
                .expect("feed should succeed");
        }
        assert!(matches!(
            graph.diagnostics().worker_state,
            StreamWorkerState::Running | StreamWorkerState::BlockedInExecution
        ));
    }
    let first_batch: Vec<_> = (0..32).map(|_| recv_u32(&output)).collect();
    assert_eq!(first_batch, (0..32u32).collect::<Vec<_>>());

    {
        let mut graph = graph.lock();
        graph.pause().expect("pause");
        assert_eq!(graph.state(), StreamGraphState::Paused);
        assert_eq!(graph.diagnostics().worker_state, StreamWorkerState::Paused);
    }
    for value in 32..64u32 {
        input
            .feed(Payload::owned("demo:u32", value))
            .expect("feed while paused should succeed");
    }
    // Quiet period for a paused worker to misbehave. Pacing, not a bound: a correct worker cannot
    // fail this check, and a longer quiet period only gives a broken one more chance to show.
    std::thread::sleep(Duration::from_millis(20));
    assert!(
        output
            .try_recv()
            .expect("try_recv while paused should not fail")
            .is_none()
    );

    {
        let mut graph = graph.lock();
        graph.resume().expect("resume");
        assert_eq!(graph.state(), StreamGraphState::Running);
    }
    let second_batch: Vec<_> = (0..32).map(|_| recv_u32(&output)).collect();
    assert_eq!(second_batch, (32..64u32).collect::<Vec<_>>());

    graph.lock().close().expect("close");
    assert_eq!(
        graph.lock().diagnostics().worker_state,
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
    let GatedHandler {
        handler,
        started,
        finished,
        release,
    } = gated_handler();
    let graph: SharedStreamGraph<SlowHandler> =
        Arc::new(Mutex::new(StreamGraph::new(runtime, handler)));

    let input = {
        let graph = graph.lock();
        graph.input("in").expect("input handle")
    };

    graph.lock().start().expect("start");
    let worker = StreamGraph::spawn_continuous(Arc::clone(&graph), Duration::from_millis(1));
    input
        .feed(Payload::owned("demo:u32", 1u32))
        .expect("feed should succeed");
    started
        .recv_timeout(SAFETY_TIMEOUT)
        .expect("handler should start");
    // The handler stays parked on `release` until the test sends it, so it is in flight for
    // everything below.
    let diagnostics = graph.lock().diagnostics();
    assert_eq!(
        diagnostics.worker_state,
        StreamWorkerState::BlockedInExecution
    );
    assert!(diagnostics.current_execution_elapsed.is_some());

    let (paused_tx, paused_rx) = mpsc::channel();
    let pause_graph = Arc::clone(&graph);
    let pause_thread = std::thread::spawn(move || {
        pause_graph.lock().pause().expect("pause");
        paused_tx.send(()).expect("pause notification");
    });

    // If the worker held the graph lock across the handler, this would never be reached (and the
    // safety timeout would fail the test).
    paused_rx
        .recv_timeout(SAFETY_TIMEOUT)
        .expect("pause should acquire graph lock while handler is still running");
    pause_thread.join().expect("pause thread");
    release.send(()).expect("release handler");
    finished
        .recv_timeout(SAFETY_TIMEOUT)
        .expect("handler should eventually finish");

    graph.lock().close().expect("close");
    let deadline = Instant::now() + SAFETY_TIMEOUT;
    while graph.lock().diagnostics().last_execution_duration.is_none() {
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
    let GatedHandler {
        handler,
        started,
        finished,
        release,
    } = gated_handler();
    let graph: SharedStreamGraph<SlowHandler> =
        Arc::new(Mutex::new(StreamGraph::new(runtime, handler)));

    let input = {
        let graph = graph.lock();
        graph.input("in").expect("input handle")
    };

    graph.lock().start().expect("start");
    let mut worker = StreamGraph::spawn_continuous(Arc::clone(&graph), Duration::from_millis(1));
    input
        .feed(Payload::owned("demo:u32", 1u32))
        .expect("feed should succeed");
    started
        .recv_timeout(SAFETY_TIMEOUT)
        .expect("handler should start");

    // The handler is parked, so the worker cannot finish within any timeout: this is a certain
    // timeout, not a race against the handler's running time.
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

    release.send(()).expect("release handler");
    finished
        .recv_timeout(SAFETY_TIMEOUT)
        .expect("handler should eventually finish");
    assert_eq!(worker.stop_timeout(SAFETY_TIMEOUT), Ok(None));
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
    let GatedHandler {
        handler,
        started,
        finished,
        release,
    } = gated_handler();
    let graph: SharedStreamGraph<SlowHandler> =
        Arc::new(Mutex::new(StreamGraph::new(runtime, handler)));

    let input = {
        let graph = graph.lock();
        graph.input("in").expect("input handle")
    };

    graph.lock().start().expect("start");
    let worker = StreamGraph::spawn_continuous(Arc::clone(&graph), Duration::from_millis(1));
    input
        .feed(Payload::owned("demo:u32", 1u32))
        .expect("feed should succeed");
    started
        .recv_timeout(SAFETY_TIMEOUT)
        .expect("handler should start");

    // The handler is still parked, so the drop below returns with it in flight. If drop waited
    // for the handler, the safety timeout would release it and `finished` would already hold a
    // message here.
    drop(worker);
    assert!(
        matches!(finished.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "dropping a worker should not wait for an in-flight handler"
    );
    release.send(()).expect("release handler");
    finished
        .recv_timeout(SAFETY_TIMEOUT)
        .expect("handler should still finish after drop requests stop");
}
