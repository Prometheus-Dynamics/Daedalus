//! Node recovery policies, blocking backpressure and stream-worker error handling.

use daedalus_runtime::sync::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use daedalus_data::model::Value;
use daedalus_planner::{Edge, ExecutionPlan, Graph, NodeInstance};
use daedalus_runtime::executor::OwnedExecutor;
use daedalus_runtime::{
    BackpressureStrategy, ExecuteError, Executor, NODE_RECOVERY_META_KEY, NodeError, NodeHandler,
    NodeRecovery, RuntimeEdgePolicy, RuntimeNode, SchedulerConfig, build_runtime,
};
use daedalus_transport::Payload;

fn chain(ids: &[&str]) -> ExecutionPlan {
    let mut graph = Graph::default();
    for id in ids {
        graph.nodes.push(NodeInstance::new(*id));
    }
    for idx in 1..ids.len() {
        graph.edges.push(Edge::new(idx - 1, "out", idx, "in"));
    }
    ExecutionPlan::new(graph, vec![])
}

/// `src` pushes 1; `flaky` forwards its input +1 but fails its first `failures` calls (or
/// panics with `panic`); `sink` records what arrives.
struct Flaky {
    failures: u32,
    panic: bool,
    calls: AtomicU32,
    seen: Arc<Mutex<Vec<i32>>>,
}

impl Flaky {
    fn new(failures: u32) -> Self {
        Self {
            failures,
            panic: false,
            calls: AtomicU32::new(0),
            seen: Arc::default(),
        }
    }
}

impl NodeHandler for Flaky {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "src" => io.push_payload("out", Payload::owned("test:i32", 1_i32)),
            "flaky" => {
                let value = io
                    .get_typed::<i32>("in")
                    .ok_or_else(|| NodeError::InvalidInput("flaky input".into()))?;
                if self.calls.fetch_add(1, Ordering::Relaxed) < self.failures {
                    assert!(!self.panic, "flaky panicked");
                    return Err(NodeError::Handler("flaky failed".into()));
                }
                io.push_payload("out", Payload::owned("test:i32", value + 1));
            }
            "sink" => {
                if let Some(value) = io.get_typed::<i32>("in") {
                    self.seen.lock().push(value);
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[test]
fn fail_is_the_default_and_fails_the_run() {
    let rt = build_runtime(
        &chain(&["src", "flaky", "sink"]),
        &SchedulerConfig::default(),
    );
    let err = Executor::new(&rt, Flaky::new(1)).run().expect_err("fail");
    assert!(matches!(err, ExecuteError::HandlerFailed { ref node, .. } if node == "flaky"));
}

#[test]
fn skip_frame_drops_the_node_output_and_keeps_running() {
    let rt = build_runtime(
        &chain(&["src", "flaky", "sink"]),
        &SchedulerConfig::default(),
    );
    let handler = Flaky::new(1);
    let seen = handler.seen.clone();
    let telemetry = Executor::new(&rt, handler)
        .with_recovery(NodeRecovery::SkipFrame)
        .run()
        .expect("skip frame recovers");
    assert_eq!(telemetry.errors.len(), 1);
    assert_eq!(telemetry.errors[0].node_id, "flaky");
    assert_eq!(telemetry.recovery().skipped_frames, 1);
    assert!(seen.lock().is_empty(), "sink was not ready");
}

#[test]
fn retry_reruns_the_handler_on_the_same_inputs() {
    let rt = build_runtime(
        &chain(&["src", "flaky", "sink"]),
        &SchedulerConfig::default(),
    );
    let handler = Flaky::new(2);
    let seen = handler.seen.clone();
    let telemetry = Executor::new(&rt, handler)
        .with_node_recovery("flaky", NodeRecovery::retry(2))
        .run()
        .expect("second retry succeeds");
    assert_eq!(*seen.lock(), vec![2]);
    assert_eq!(telemetry.recovery().retries, 2);
    assert_eq!(telemetry.recovery().recovered_retries, 1);
    assert!(telemetry.errors.is_empty());
}

#[test]
fn exhausted_retries_fail_like_fail() {
    let rt = build_runtime(
        &chain(&["src", "flaky", "sink"]),
        &SchedulerConfig::default(),
    );
    let err = Executor::new(&rt, Flaky::new(5))
        .with_node_recovery("flaky", NodeRecovery::retry(2))
        .run()
        .expect_err("retries exhausted");
    assert!(matches!(err, ExecuteError::HandlerFailed { .. }));
}

#[test]
fn recovery_metadata_selects_a_node_policy() {
    let mut plan = chain(&["src", "flaky", "sink"]);
    plan.graph.nodes[1] = NodeInstance::new("flaky")
        .with_metadata(NODE_RECOVERY_META_KEY, Value::String("skip_frame".into()));
    let rt = build_runtime(&plan, &SchedulerConfig::default());
    let telemetry = Executor::new(&rt, Flaky::new(1))
        .run()
        .expect("metadata policy recovers");
    assert_eq!(telemetry.recovery().skipped_frames, 1);

    // A builder override wins over metadata.
    let err = Executor::new(&rt, Flaky::new(1))
        .with_node_recovery("flaky", NodeRecovery::Fail)
        .run()
        .expect_err("override fails");
    assert!(matches!(err, ExecuteError::HandlerFailed { .. }));
}

#[test]
fn unknown_node_override_is_reported() {
    let rt = build_runtime(&chain(&["src", "flaky"]), &SchedulerConfig::default());
    let mut exec = Executor::new(&rt, Flaky::new(0));
    let err = exec
        .try_set_node_recovery("nope", NodeRecovery::SkipFrame)
        .expect_err("unknown node");
    assert_eq!(err.node, "nope");
    assert!(exec.try_set_node_priority("flaky", 3).is_ok());
}

/// Catching a panic needs `std` (without it panics unwind as under `Fail`).
#[cfg(feature = "std")]
#[test]
fn recovering_policy_catches_panics() {
    let rt = build_runtime(
        &chain(&["src", "flaky", "sink"]),
        &SchedulerConfig::default(),
    );
    let handler = Flaky {
        panic: true,
        ..Flaky::new(1)
    };
    let telemetry = Executor::new(&rt, handler)
        .with_recovery(NodeRecovery::SkipFrame)
        .run()
        .expect("panic recovered");
    assert_eq!(telemetry.recovery().caught_panics, 1);
    assert!(telemetry.errors[0].message.contains("flaky panicked"));
}

#[cfg(feature = "threads")]
#[test]
fn parallel_runs_apply_node_recovery() {
    let mut graph = Graph::default();
    for id in ["good", "bad"] {
        graph.nodes.push(NodeInstance::new(id));
    }
    let rt = build_runtime(
        &ExecutionPlan::new(graph, vec![]),
        &SchedulerConfig::default(),
    );
    let handler = |node: &RuntimeNode,
                   _: &daedalus_runtime::state::ExecutionContext,
                   _: &mut daedalus_runtime::io::NodeIo| {
        if node.id == "bad" {
            return Err(NodeError::Handler("bad".into()));
        }
        Ok(())
    };
    let telemetry = Executor::new(&rt, handler)
        .with_pool_size(Some(2))
        .with_node_recovery("bad", NodeRecovery::SkipFrame)
        .with_node_priority("bad", 5)
        .run_parallel()
        .expect("recovered in parallel");
    assert_eq!(telemetry.recovery().skipped_frames, 1);
    assert_eq!(telemetry.nodes_executed, 2);
}

/// `counter` keeps a count in its node state, recording each value and failing on the second.
struct Counter {
    seen: Arc<Mutex<Vec<u32>>>,
}

impl NodeHandler for Counter {
    fn run(
        &self,
        _node: &RuntimeNode,
        ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        let count = ctx.state.take_node_state::<u32>(&ctx.node_id).unwrap_or(0) + 1;
        ctx.state.set_node_state(&ctx.node_id, count);
        self.seen.lock().push(count);
        if count == 2 {
            return Err(NodeError::Handler("second call".into()));
        }
        Ok(())
    }
}

#[test]
fn restart_resets_node_state() {
    let rt = Arc::new(build_runtime(
        &chain(&["counter"]),
        &SchedulerConfig::default(),
    ));
    for (policy, expected) in [
        (NodeRecovery::Restart, vec![1, 2, 1]),
        (NodeRecovery::SkipFrame, vec![1, 2, 3]),
    ] {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut exec =
            OwnedExecutor::new(rt.clone(), Counter { seen: seen.clone() }).with_recovery(policy);
        for _ in 0..3 {
            exec.run_in_place().expect("recovered");
        }
        assert_eq!(*seen.lock(), expected, "{policy:?}");
    }
}

/// `src` pushes the next value each call; `sink` records every value it pops.
struct Pump {
    produced: AtomicU32,
    seen: Arc<Mutex<Vec<u32>>>,
}

impl NodeHandler for Pump {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "src" => {
                let value = self.produced.fetch_add(1, Ordering::Relaxed);
                io.push_payload("out", Payload::owned("test:u32", value));
            }
            _ => {
                let values = io
                    .inputs_for("in")
                    .filter_map(|payload| payload.inner.get_ref::<u32>().copied());
                self.seen.lock().extend(values);
            }
        }
        Ok(())
    }
}

/// Run `src -> sink` over a bounded(2) drop-oldest queue for 6 ticks with `sink` inactive for
/// the first three; returns what `sink` saw and the deferrals.
fn pump(blocking: bool) -> (Vec<u32>, usize) {
    let config = SchedulerConfig {
        default_policy: RuntimeEdgePolicy::bounded(2),
        backpressure: BackpressureStrategy::None,
    };
    let rt = Arc::new(build_runtime(&chain(&["src", "sink"]), &config));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handler = Pump {
        produced: AtomicU32::new(0),
        seen: seen.clone(),
    };
    let mut exec = OwnedExecutor::new(rt, handler)
        .with_node_fusion(false)
        .with_blocking_backpressure(blocking);
    let mut deferrals = 0;
    for tick in 0..6 {
        let sink_active = tick >= 3;
        exec.set_active_nodes_mask(Some(Arc::new(vec![true, sink_active])));
        deferrals += exec.run_in_place().expect("tick").backpressure_deferrals;
    }
    let seen = seen.lock().clone();
    (seen, deferrals)
}

#[test]
fn blocking_backpressure_defers_the_producer_instead_of_dropping() {
    // Without blocking the full queue drops the oldest values while the sink is idle.
    let (dropping, deferrals) = pump(false);
    assert_eq!(deferrals, 0);
    assert_eq!(dropping, vec![2, 3, 4, 5]);

    // With it the producer waits at ticks 2 and 3 (queue full) and nothing is lost.
    let (blocking, deferrals) = pump(true);
    assert_eq!(deferrals, 2);
    assert_eq!(blocking, vec![0, 1, 2, 3]);
}

#[cfg(feature = "threads")]
mod stream_worker {
    use super::*;
    use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY};
    use daedalus_runtime::{SharedStreamGraph, StreamErrorPolicy, StreamGraph, StreamWorkerConfig};
    use std::time::Duration;

    const SAFETY_TIMEOUT: Duration = Duration::from_secs(30);

    fn echo_plan() -> ExecutionPlan {
        let mut graph = Graph::default();
        graph.nodes.push(
            NodeInstance::new(HOST_BRIDGE_ID)
                .with_label("host")
                .with_inputs(["out"])
                .with_outputs(["in"])
                .with_metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
                .with_metadata("dynamic_inputs", Value::String("generic".into()))
                .with_metadata("dynamic_outputs", Value::String("generic".into())),
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

    /// Echoes `u32`s, failing on 1 (or panicking on 1 with `panic`).
    struct FailOnOne {
        panic: bool,
    }

    impl NodeHandler for FailOnOne {
        fn run(
            &self,
            node: &RuntimeNode,
            _ctx: &daedalus_runtime::state::ExecutionContext,
            io: &mut daedalus_runtime::io::NodeIo,
        ) -> Result<(), NodeError> {
            if node.id != "echo" {
                return Ok(());
            }
            let value = io.get_typed::<u32>("in").unwrap_or_default();
            if value == 1 {
                assert!(!self.panic, "echo panicked");
                return Err(NodeError::Handler("echo failed".into()));
            }
            io.push_payload("out", Payload::owned("demo:u32", value));
            Ok(())
        }
    }

    fn run(handler: FailOnOne, on_error: StreamErrorPolicy) -> (Vec<u32>, u64, Option<String>) {
        let runtime = Arc::new(build_runtime(&echo_plan(), &SchedulerConfig::default()));
        let graph: SharedStreamGraph<FailOnOne> =
            Arc::new(Mutex::new(StreamGraph::new(runtime, handler)));
        let (input, output) = {
            let graph = graph.lock();
            (
                graph.input("in").expect("input"),
                graph.output("out").expect("output"),
            )
        };
        graph.lock().start().expect("start");
        let config = StreamWorkerConfig::default()
            .with_idle_sleep(Duration::from_millis(1))
            .with_error_policy(on_error);
        let mut worker = StreamGraph::spawn_continuous_with_config(Arc::clone(&graph), config);
        let mut received = Vec::new();
        for value in 0..3u32 {
            input.feed(Payload::owned("demo:u32", value)).expect("feed");
            if value == 1 {
                // The failing tick produces nothing; wait for the worker to record it.
                let started = std::time::Instant::now();
                while worker.diagnostics().tick_errors == 0 {
                    assert!(
                        started.elapsed() < SAFETY_TIMEOUT,
                        "tick error not recorded"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                if on_error == StreamErrorPolicy::Stop {
                    break;
                }
                continue;
            }
            let payload = output
                .recv_timeout(SAFETY_TIMEOUT)
                .expect("recv")
                .expect("payload before timeout");
            received.push(*payload.get_ref::<u32>().expect("u32"));
        }
        let diagnostics = worker.diagnostics();
        let last_error = worker.stop_timeout(SAFETY_TIMEOUT).expect("stop");
        (received, diagnostics.tick_errors, last_error)
    }

    #[test]
    fn continue_keeps_the_worker_running_after_a_failed_tick() {
        for panic in [false, true] {
            let (received, tick_errors, last_error) =
                run(FailOnOne { panic }, StreamErrorPolicy::Continue);
            assert_eq!(received, vec![0, 2], "panic {panic}");
            assert_eq!(tick_errors, 1);
            assert!(last_error.is_some());
        }
    }

    #[test]
    fn stop_ends_the_worker_on_a_failed_tick() {
        let (received, tick_errors, last_error) =
            run(FailOnOne { panic: false }, StreamErrorPolicy::Stop);
        assert_eq!(received, vec![0]);
        assert_eq!(tick_errors, 1);
        assert!(last_error.expect("error").contains("echo failed"));
    }
}

/// Like [`Flaky`], but `flaky` takes its input by value (`take_owned_or_clone`), as a consuming
/// handler does.
struct OwnedFlaky {
    failures: u32,
    calls: AtomicU32,
    seen: Arc<Mutex<Vec<i32>>>,
}

impl NodeHandler for OwnedFlaky {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "src" => io.push_payload("out", Payload::owned("test:i32", 1_i32)),
            "flaky" => {
                let value = io
                    .take_owned_or_clone::<i32>("in")
                    .ok_or_else(|| NodeError::InvalidInput("flaky missing value".into()))?;
                if self.calls.fetch_add(1, Ordering::Relaxed) < self.failures {
                    return Err(NodeError::Handler("flaky failed".into()));
                }
                io.push_payload("out", Payload::owned("test:i32", value + 1));
            }
            "sink" => {
                if let Some(value) = io.get_typed::<i32>("in") {
                    self.seen.lock().push(value);
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[test]
fn retry_gives_a_by_value_consumer_its_input_again() {
    let rt = build_runtime(
        &chain(&["src", "flaky", "sink"]),
        &SchedulerConfig::default(),
    );
    let handler = OwnedFlaky {
        failures: 1,
        calls: AtomicU32::new(0),
        seen: Arc::default(),
    };
    let seen = handler.seen.clone();
    let telemetry = Executor::new(&rt, handler)
        .with_node_recovery("flaky", NodeRecovery::retry(1))
        .run()
        .expect("the retry succeeds");
    assert_eq!(*seen.lock(), vec![2]);
    assert_eq!(telemetry.recovery().recovered_retries, 1);
}
