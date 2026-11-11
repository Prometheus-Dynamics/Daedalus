//! StreamGraph: a continuous worker, its lifecycle and diagnostics, on a plan of plain-function
//! steps.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_host::stream::{
    StreamErrorPolicy, StreamExecutionMode, StreamFeed, StreamGraph, StreamGraphState,
    StreamWorkerConfig, StreamWorkerStopError,
};
use daedalus_host::{Dir, Host, PortSpec, Ports};
use daedalus_plan::Span;
use daedalus_runner::{Memory, Plan, Recovery, Runner, SlotKind, SlotSpec, StepMeta};
use daedalus_types::{SpaceIndex, TypeIndex, TypeKey};
use daedalus_value::{Owner, Value};

const TY: TypeIndex = TypeIndex(0);
const SP: SpaceIndex = SpaceIndex(0);

static KEY: TypeKey = TypeKey::new("demo.a");
static TYPES: [TypeKey; 1] = [TypeKey::new("demo.a")];

/// Passes `in` to `out` and counts its calls. While a gate is set it waits on that gate first.
struct Echo {
    calls: Arc<AtomicU32>,
    gate: Option<Arc<Barrier>>,
}

impl OwnedStep for Echo {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        if let Some(gate) = &self.gate {
            gate.wait();
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
        let value = *slots.cell(0).unwrap();
        *slots.cell(1).unwrap() = value;
        slots.cell(0).unwrap().state = daedalus_abi::SlotState::Moved as u32;
        StepEnd::Done
    }
}

fn leak<T>(values: Vec<T>) -> &'static mut [T] {
    Box::leak(values.into_boxed_slice())
}

/// `in` and `other` feed slots 0 and 1; `out` reads slot 2. Only `in` wakes the step.
fn host(calls: &Arc<AtomicU32>, gate: Option<Arc<Barrier>>) -> Host<'static> {
    let specs: &'static [SlotSpec] = leak(
        [8, 8, 64]
            .map(|cap| SlotSpec {
                cap,
                kind: SlotKind::Fifo,
            })
            .to_vec(),
    );
    let span = |start, len| Span { start, len };
    let meta: &'static [StepMeta] = leak(vec![StepMeta {
        wake: span(0, 0),
        inputs: span(0, 1),
        outputs: span(1, 1),
        always: false,
        may_pend: false,
        recovery: Recovery::Fail,
        fire: daedalus_runner::Fire::Any,
        required: span(0, 0),
        reuse: false,
    }]);
    let table: &'static [u32] = leak(vec![0, 2]);
    let echo = Echo {
        calls: Arc::clone(calls),
        gate,
    };
    let steps = leak(vec![Step::boxed(echo)]);
    let memory = Memory {
        cells: leak(vec![daedalus_abi::Slot::EMPTY; 3]),
        queues: leak(vec![daedalus_runner::Queue::default(); 3]),
        storage: leak(vec![daedalus_abi::Slot::EMPTY; 80]),
        dirty: leak(vec![0; 1]),
        carry: leak(vec![0; 1]),
        always: leak(vec![0; 1]),
        pending: leak(Vec::new()),
        health: leak(vec![daedalus_runner::StepHealth::default(); 1]),
        activity: leak(vec![0; 1]),
        inline: leak(vec![[0u8; 16]; 80]),
    };
    let runner = Runner::load(
        Plan {
            steps,
            meta,
            table,
            slots: specs,
        },
        memory,
    )
    .unwrap();
    const WAKE_ZERO: [u32; 1] = [0];
    let ports: &'static [PortSpec<'static>] = leak(vec![
        PortSpec {
            name: "in",
            dir: Dir::Input,
            slot: 0,
            ty: TY,
            space: SP,
            key: &KEY,
            wake: &WAKE_ZERO,
        },
        PortSpec {
            name: "other",
            dir: Dir::Input,
            slot: 1,
            ty: TY,
            space: SP,
            key: &KEY,
            wake: &[],
        },
        PortSpec {
            name: "out",
            dir: Dir::Output,
            slot: 2,
            ty: TY,
            space: SP,
            key: &KEY,
            wake: &[],
        },
    ]);
    Host::new(runner, Ports::new(ports).unwrap(), &TYPES)
}

fn value(bytes: &'static [u8]) -> Value {
    Value {
        ty: TY,
        space: SP,
        owner: Owner::Static(bytes),
    }
}

fn graph(calls: &Arc<AtomicU32>, gate: Option<Arc<Barrier>>) -> StreamGraph<Host<'static>> {
    StreamGraph::new(host(calls, gate), &["in", "other"], &["out"])
}

fn spawn(graph: &mut StreamGraph<Host<'static>>) {
    graph.spawn_continuous(StreamWorkerConfig::default().with_idle_sleep(Duration::from_millis(1)));
}

#[test]
fn close_is_scoped_to_one_input_port() {
    let calls = Arc::new(AtomicU32::new(0));
    let graph = graph(&calls, None);
    let input = graph.input("in").unwrap();
    let other = graph.input("other").unwrap();
    input.close();
    assert_eq!(input.feed(value(b"a")), StreamFeed::Closed);
    assert_eq!(other.feed(value(b"b")), StreamFeed::Accepted);
    assert!(other.stats().accepted == 1);
    assert!(input.stats().closed);
}

#[test]
fn an_inactive_input_stays_queued() {
    let calls = Arc::new(AtomicU32::new(0));
    let mut graph = graph(&calls, None);
    let other = graph.input("other").unwrap();
    for _ in 0..3 {
        other.feed(value(b"held"));
    }
    graph.poll().unwrap().unwrap();
    // Nothing consumes `other`, so the values wait in the graph, and the step never ran.
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    let stats = other.stats();
    assert_eq!((stats.accepted, stats.dropped), (3, 0));
}

#[test]
fn diagnostics_report_serial_execution_and_the_applied_config() {
    let calls = Arc::new(AtomicU32::new(0));
    let mut graph = graph(&calls, None);
    let config = StreamWorkerConfig {
        on_error: StreamErrorPolicy::Stop,
        ..StreamWorkerConfig::default().with_idle_sleep(Duration::from_millis(3))
    };
    graph.spawn_continuous(config);
    let report = graph.diagnostics();
    assert_eq!(report.execution, StreamExecutionMode::Serial);
    assert_eq!(report.config, config);
    assert_eq!(report.inputs.len(), 2);
    assert_eq!(report.outputs.len(), 1);
    assert_eq!(report.outputs[0].name, "out");
    graph.stop();
}

#[test]
fn zero_idle_sleep_normalizes_to_the_default() {
    let config = StreamWorkerConfig::default().with_idle_sleep(Duration::ZERO);
    assert_eq!(config.idle_sleep, StreamWorkerConfig::default().idle_sleep);
}

#[test]
fn a_worker_pauses_resumes_and_shuts_down_under_pressure() {
    let calls = Arc::new(AtomicU32::new(0));
    let mut graph = graph(&calls, None);
    let input = graph.input("in").unwrap();
    let output = graph.output("out").unwrap();
    spawn(&mut graph);
    let values: [&'static [u8]; 2] = [b"even", b"odd!"];
    for round in 0..32usize {
        assert_eq!(input.feed(value(values[round % 2])), StreamFeed::Accepted);
        output
            .recv_timeout(Duration::from_secs(5))
            .expect("the first batch");
    }
    graph.pause();
    assert_eq!(graph.state(), StreamGraphState::Paused);
    for round in 0..16usize {
        assert_eq!(input.feed(value(values[round % 2])), StreamFeed::Accepted);
    }
    std::thread::sleep(Duration::from_millis(30));
    let before = calls.load(Ordering::Relaxed);
    assert!(output.try_recv().is_none() || before <= 33);
    graph.resume();
    for _ in 0..16 {
        output
            .recv_timeout(Duration::from_secs(5))
            .expect("the second batch");
    }
    let stopped = graph.stop().unwrap();
    assert!(stopped.last_error.is_none());
    assert!(stopped.host.is_some());
}

#[test]
fn the_worker_holds_no_lock_while_a_body_runs() {
    let calls = Arc::new(AtomicU32::new(0));
    let gate = Arc::new(Barrier::new(2));
    let mut graph = graph(&calls, Some(Arc::clone(&gate)));
    let input = graph.input("in").unwrap();
    spawn(&mut graph);
    assert_eq!(input.feed(value(b"first")), StreamFeed::Accepted);
    // The body is parked on the gate. A feed and a stats read must still return.
    while calls.load(Ordering::Relaxed) == 0 {
        assert_eq!(input.feed(value(b"later")), StreamFeed::Accepted);
        gate.wait();
    }
    assert!(input.stats().accepted >= 2);
    let _ = graph.stop_timeout(Duration::from_secs(5));
}

#[test]
fn a_stop_timeout_reports_a_slow_body_without_deadlocking() {
    let calls = Arc::new(AtomicU32::new(0));
    let gate = Arc::new(Barrier::new(2));
    let mut graph = graph(&calls, Some(Arc::clone(&gate)));
    let input = graph.input("in").unwrap();
    spawn(&mut graph);
    input.feed(value(b"parks the body"));
    std::thread::sleep(Duration::from_millis(20));
    let result = graph.stop_timeout(Duration::from_millis(20)).unwrap();
    assert!(matches!(
        result,
        Err(StreamWorkerStopError::SlowBody { .. })
    ));
    gate.wait();
}

#[test]
fn dropping_the_graph_requests_a_stop_without_waiting_for_a_slow_body() {
    let calls = Arc::new(AtomicU32::new(0));
    let gate = Arc::new(Barrier::new(2));
    let mut graph = graph(&calls, Some(Arc::clone(&gate)));
    let input = graph.input("in").unwrap();
    spawn(&mut graph);
    input.feed(value(b"parks the body"));
    std::thread::sleep(Duration::from_millis(20));
    drop(graph);
    // The drop returned while the body is still parked; release it so the worker can finish.
    gate.wait();
}

#[test]
fn a_subscriber_gets_a_handle_to_every_value() {
    let calls = Arc::new(AtomicU32::new(0));
    let mut graph = graph(&calls, None);
    let input = graph.input("in").unwrap();
    let output = graph.output("out").unwrap();
    let subscription = output.subscribe();
    input.feed(value(b"seen"));
    graph.poll().unwrap().unwrap();
    assert_eq!(subscription.try_recv().unwrap().bytes().unwrap(), b"seen");
    assert_eq!(output.try_recv().unwrap().bytes().unwrap(), b"seen");
}
