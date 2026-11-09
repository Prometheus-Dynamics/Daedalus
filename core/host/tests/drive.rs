//! The drive loops and the waits, on a one-step graph.

use std::os::fd::AsFd;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_host::stream::{StreamFeed, StreamGraph};
use daedalus_host::{Dir, Host, InboundWait, PortSpec, Ports};
use daedalus_plan::Span;
use daedalus_runner::event_fd::poll_readable;
use daedalus_runner::{Memory, Plan, Recovery, Runner, SlotKind, SlotSpec, StepMeta};
use daedalus_types::{SpaceIndex, TypeIndex, TypeKey};
use daedalus_value::{Owner, Value};

const TY: TypeIndex = TypeIndex(0);
const SP: SpaceIndex = SpaceIndex(0);
static KEY: TypeKey = TypeKey::new("demo.a");
static TYPES: [TypeKey; 1] = [TypeKey::new("demo.a")];

struct Echo(Arc<AtomicU32>);

impl OwnedStep for Echo {
    fn run(&mut self, slots: &mut Slots, _: &mut StepCx) -> StepEnd {
        self.0.fetch_add(1, Ordering::Relaxed);
        let value = *slots.cell(0).unwrap();
        *slots.cell(1).unwrap() = value;
        slots.cell(0).unwrap().state = daedalus_abi::SlotState::Moved as u32;
        StepEnd::Done
    }
}

fn leak<T>(values: Vec<T>) -> &'static mut [T] {
    Box::leak(values.into_boxed_slice())
}

fn graph(calls: &Arc<AtomicU32>) -> StreamGraph<Host<'static>> {
    let specs: &'static [SlotSpec] = leak(
        [4, 64]
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
    let table: &'static [u32] = leak(vec![0, 1]);
    let memory = Memory {
        cells: leak(vec![daedalus_abi::Slot::EMPTY; 2]),
        queues: leak(vec![daedalus_runner::Queue::default(); 2]),
        storage: leak(vec![daedalus_abi::Slot::EMPTY; 68]),
        dirty: leak(vec![0; 1]),
        carry: leak(vec![0; 1]),
        always: leak(vec![0; 1]),
        pending: leak(Vec::new()),
        health: leak(vec![daedalus_runner::StepHealth::default(); 1]),
        activity: leak(vec![0; 1]),
        inline: leak(vec![[0u8; 16]; 68]),
    };
    let runner = Runner::load(
        Plan {
            steps: leak(vec![Step::boxed(Echo(Arc::clone(calls)))]),
            meta,
            table,
            slots: specs,
        },
        memory,
    )
    .unwrap();
    const WAKE: [u32; 1] = [0];
    let ports: &'static [PortSpec<'static>] = leak(vec![
        PortSpec {
            name: "in",
            dir: Dir::Input,
            slot: 0,
            ty: TY,
            space: SP,
            key: &KEY,
            wake: &WAKE,
        },
        PortSpec {
            name: "out",
            dir: Dir::Output,
            slot: 1,
            ty: TY,
            space: SP,
            key: &KEY,
            wake: &[],
        },
    ]);
    let host = Host::new(runner, Ports::new(ports).unwrap(), &TYPES);
    StreamGraph::new(host, &["in"], &["out"])
}

fn value() -> Value {
    Value {
        ty: TY,
        space: SP,
        owner: Owner::Static(b"v"),
    }
}

#[test]
fn tick_on_input_waits_for_a_feed() {
    let calls = Arc::new(AtomicU32::new(0));
    let mut graph = graph(&calls);
    let input = graph.input("in").unwrap();
    assert_eq!(
        graph.wait_for_input(Some(Duration::from_millis(10))),
        InboundWait::TimedOut
    );
    let feeder = std::thread::spawn(move || {
        input.feed(value());
    });
    assert_eq!(graph.wait_for_input(None), InboundWait::Ready);
    feeder.join().unwrap();
    assert!(graph.tick_ready().unwrap().is_some());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(graph.tick_ready().unwrap().is_none());
}

#[test]
fn drive_blocking_processes_input_until_stopped() {
    let calls = Arc::new(AtomicU32::new(0));
    let mut graph = graph(&calls);
    let input = graph.input("in").unwrap();
    let stop = graph.stop_handle();
    let stopper = stop.clone();
    let seen = Arc::clone(&calls);
    let feeder = std::thread::spawn(move || {
        for _ in 0..5 {
            assert_eq!(input.feed(value()), StreamFeed::Accepted);
        }
        while seen.load(Ordering::Relaxed) < 5 {
            std::thread::yield_now();
        }
        stopper.stop();
    });
    graph.drive_blocking(&stop, |_| {}).unwrap();
    feeder.join().unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 5);
    // A reset handle can be used again.
    stop.reset();
    assert!(!stop.is_stopped());
}

#[test]
fn drive_blocking_exits_when_the_inputs_close() {
    let calls = Arc::new(AtomicU32::new(0));
    let mut graph = graph(&calls);
    let input = graph.input("in").unwrap();
    input.feed(value());
    let signal = graph.inbound_signal();
    let stop = graph.stop_handle();
    graph.tick_ready().unwrap();
    signal.close();
    graph.drive_blocking(&stop, |_| {}).unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn tick_until_idle_drains_everything_queued() {
    let calls = Arc::new(AtomicU32::new(0));
    let mut graph = graph(&calls);
    let input = graph.input("in").unwrap();
    for _ in 0..3 {
        input.feed(value());
    }
    graph.tick_until_idle().unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 3);
}

#[test]
fn one_poll_drives_a_host_fd_and_the_inbound_fd() {
    let calls = Arc::new(AtomicU32::new(0));
    let mut graph = graph(&calls);
    let input = graph.input("in").unwrap();
    let inbound = graph.inbound_fd().unwrap();
    let (source_tx, source_rx) = UnixStream::pair().unwrap();
    source_rx.set_nonblocking(true).unwrap();
    use std::io::{Read, Write};

    let mut source_tx = source_tx;
    source_tx.write_all(&[1]).unwrap();
    input.feed(value());

    let mut seen_host_fd = false;
    let mut seen_graph = false;
    for _ in 0..4 {
        let ready =
            poll_readable(&[source_rx.as_fd(), inbound.as_fd()], 5_000).expect("poll works");
        assert!(ready[0] || ready[1], "poll timed out");
        if ready[0] {
            let mut buf = [0u8; 1];
            (&source_rx).read_exact(&mut buf).unwrap();
            seen_host_fd = true;
        }
        if ready[1] {
            inbound.clear();
            graph.tick_ready().unwrap();
            inbound.rearm();
            seen_graph = true;
        }
        if seen_host_fd && seen_graph {
            break;
        }
    }
    assert!(seen_host_fd && seen_graph);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[test]
fn the_waiter_future_wakes_on_input() {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    let calls = Arc::new(AtomicU32::new(0));
    let graph = graph(&calls);
    let input = graph.input("in").unwrap();
    let waiter = graph.inbound_signal().waiter();
    let mut waiter = pin!(waiter);
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(waiter.as_mut().poll(&mut cx), Poll::Pending);
    input.feed(value());
    assert_eq!(
        waiter.as_mut().poll(&mut cx),
        Poll::Ready(InboundWait::Ready)
    );
}
