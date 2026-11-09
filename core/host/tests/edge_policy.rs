//! Pressure and freshness rules, on the slot ring and through a host port.

use core::time::Duration;

use daedalus_abi::{Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_host::{Dir, Host, HostError, PortPolicy, PortSpec, Ports};
use daedalus_plan::Span;
use daedalus_runner::{Memory, Plan, Recovery, Runner, SlotKind, SlotSpec, StepMeta};
use daedalus_types::{SpaceIndex, TypeIndex, TypeKey};
use daedalus_value::{DropReason, Freshness, Keep, Overflow, Owner, Pressure, Push, Ring, Value};

const TY: TypeIndex = TypeIndex(0);
const SP: SpaceIndex = SpaceIndex(0);

fn value(bytes: &'static [u8]) -> Value {
    Value {
        ty: TY,
        space: SP,
        owner: Owner::Static(bytes),
    }
}

fn drain(ring: &mut Ring<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(next) = ring.pop() {
        out.push(next.bytes().unwrap()[0]);
    }
    out
}

fn bounded(cap: u32, overflow: Overflow) -> Pressure {
    Pressure::Bounded { cap, overflow }
}

fn cells(count: usize) -> Vec<Option<Value>> {
    (0..count).map(|_| None).collect()
}

/// Pushes each single-byte value in order through a policy at clock zero.
fn apply(policy: Pressure, values: &[&'static [u8]]) -> Vec<u8> {
    let mut storage = cells(8);
    let mut ring = Ring::new(&mut storage);
    let mut opened = 0;
    for bytes in values {
        policy.push(&mut ring, value(bytes), &mut opened, 0);
    }
    drain(&mut ring)
}

#[test]
fn bounded_drop_oldest_keeps_the_newest() {
    let got = apply(bounded(2, Overflow::DropOldest), &[b"1", b"2", b"3", b"4"]);
    assert_eq!(got, b"34");
}

#[test]
fn bounded_drop_newest_keeps_the_oldest() {
    let got = apply(bounded(2, Overflow::DropNewest), &[b"1", b"2", b"3", b"4"]);
    assert_eq!(got, b"12");
}

#[test]
fn a_full_bounded_queue_does_not_enqueue_on_error() {
    let mut storage = cells(4);
    let mut ring = Ring::new(&mut storage);
    let mut opened = 0;
    let policy = bounded(1, Overflow::ErrorOnFull);
    assert!(matches!(
        policy.push(&mut ring, value(b"a"), &mut opened, 0).push,
        Push::Stored
    ));
    let pushed = policy.push(&mut ring, value(b"b"), &mut opened, 0);
    assert!(matches!(pushed.push, Push::Full(_)));
    assert_eq!(pushed.reason, None);
    assert_eq!(ring.len(), 1);
}

#[test]
fn latest_only_keeps_the_newest_and_says_why() {
    let mut storage = cells(4);
    let mut ring = Ring::new(&mut storage);
    let mut opened = 0;
    let policy = Pressure::LatestOnly;
    policy.push(&mut ring, value(b"a"), &mut opened, 0);
    let pushed = policy.push(&mut ring, value(b"b"), &mut opened, 0);
    assert!(matches!(pushed.push, Push::Dropped(_)));
    assert_eq!(pushed.reason, Some(DropReason::LatestOnlyReplace));
    assert_eq!(drain(&mut ring), b"b");
}

#[test]
fn clear_keeps_the_bounded_capacity() {
    let policy = bounded(2, Overflow::DropOldest);
    assert_eq!(policy.capacity(8), 2);
    let mut storage = cells(8);
    let mut ring = Ring::new(&mut storage);
    let mut opened = 0;
    for bytes in [b"1", b"2", b"3"] {
        policy.push(&mut ring, value(bytes), &mut opened, 0);
    }
    drain(&mut ring);
    assert!(ring.is_empty());
    assert_eq!(ring.capacity(), 8);
    assert_eq!(policy.capacity(8), 2);
    policy.push(&mut ring, value(b"9"), &mut opened, 0);
    assert_eq!(drain(&mut ring), b"9");
}

#[test]
fn coalesce_keep_oldest_keeps_the_value_that_opened_the_window() {
    let policy = bounded(
        1,
        Overflow::Coalesce {
            keep: Keep::Oldest,
            window_ns: 100,
        },
    );
    let mut storage = cells(4);
    let mut ring = Ring::new(&mut storage);
    let mut opened = 0;
    policy.push(&mut ring, value(b"a"), &mut opened, 10);
    let inside = policy.push(&mut ring, value(b"b"), &mut opened, 50);
    assert_eq!(inside.reason, Some(DropReason::Coalesced));
    assert_eq!(ring.peek().unwrap().bytes().unwrap(), b"a");
    // Past the window, the next arrival opens a new one.
    policy.push(&mut ring, value(b"c"), &mut opened, 200);
    assert_eq!(drain(&mut ring), b"c");
}

#[test]
fn coalesce_keep_newest_keeps_the_newest() {
    let policy = bounded(
        1,
        Overflow::Coalesce {
            keep: Keep::Newest,
            window_ns: 100,
        },
    );
    let got = apply(policy, &[b"a", b"b", b"c"]);
    assert_eq!(got, b"c");
}

#[test]
fn fifo_drains_in_arrival_order() {
    let got = apply(Pressure::BufferAll, &[b"1", b"2", b"3"]);
    assert_eq!(got, b"123");
}

#[test]
fn metadata_strings_keep_their_names() {
    assert_eq!(
        Pressure::from_meta("fifo", None, None),
        Some(Pressure::BufferAll)
    );
    assert_eq!(
        Pressure::from_meta("latest_only", None, None),
        Some(Pressure::LatestOnly)
    );
    assert_eq!(
        Pressure::from_meta("bounded", Some(3), Some("drop_oldest")),
        Some(bounded(3, Overflow::DropOldest))
    );
    assert_eq!(Pressure::from_meta("nonsense", None, None), None);
    assert_eq!(
        Freshness::from_meta("max_age", Some(5)),
        Some(Freshness::MaxAge(Duration::from_nanos(5)))
    );
}

/// Does nothing: the tests look at the input queue.
struct Sink;

impl StepWord for Sink {
    fn run(_: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Done
    }
}

static KEY: TypeKey = TypeKey::new("demo.a");
static TYPES: [TypeKey; 1] = [TypeKey::new("demo.a")];
static PORTS: [PortSpec<'static>; 1] = [PortSpec {
    name: "in",
    dir: Dir::Input,
    slot: 0,
    ty: TY,
    space: SP,
    key: &KEY,
    wake: &[],
}];

/// A host with one input port and no steps that consume it, so its queue can be inspected.
fn host() -> Host<'static> {
    let specs: &'static [SlotSpec] = Box::leak(Box::new([SlotSpec {
        cap: 4,
        kind: SlotKind::Fifo,
    }]));
    let span = Span { start: 0, len: 0 };
    let meta: &'static [StepMeta] = Box::leak(Box::new([StepMeta {
        wake: span,
        inputs: span,
        outputs: span,
        always: false,
        may_pend: false,
        recovery: Recovery::Fail,
        fire: daedalus_runner::Fire::Any,
        required: span,
        reuse: false,
    }]));
    let steps: &'static mut [Step] = Box::leak(vec![Step::of::<Sink>(0)].into_boxed_slice());
    let memory = Memory {
        cells: Box::leak(vec![daedalus_abi::Slot::EMPTY; 1].into_boxed_slice()),
        queues: Box::leak(vec![daedalus_runner::Queue::default(); 1].into_boxed_slice()),
        storage: Box::leak(vec![daedalus_abi::Slot::EMPTY; 4].into_boxed_slice()),
        dirty: Box::leak(vec![0; 1].into_boxed_slice()),
        carry: Box::leak(vec![0; 1].into_boxed_slice()),
        always: Box::leak(vec![0; 1].into_boxed_slice()),
        pending: Box::leak(Vec::new().into_boxed_slice()),
        health: Box::leak(vec![daedalus_runner::StepHealth::default(); 1].into_boxed_slice()),
        activity: Box::leak(vec![0; 1].into_boxed_slice()),
        inline: Box::leak(vec![[0u8; 16]; 4].into_boxed_slice()),
    };
    let runner = Runner::load(
        Plan {
            steps,
            meta,
            table: &[],
            slots: specs,
        },
        memory,
    )
    .unwrap();
    Host::new(runner, Ports::new(&PORTS).unwrap(), &TYPES)
}

fn queued(host: &mut Host<'static>) -> u32 {
    host.runner().queue(0).unwrap().len
}

#[test]
fn a_host_port_uses_the_same_pressure_rules() {
    let mut host = host();
    host.set_port_policy(
        "in",
        PortPolicy {
            pressure: bounded(2, Overflow::DropOldest),
            freshness: Freshness::PreserveAll,
        },
    )
    .unwrap();
    assert_eq!(host.feed("in", value(b"1")).unwrap().dropped, None);
    assert_eq!(host.feed("in", value(b"2")).unwrap().dropped, None);
    assert_eq!(
        host.feed("in", value(b"3")).unwrap().dropped,
        Some(DropReason::DropOldest)
    );
    assert_eq!(queued(&mut host), 2);
    assert_eq!(host.port_drops("in").unwrap().drop_oldest, 1);
}

#[test]
fn an_error_on_full_port_reports_full() {
    let mut host = host();
    let policy = PortPolicy {
        pressure: bounded(1, Overflow::ErrorOnFull),
        freshness: Freshness::PreserveAll,
    };
    host.set_port_policy("in", policy).unwrap();
    host.feed("in", value(b"1")).unwrap();
    assert!(matches!(
        host.feed("in", value(b"2")),
        Err(HostError::Full { .. })
    ));
}

#[test]
fn max_age_drops_old_values_on_an_injected_clock() {
    let mut host = host();
    let policy = PortPolicy {
        pressure: Pressure::BufferAll,
        freshness: Freshness::MaxAge(Duration::from_nanos(100)),
    };
    host.set_port_policy("in", policy).unwrap();
    host.set_clock(0);
    host.feed("in", value(b"old")).unwrap();
    host.set_clock(90);
    host.feed("in", value(b"new")).unwrap();
    host.set_clock(150);
    host.step().unwrap();
    assert_eq!(queued(&mut host), 1);
    assert_eq!(host.port_drops("in").unwrap().max_age, 1);
}

#[test]
fn max_lag_drops_values_behind_the_newest() {
    let mut host = host();
    let policy = PortPolicy {
        pressure: Pressure::BufferAll,
        freshness: Freshness::MaxLag(Duration::from_nanos(10)),
    };
    host.set_port_policy("in", policy).unwrap();
    host.set_clock(0);
    host.feed("in", value(b"a")).unwrap();
    host.set_clock(100);
    host.feed("in", value(b"b")).unwrap();
    host.step().unwrap();
    assert_eq!(queued(&mut host), 1);
    assert_eq!(host.port_drops("in").unwrap().max_lag, 1);
}

#[test]
fn latest_by_sequence_drops_out_of_order_arrivals() {
    let mut host = host();
    let policy = PortPolicy {
        pressure: Pressure::BufferAll,
        freshness: Freshness::LatestBySequence,
    };
    host.set_port_policy("in", policy).unwrap();
    assert_eq!(
        host.feed_marked("in", value(b"a"), 5).unwrap().dropped,
        None
    );
    assert_eq!(
        host.feed_marked("in", value(b"b"), 3).unwrap().dropped,
        Some(DropReason::OutOfOrder)
    );
    assert_eq!(queued(&mut host), 1);
}
