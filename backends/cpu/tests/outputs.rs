//! How a CPU node's output is kept between ticks: a value read by two consumers is refilled once
//! both let go, a value taken by its one consumer is moved and never cloned, a value a consumer
//! holds across ticks is never overwritten, and a panic in a body leaves nothing live.
//!
//! The live count is the process's, so the tests take turns.

#![cfg(feature = "merge")]

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_cpu::CpuBackend;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_macros::node;
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::{NodeRecord, Registry};
use daedalus_types::{TypeExpr, TypeKey, Typed};

#[global_allocator]
static COUNTING: CountingAlloc = CountingAlloc;

static TURN: Mutex<()> = Mutex::new(());

fn turn() -> MutexGuard<'static, ()> {
    TURN.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How many `Big`s are alive. Each one counts itself in and out.
static LIVE: AtomicI64 = AtomicI64::new(0);

/// A value that counts itself, and that must never be cloned: a graph that copies it panics.
struct Big {
    x: i64,
    _bytes: Vec<u8>,
}

impl Big {
    fn new(x: i64) -> Big {
        LIVE.fetch_add(1, Ordering::SeqCst);
        // The bytes are not made here: a body that made them would allocate each tick, which is
        // the body's own cost and not the output's.
        Big {
            x,
            _bytes: Vec::new(),
        }
    }
}

impl Drop for Big {
    fn drop(&mut self) {
        LIVE.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Clone for Big {
    fn clone(&self) -> Big {
        panic!("a Big was cloned");
    }
}

impl Typed for Big {
    const KEY: TypeKey = TypeKey::new("outputs:big");
    const EXPR: TypeExpr = TypeExpr::Opaque(Self::KEY);
}

#[node(id = "k.make", inputs(x), outputs(value))]
fn make(x: i64) -> Big {
    Big::new(x)
}

#[node(id = "k.sum_a", inputs(value), outputs(sum))]
fn sum_a(value: &Big) -> i64 {
    value.x * 10
}

#[node(id = "k.sum_b", inputs(value), outputs(sum))]
fn sum_b(value: &Big) -> i64 {
    value.x + 1
}

#[node(id = "k.add", inputs(a, b), outputs(total))]
fn add(a: i64, b: i64) -> i64 {
    a + b
}

#[node(id = "k.pass", inputs(value), outputs(value))]
fn pass(value: Big) -> Big {
    value
}

#[node(id = "k.take", inputs(value), outputs(x))]
fn take(value: Big) -> i64 {
    value.x
}

#[node(id = "k.boom", inputs(value), outputs(value))]
fn boom(value: Big) -> Big {
    if value.x == 3 {
        panic!("the body fails on 3");
    }
    value
}

/// Keeps the first value it is given, through a handle, for as long as it runs.
#[derive(Default)]
struct Held(Option<Arc<Big>>);

#[node(id = "k.hold", inputs(value), outputs(x), state(Held))]
fn hold(value: &Arc<Big>, held: &mut Held) -> i64 {
    held.0.get_or_insert_with(|| Arc::clone(value)).x
}

const NODES: [&NodeRecord; 8] = [
    &MAKE_NODE,
    &SUM_A_NODE,
    &SUM_B_NODE,
    &ADD_NODE,
    &PASS_NODE,
    &TAKE_NODE,
    &BOOM_NODE,
    &HOLD_NODE,
];

fn edge(from: u32, from_port: &str, to: u32, to_port: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

/// The host bridge is node 0: it feeds `x` and reads `out`, an `i64`.
fn bridge() -> NodeDoc {
    let mut host = NodeDoc::new("host");
    host.kind = NodeKindDoc::HostBridge;
    host.port_types.insert("x".into(), "i64".into());
    host.port_types.insert("out".into(), "i64".into());
    host
}

/// Plans `nodes` and `edges` on the CPU backend. Merging can be turned off, so both the one step
/// per node form and the merged form are run.
fn planned(ids: &[&str], edges: Vec<EdgeDoc>, merge: bool) -> Planned {
    let mut nodes = vec![bridge()];
    nodes.extend(ids.iter().map(|id| NodeDoc::new(*id)));
    let graph = GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    };
    let mut registry = Registry::new();
    registry.register_all(&NODES).unwrap();
    let config = Config {
        merge,
        ..Config::default()
    };
    plan(&graph, &registry, &[&CpuBackend], &config).unwrap()
}

fn load(planned: Planned) -> LoadedHost {
    LoadedHost::load(planned, LoadOptions::default()).unwrap()
}

fn through(host: &mut LoadedHost, x: i64) -> i64 {
    let io = host.host();
    io.feed_typed("x", Arc::new(x)).unwrap();
    io.step().unwrap();
    io.take_copy::<i64>("out").unwrap().unwrap()
}

/// `through`, also counting the allocations the tick itself makes. The feed is made before the
/// count starts, since making the input is not part of a tick.
fn through_counted(host: &mut LoadedHost, x: i64) -> (i64, usize) {
    let io = host.host();
    io.feed_typed("x", Arc::new(x)).unwrap();
    let (stepped, allocations) = counted(|| io.step().is_ok());
    assert!(stepped, "the tick ran");
    (io.take_copy::<i64>("out").unwrap().unwrap(), allocations)
}

/// `x -> make`, read by `sum_a` and `sum_b`, which are added and sent to the host.
/// Node ids: 1 make, 2 sum_a, 3 sum_b, 4 add.
fn two_readers(merge: bool) -> LoadedHost {
    let edges = vec![
        edge(0, "x", 1, "x"),
        edge(1, "value", 2, "value"),
        edge(1, "value", 3, "value"),
        edge(2, "sum", 4, "a"),
        edge(3, "sum", 4, "b"),
        edge(4, "total", 0, "out"),
    ];
    load(planned(
        &["k.make", "k.sum_a", "k.sum_b", "k.add"],
        edges,
        merge,
    ))
}

#[test]
fn a_value_read_by_two_consumers_is_kept_and_both_see_it_each_tick() {
    let _turn = turn();
    for merge in [false, true] {
        let mut host = two_readers(merge);
        for x in 1..=50 {
            // sum_a gives 10x, sum_b gives x + 1: both read the value made this tick.
            let (sum, allocations) = through_counted(&mut host, x);
            assert_eq!(sum, 11 * x + 1, "merge {merge}, x {x}");
            // Once warm, the value is refilled in place: the tick allocates nothing.
            if x > 2 {
                assert_eq!(allocations, 0, "merge {merge}, x {x}");
            }
        }
        drop(host);
        assert_eq!(
            LIVE.load(Ordering::SeqCst),
            0,
            "merge {merge}: nothing left alive"
        );
    }
}

#[test]
fn a_value_taken_by_its_consumer_is_moved_and_never_cloned() {
    let _turn = turn();
    // make -> pass -> pass -> take: each consumer takes the value by value, so it moves along.
    // `Big` panics when cloned, so any copy fails the test.
    let edges = vec![
        edge(0, "x", 1, "x"),
        edge(1, "value", 2, "value"),
        edge(2, "value", 3, "value"),
        edge(3, "value", 4, "value"),
        edge(4, "x", 0, "out"),
    ];
    for merge in [false, true] {
        let mut host = load(planned(
            &["k.make", "k.pass", "k.pass", "k.take"],
            edges.clone(),
            merge,
        ));
        for x in 1..=50 {
            let (taken, allocations) = through_counted(&mut host, x);
            assert_eq!(taken, x, "merge {merge}, x {x}");
            // Each stage moves the value and refills the cell it came from: nothing is made.
            if x > 2 {
                assert_eq!(allocations, 0, "merge {merge}, x {x}");
            }
        }
        drop(host);
        assert_eq!(
            LIVE.load(Ordering::SeqCst),
            0,
            "merge {merge}: nothing left alive"
        );
    }
}

#[test]
fn a_value_a_consumer_holds_across_ticks_is_never_overwritten() {
    let _turn = turn();
    // make -> hold keeps the first value through a handle. The producer must not refill that
    // value while it is held, so every later tick still reads the first one.
    let edges = vec![
        edge(0, "x", 1, "x"),
        edge(1, "value", 2, "value"),
        edge(2, "x", 0, "out"),
    ];
    for merge in [false, true] {
        let mut host = load(planned(&["k.make", "k.hold"], edges.clone(), merge));
        for x in 1..=20 {
            assert_eq!(through(&mut host, x), 1, "merge {merge}, x {x}");
        }
        drop(host);
        assert_eq!(
            LIVE.load(Ordering::SeqCst),
            0,
            "merge {merge}: nothing left alive"
        );
    }
}

#[test]
fn a_panic_in_a_body_leaves_nothing_leaked_or_freed_twice() {
    let _turn = turn();
    // make -> boom -> take. Boom panics on 3, with the value in its hands.
    let edges = vec![
        edge(0, "x", 1, "x"),
        edge(1, "value", 2, "value"),
        edge(2, "value", 3, "value"),
        edge(3, "x", 0, "out"),
    ];
    for merge in [false, true] {
        let mut host = load(planned(
            &["k.make", "k.boom", "k.take"],
            edges.clone(),
            merge,
        ));
        assert_eq!(through(&mut host, 1), 1);
        {
            let io = host.host();
            io.feed_typed("x", Arc::new(3_i64)).unwrap();
            // The body panics with the value in its hands. The panic may come out of the step or
            // be reported as a fault; either way nothing is left behind.
            let _ = catch_unwind(AssertUnwindSafe(|| io.step().is_ok()));
        }
        // The graph still runs after the failure, and the next tick's value is right.
        assert_eq!(through(&mut host, 4), 4, "merge {merge}");
        drop(host);
        assert_eq!(
            LIVE.load(Ordering::SeqCst),
            0,
            "merge {merge}: nothing left alive"
        );
    }
}

/// `x -> make`, with `make`'s output sent only to the host, as `big`.
fn host_only(merge: bool) -> LoadedHost {
    let mut host = bridge();
    host.port_types.remove("out");
    host.port_types.insert("big".into(), "outputs:big".into());
    let mut nodes = vec![host];
    nodes.push(NodeDoc::new("k.make"));
    let graph = GraphBody {
        nodes,
        edges: vec![edge(0, "x", 1, "x"), edge(1, "value", 0, "big")],
        groups: Vec::new(),
    };
    let mut registry = Registry::new();
    registry.register_all(&NODES).unwrap();
    let config = Config {
        merge,
        ..Config::default()
    };
    load(plan(&graph, &registry, &[&CpuBackend], &config).unwrap())
}

#[test]
fn a_host_that_takes_its_output_by_value_gets_it_moved_with_no_copy_or_allocation() {
    let _turn = turn();
    // `take::<Big>` takes the value out of the cell the producer refills. `Big` panics when
    // cloned, so a copy fails the test. The tick and the take are counted; the feed is not.
    for merge in [false, true] {
        let mut host = host_only(merge);
        for x in 1..=50 {
            let io = host.host();
            io.feed_typed("x", Arc::new(x)).unwrap();
            let ((stepped, taken), allocations) = counted(|| {
                let stepped = io.step().is_ok();
                (stepped, io.take::<Big>("big"))
            });
            assert!(stepped, "merge {merge}, x {x}: the tick ran");
            let big = taken.unwrap().expect("a value for the host");
            assert_eq!(big.x, x, "merge {merge}, x {x}");
            drop(big);
            if x > 2 {
                assert_eq!(allocations, 0, "merge {merge}, x {x}");
            }
        }
        drop(host);
        assert_eq!(
            LIVE.load(Ordering::SeqCst),
            0,
            "merge {merge}: nothing left alive"
        );
    }
}
