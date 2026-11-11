//! The graphs the budget table is measured on. Each is built once, and then one function runs
//! one steady-state tick of it, which is what is counted.

#![forbid(unsafe_code)]

pub mod detector;
pub mod plugin;

use std::sync::Arc;

use daedalus_cpu::CpuBackend;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_macros::node;
use daedalus_planner::{Config, plan};
use daedalus_registry::{NodeRecord, Registry};
use daedalus_types::{TypeExpr, TypeKey, Typed};

/// A no-op stage on a number: the chain's stages do no work, so what is measured is the graph.
#[node(id = "budget.stage", inputs(x), outputs(y))]
fn stage(x: u64) -> u64 {
    x.wrapping_add(1)
}

/// A stage that does clearly more work than [`stage`]: the planted regression. One more
/// operation is a handful of instructions, which is less than the count moves by when the
/// compiler lays the chain out differently, so the planted work is a loop that no layout hides.
#[node(id = "budget.stage_extra", inputs(x), outputs(y))]
fn stage_extra(x: u64) -> u64 {
    let mut mixed = x;
    for round in 0..512u64 {
        mixed = std::hint::black_box(mixed.wrapping_mul(3).wrapping_add(round));
    }
    std::hint::black_box(mixed);
    x.wrapping_add(1)
}

/// A stage that allocates once a call: the planted regression for the allocation rows.
#[node(id = "budget.stage_alloc", inputs(x), outputs(y))]
fn stage_alloc(x: u64) -> u64 {
    let boxed = std::hint::black_box(Box::new(x));
    boxed.wrapping_add(1)
}

/// A large value, passed by handle: its megabyte is never copied.
pub struct Big {
    pub bytes: Vec<u8>,
}

impl Typed for Big {
    const KEY: TypeKey = TypeKey::new("budget:big");
    const EXPR: TypeExpr = TypeExpr::Opaque(Self::KEY);
}

/// Reads a large value through its handle.
#[node(id = "budget.first", inputs(big), outputs(byte))]
fn first(big: &Big) -> u64 {
    u64::from(big.bytes[0])
}

/// Reads the same large value somewhere else.
#[node(id = "budget.last", inputs(big), outputs(byte))]
fn last(big: &Big) -> u64 {
    u64::from(big.bytes[big.bytes.len() - 1])
}

#[node(id = "budget.sum", inputs(a, b), outputs(sum))]
fn sum(a: u64, b: u64) -> u64 {
    a + b
}

/// Hands a large value on by its handle: the node takes `Arc<Big>` and returns it, so the
/// megabyte isn't moved and nothing is allocated.
#[node(id = "budget.pass", inputs(big), outputs(big))]
fn pass(big: Arc<Big>) -> Arc<Big> {
    big
}

const NODES: [&NodeRecord; 7] = [
    &STAGE_NODE,
    &STAGE_EXTRA_NODE,
    &STAGE_ALLOC_NODE,
    &FIRST_NODE,
    &LAST_NODE,
    &SUM_NODE,
    &PASS_NODE,
];

pub(crate) fn edge(from: u32, from_port: &str, to: u32, to_port: &str) -> EdgeDoc {
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

/// A host bridge with the ports given, each with its type key.
pub(crate) fn bridge(ports: &[(&str, &str)]) -> NodeDoc {
    let mut doc = NodeDoc::new("host");
    doc.kind = NodeKindDoc::HostBridge;
    for (port, key) in ports {
        doc.port_types.insert(port.to_string(), key.to_string());
    }
    doc
}

/// Plans `graph` on the CPU backend and loads it. `merge` lets the backend fuse nodes.
pub(crate) fn load(graph: &GraphBody, records: &[&'static NodeRecord], merge: bool) -> LoadedHost {
    let mut registry = Registry::new();
    registry.register_all(records).expect("the nodes register");
    let config = Config {
        merge,
        ..Config::default()
    };
    let planned = plan(graph, &registry, &[&CpuBackend], &config).expect("the graph plans");
    LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads")
}

/// Which stage a chain is made of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Plain,
    /// One stage of the chain does one more operation.
    ExtraOp,
    /// One stage of the chain allocates.
    Allocates,
}

/// A chain of `stages` no-op stages between the host ports `in` and `out`.
pub fn chain(stages: u32, kind: Stage, merge: bool) -> LoadedHost {
    let mut nodes = vec![bridge(&[("in", "u64"), ("out", "u64")])];
    let mut edges = Vec::new();
    for at in 1..=stages {
        let id = match (kind, at) {
            (Stage::ExtraOp, 1) => "budget.stage_extra",
            (Stage::Allocates, 1) => "budget.stage_alloc",
            _ => "budget.stage",
        };
        nodes.push(NodeDoc::new(id));
        edges.push(match at {
            1 => edge(0, "in", 1, "x"),
            at => edge(at - 1, "y", at, "x"),
        });
    }
    edges.push(edge(stages, "y", 0, "out"));
    let graph = GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    };
    load(&graph, &NODES, merge)
}

/// One tick of a chain with a value in it: fed in place, stepped, taken.
pub fn chain_tick(host: &mut LoadedHost, value: u64) -> u64 {
    let io = host.host();
    io.feed_inline("in", &value.to_le_bytes()).expect("a feed");
    io.step().expect("a tick");
    io.take_copy::<u64>("out")
        .expect("a take")
        .expect("a value")
}

/// One tick of a chain with nothing fed: nothing is dirty, so no stage runs.
pub fn idle_tick(host: &mut LoadedHost) -> bool {
    host.host().step().expect("a tick").idle
}

/// A graph that passes one large value to two readers by handle, and sums what they read.
pub fn large_handle(moved: bool) -> LoadedHost {
    let mut nodes = vec![
        bridge(&[("big", "budget:big"), ("sum", "u64")]),
        NodeDoc::new("budget.first"),
        NodeDoc::new("budget.last"),
        NodeDoc::new("budget.sum"),
    ];
    let mut edges = vec![
        edge(1, "byte", 3, "a"),
        edge(2, "byte", 3, "b"),
        edge(3, "sum", 0, "sum"),
    ];
    if moved {
        // The value goes through a node that takes it and hands it on, and then to the readers.
        nodes.push(NodeDoc::new("budget.pass"));
        edges.extend([
            edge(0, "big", 4, "big"),
            edge(4, "big", 1, "big"),
            edge(4, "big", 2, "big"),
        ]);
    } else {
        edges.extend([edge(0, "big", 1, "big"), edge(0, "big", 2, "big")]);
    }
    let graph = GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    };
    load(&graph, &NODES, true)
}

/// A megabyte whose first and last bytes are 1 and 2.
pub fn big() -> Arc<Big> {
    let mut bytes = vec![0u8; 1 << 20];
    bytes[0] = 1;
    bytes[(1 << 20) - 1] = 2;
    Arc::new(Big { bytes })
}

/// Feeds the large value's handle. It is fed before the tick that is counted. A graph that
/// moves the value needs the only handle to it, so that one is given a value of its own.
pub fn large_feed(host: &mut LoadedHost, big: Arc<Big>) {
    host.host().feed_typed("big", big).expect("a feed");
}

pub fn large_step(host: &mut LoadedHost) -> u64 {
    let io = host.host();
    io.step().expect("a tick");
    io.take_copy::<u64>("sum").expect("a take").expect("a sum")
}
