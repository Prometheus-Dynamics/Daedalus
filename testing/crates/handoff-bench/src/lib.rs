//! Three ways to pass a large value along a graph, on the same graphs, so what each costs can
//! be counted: instructions and allocations a tick, copies of the bytes, and reference counts
//! touched.
//!
//! - [`Path::Handle`]: the value is a struct that owns its bytes. A node reads it through a
//!   reference, or takes it and hands it on.
//! - [`Path::Shared`]: the value is a reference-counted run of bytes. Handing it to another
//!   holder is a count, not a copy.
//! - [`Path::Vec`]: the value is a plain `Vec<u8>`, and a node that hands one on makes its
//!   own. This is the contrast.
//!
//! The value types are this crate's own: nothing in the core knows them. Each graph is built
//! once, and [`tick`] is one steady-state tick of it.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use daedalus_cpu::{CpuBackend, FanIn};
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_macros::node;
use daedalus_planner::{Config, plan};
use daedalus_registry::{NodeRecord, Registry};
use daedalus_types::{TypeExpr, TypeKey, Typed};

/// Times the bytes of a value were copied, by any path.
static COPIES: AtomicU64 = AtomicU64::new(0);
/// Times a shared value's reference count was raised by a node or the runner.
static RETAINS: AtomicU64 = AtomicU64::new(0);

/// How many copies of a value's bytes, and how many raised reference counts, since the
/// process started. The counters are the process's: a test that reads them runs alone.
pub fn touched() -> (u64, u64) {
    (
        COPIES.load(Ordering::Relaxed),
        RETAINS.load(Ordering::Relaxed),
    )
}

/// A large value that owns its bytes.
pub struct Frame {
    pub bytes: Vec<u8>,
}

impl Typed for Frame {
    const KEY: TypeKey = TypeKey::new("handoff:frame");
    const EXPR: TypeExpr = TypeExpr::Opaque(Self::KEY);
}

/// A second holder of a `Frame` gets its own copy of the bytes: the runner clones a value
/// that two nodes each take.
impl Clone for Frame {
    fn clone(&self) -> Frame {
        COPIES.fetch_add(1, Ordering::Relaxed);
        Frame {
            bytes: self.bytes.clone(),
        }
    }
}

/// A large value whose bytes are shared.
pub struct Shared {
    pub bytes: Arc<[u8]>,
}

impl Typed for Shared {
    const KEY: TypeKey = TypeKey::new("handoff:shared");
    const EXPR: TypeExpr = TypeExpr::Opaque(Self::KEY);
}

/// A second holder of a `Shared` raises the count and copies nothing.
impl Clone for Shared {
    fn clone(&self) -> Shared {
        RETAINS.fetch_add(1, Ordering::Relaxed);
        Shared {
            bytes: Arc::clone(&self.bytes),
        }
    }
}

/// A large value as a plain vector.
pub struct Plain {
    pub bytes: Vec<u8>,
}

impl Typed for Plain {
    const KEY: TypeKey = TypeKey::new("handoff:plain");
    const EXPR: TypeExpr = TypeExpr::Opaque(Self::KEY);
}

impl Clone for Plain {
    fn clone(&self) -> Plain {
        COPIES.fetch_add(1, Ordering::Relaxed);
        Plain {
            bytes: self.bytes.clone(),
        }
    }
}

fn ends(bytes: &[u8]) -> u64 {
    u64::from(bytes[0]) + u64::from(bytes[bytes.len() - 1])
}

/// Takes the value and hands it on. Nothing is read and nothing is copied.
#[node(id = "handoff.frame.pass", inputs(value), outputs(value))]
fn pass_frame(value: Frame) -> Frame {
    value
}

#[node(id = "handoff.frame.read", inputs(value), outputs(sum))]
fn read_frame(value: &Frame) -> u64 {
    ends(&value.bytes)
}

#[node(id = "handoff.shared.pass", inputs(value), outputs(value))]
fn pass_shared(value: Shared) -> Shared {
    value
}

#[node(id = "handoff.shared.read", inputs(value), outputs(sum))]
fn read_shared(value: &Shared) -> u64 {
    ends(&value.bytes)
}

/// The contrast: a stage that is handed a vector and gives the next stage one of its own.
#[node(id = "handoff.plain.pass", inputs(value), outputs(value))]
fn pass_plain(value: &Plain) -> Plain {
    COPIES.fetch_add(1, Ordering::Relaxed);
    Plain {
        bytes: value.bytes.clone(),
    }
}

#[node(id = "handoff.plain.read", inputs(value), outputs(sum))]
fn read_plain(value: &Plain) -> u64 {
    ends(&value.bytes)
}

#[node(id = "handoff.total", inputs(items), outputs(total))]
fn total(items: FanIn<'_, u64>) -> u64 {
    items.iter().map(|item| *item).sum()
}

const NODES: [&NodeRecord; 7] = [
    &PASS_FRAME_NODE,
    &READ_FRAME_NODE,
    &PASS_SHARED_NODE,
    &READ_SHARED_NODE,
    &PASS_PLAIN_NODE,
    &READ_PLAIN_NODE,
    &TOTAL_NODE,
];

/// Which way the value is passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Path {
    Handle,
    Shared,
    Vec,
}

impl Path {
    pub const ALL: [Path; 3] = [Path::Handle, Path::Shared, Path::Vec];

    pub fn name(self) -> &'static str {
        match self {
            Path::Handle => "handle",
            Path::Shared => "shared",
            Path::Vec => "vec",
        }
    }

    pub fn parse(text: &str) -> Option<Path> {
        Path::ALL.into_iter().find(|path| path.name() == text)
    }

    fn ids(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Path::Handle => ("handoff:frame", "handoff.frame.pass", "handoff.frame.read"),
            Path::Shared => (
                "handoff:shared",
                "handoff.shared.pass",
                "handoff.shared.read",
            ),
            Path::Vec => ("handoff:plain", "handoff.plain.pass", "handoff.plain.read"),
        }
    }
}

/// The shape of the graph.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// The value goes through this many stages, each taking it and handing it on, and the
    /// last stage's output is read once. Every value has one consumer.
    Chain(u32),
    /// The host's value is read through a reference by this many nodes.
    Readers(u32),
    /// The host's value is taken by this many stages. More than one can't each have it, so
    /// this plans only for one: see [`plans`].
    Takers(u32),
}

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

/// Builds the graph for `path` in `shape`, plans it on the CPU backend and loads it. The host
/// feeds `in` and takes the sum of what was read from `out`.
pub fn graph(path: Path, shape: Shape) -> LoadedHost {
    let planned = planned(path, shape).expect("the graph plans");
    LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads")
}

/// Whether the graph for `path` in `shape` plans, and why not when it doesn't.
pub fn plans(path: Path, shape: Shape) -> Result<(), String> {
    planned(path, shape).map(|_| ())
}

fn planned(path: Path, shape: Shape) -> Result<daedalus_planner::Planned, String> {
    let (key, pass, read) = path.ids();
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    bridge.port_types.insert("in".into(), key.into());
    bridge.port_types.insert("out".into(), "u64".into());
    let mut nodes = vec![bridge];
    let mut edges = Vec::new();
    // Every graph ends in one `total`, fed by its readers.
    let mut readers = Vec::new();
    match shape {
        Shape::Chain(stages) => {
            for at in 1..=stages {
                nodes.push(NodeDoc::new(pass));
                edges.push(match at {
                    1 => edge(0, "in", 1, "value"),
                    at => edge(at - 1, "value", at, "value"),
                });
            }
            nodes.push(NodeDoc::new(read));
            edges.push(edge(stages, "value", stages + 1, "value"));
            readers.push(stages + 1);
        }
        Shape::Readers(count) => {
            for at in 1..=count {
                nodes.push(NodeDoc::new(read));
                edges.push(edge(0, "in", at, "value"));
                readers.push(at);
            }
        }
        Shape::Takers(count) => {
            for at in 0..count {
                let (taker, reader) = (1 + at * 2, 2 + at * 2);
                nodes.push(NodeDoc::new(pass));
                nodes.push(NodeDoc::new(read));
                edges.push(edge(0, "in", taker, "value"));
                edges.push(edge(taker, "value", reader, "value"));
                readers.push(reader);
            }
        }
    }
    let total = nodes.len() as u32;
    nodes.push(NodeDoc::new("handoff.total"));
    for (at, reader) in readers.iter().enumerate() {
        edges.push(edge(*reader, "sum", total, &format!("items{at}")));
    }
    edges.push(edge(total, "total", 0, "out"));
    let body = GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    };
    let mut registry = Registry::new();
    registry.register_all(&NODES).expect("the nodes register");
    plan(&body, &registry, &[&CpuBackend], &Config::default()).map_err(|error| format!("{error:?}"))
}

/// How many nodes read the value in `shape`, which is what `out` sums over.
pub fn readers(shape: Shape) -> u64 {
    match shape {
        Shape::Chain(_) => 1,
        Shape::Readers(count) | Shape::Takers(count) => u64::from(count),
    }
}

/// `len` bytes whose first and last are 1 and 2.
fn bytes(len: usize) -> Vec<u8> {
    let mut bytes = vec![0; len.max(2)];
    bytes[0] = 1;
    let last = bytes.len() - 1;
    bytes[last] = 2;
    bytes
}

/// A value for `path`, ready to feed. Making one is not part of a tick.
pub enum Fed {
    Frame(Arc<Frame>),
    Shared(Arc<Shared>),
    Plain(Arc<Plain>),
}

pub fn value(path: Path, len: usize) -> Fed {
    let bytes = bytes(len);
    match path {
        Path::Handle => Fed::Frame(Arc::new(Frame { bytes })),
        Path::Shared => Fed::Shared(Arc::new(Shared {
            bytes: bytes.into(),
        })),
        Path::Vec => Fed::Plain(Arc::new(Plain { bytes })),
    }
}

impl Fed {
    /// Another handle to the same value, for a graph whose nodes only read it.
    pub fn again(&self) -> Fed {
        match self {
            Fed::Frame(value) => Fed::Frame(Arc::clone(value)),
            Fed::Shared(value) => Fed::Shared(Arc::clone(value)),
            Fed::Plain(value) => Fed::Plain(Arc::clone(value)),
        }
    }
}

/// Feeds a value. Not part of what [`tick`] counts.
pub fn feed(host: &mut LoadedHost, value: Fed) {
    let io = host.host();
    match value {
        Fed::Frame(value) => io.feed_typed("in", value),
        Fed::Shared(value) => io.feed_typed("in", value),
        Fed::Plain(value) => io.feed_typed("in", value),
    }
    .expect("a feed");
}

/// One tick of a graph that has been fed, and the sum its readers found.
pub fn tick(host: &mut LoadedHost) -> u64 {
    let io = host.host();
    io.step().expect("a tick");
    io.take_copy::<u64>("out").expect("a take").expect("a sum")
}

/// What `ticks` steady-state ticks cost, a tick: allocations, copies of the bytes, and
/// reference counts raised. The value fed each tick is made, or shared again, outside the
/// count. The copies and counts are the process's, so nothing else may run graphs meanwhile.
pub fn cost(path: Path, shape: Shape, len: usize, ticks: u64) -> Cost {
    let mut host = graph(path, shape);
    let kept = value(path, len);
    // A graph that takes the value needs the only handle to it. One that reads it is fed the
    // same value every tick.
    let fresh = !matches!(shape, Shape::Readers(_));
    let next = |kept: &Fed| match fresh {
        true => value(path, len),
        false => kept.again(),
    };
    for _ in 0..16 {
        feed(&mut host, next(&kept));
        assert_eq!(tick(&mut host), 3 * readers(shape));
    }
    let (copies, retains) = touched();
    let mut allocations = 0;
    for _ in 0..ticks {
        feed(&mut host, next(&kept));
        let (sum, count) = daedalus_abi::counting::counted(|| tick(&mut host));
        assert_eq!(sum, 3 * readers(shape));
        allocations += count as u64;
    }
    let after = touched();
    Cost {
        allocations: allocations / ticks,
        copies: (after.0 - copies) / ticks,
        retains: (after.1 - retains) / ticks,
    }
}

/// What one steady-state tick cost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cost {
    pub allocations: u64,
    pub copies: u64,
    pub retains: u64,
}
