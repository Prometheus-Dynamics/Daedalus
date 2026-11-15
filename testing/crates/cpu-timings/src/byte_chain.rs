//! The C1 chain: 1, 4 and 16 pass-through CPU stages carrying one 307,200-byte value (640 x 480
//! bytes), handed on by reference. The value is in dma-heap memory when this process can
//! allocate from `/dev/dma_heap/system`, and in ordinary memory otherwise; the output says which.
//!
//! Each tick feeds one handle to the value, runs the chain, and takes the value back out. The
//! value type has no `Clone`, so nothing can copy it on the way through. The chain is planned
//! twice, unfused and fused, and the least-squares line through the per-stage p50s gives the
//! cost per stage.

use std::sync::Arc;
use std::time::Instant;

use daedalus_dma_sys::{DmaBuf, DmaHeap};
use daedalus_graph::{GraphBody, NodeDoc, NodeKindDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_macros::node;
use daedalus_planner::{Config, plan};
use daedalus_registry::{NodeRecord, Registry};
use daedalus_types::{TypeExpr, TypeKey, Typed};

use crate::edge;

/// Bytes in the value: 640 x 480 bytes.
const BYTES: usize = 640 * 480;
/// The stage counts the fit is taken over.
const STAGES: [u32; 3] = [1, 4, 16];
/// Ticks timed when none is given.
const TICKS: usize = 5_000;
/// Ticks run before anything is timed.
const WARM_UP: usize = 200;

/// Where the value's bytes live. The payload is held, not read: it keeps the memory alive.
#[allow(dead_code)]
enum Backing {
    Dma(DmaBuf),
    Heap(Vec<u8>),
}

/// One value: a byte buffer. It is never cloned: stages hand on the `Arc` that holds it.
#[allow(dead_code)] // held for its memory; see `Backing`
pub struct Bytes {
    backing: Backing,
}

impl Typed for Bytes {
    const KEY: TypeKey = TypeKey::new("timings:bytes");
    const EXPR: TypeExpr = TypeExpr::Opaque(Self::KEY);
}

/// Takes the value by reference and hands the same handle on. Nothing is read or copied.
#[node(id = "timings.bytes.pass", inputs(x), outputs(out))]
fn pass_bytes(x: Arc<Bytes>) -> Arc<Bytes> {
    x
}

const NODES: [&NodeRecord; 1] = [&PASS_BYTES_NODE];

/// The value in dma-heap memory if the heap can be opened, else in ordinary memory, with the
/// reason in the second part of the pair.
fn value() -> (Arc<Bytes>, String) {
    let pattern = vec![0x5a_u8; BYTES];
    let heap = match DmaHeap::open("system") {
        Ok(heap) => heap,
        Err(error) => {
            let why =
                DmaHeap::why_not("system").map_or_else(|| error.to_string(), |why| why.to_string());
            return (ordinary(&pattern), format!("ordinary memory, {why}"));
        }
    };
    match fill(&heap, &pattern) {
        Ok(buf) => (
            Arc::new(Bytes {
                backing: Backing::Dma(buf),
            }),
            "dma-heap /dev/dma_heap/system".to_owned(),
        ),
        Err(error) => (
            ordinary(&pattern),
            format!("ordinary memory, the dma-heap allocation failed: {error}"),
        ),
    }
}

/// A dma-buf from `heap`, with `pattern` written in, so its pages are touched before timing.
fn fill(heap: &DmaHeap, pattern: &[u8]) -> Result<DmaBuf, daedalus_dma_sys::Error> {
    let buf = heap.allocate(BYTES as u64)?;
    let mapping = buf.as_ref().map()?;
    mapping.write(0, pattern)?;
    Ok(buf)
}

fn ordinary(pattern: &[u8]) -> Arc<Bytes> {
    Arc::new(Bytes {
        backing: Backing::Heap(pattern.to_vec()),
    })
}

/// A chain of `stages` pass-through stages between the host's `x` and `out`.
fn chain(stages: u32, merge: bool) -> LoadedHost {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    for port in ["x", "out"] {
        bridge
            .port_types
            .insert(port.to_string(), Bytes::KEY.as_str().to_string());
    }
    let mut nodes = vec![bridge];
    let mut edges = Vec::new();
    for at in 1..=stages {
        nodes.push(NodeDoc::new("timings.bytes.pass"));
        edges.push(match at {
            1 => edge(0, "x", 1, "x"),
            at => edge(at - 1, "out", at, "x"),
        });
    }
    edges.push(edge(stages, "out", 0, "out"));
    let graph = GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    };
    let mut registry = Registry::new();
    registry.register_all(&NODES).expect("the node registers");
    let config = Config {
        merge,
        ..Config::default()
    };
    let planned =
        plan(&graph, &registry, &[&daedalus_cpu::CpuBackend], &config).expect("the graph plans");
    LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads")
}

/// One tick: the handle fed in, the chain run, the value taken back out. Returns whether it came
/// out.
fn tick(host: &mut LoadedHost, value: &Arc<Bytes>) -> bool {
    let io = host.host();
    io.feed_typed("x", Arc::clone(value)).expect("a feed");
    io.step().expect("a tick");
    io.take_value("out").expect("a take").is_some()
}

/// Per-tick wall times in nanoseconds, sorted, after a warm-up. Each time includes the feed, the
/// run and the take, and two clock reads.
fn timed(stages: u32, merge: bool, ticks: usize, value: &Arc<Bytes>) -> Vec<u64> {
    let mut host = chain(stages, merge);
    for _ in 0..WARM_UP {
        assert!(tick(&mut host, value), "a value came out of the warm-up");
    }
    let mut times = Vec::with_capacity(ticks);
    for _ in 0..ticks {
        let start = Instant::now();
        let out = tick(&mut host, value);
        let elapsed = start.elapsed().as_nanos() as u64;
        assert!(out, "a value came out of a tick");
        times.push(elapsed);
    }
    times.sort_unstable();
    times
}

fn p50(sorted: &[u64]) -> u64 {
    sorted[sorted.len() / 2]
}

fn p99(sorted: &[u64]) -> u64 {
    sorted[(sorted.len() * 99 / 100).min(sorted.len() - 1)]
}

/// Least squares `intercept + slope * x` through `(x, y)` points: (intercept, slope).
fn fit(points: &[(f64, f64)]) -> (f64, f64) {
    let n = points.len() as f64;
    let sum_x: f64 = points.iter().map(|(x, _)| x).sum();
    let sum_y: f64 = points.iter().map(|(_, y)| y).sum();
    let sum_xy: f64 = points.iter().map(|(x, y)| x * y).sum();
    let sum_xx: f64 = points.iter().map(|(x, _)| x * x).sum();
    let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x);
    ((sum_y - slope * sum_x) / n, slope)
}

/// Runs the C1 chain: unfused, then fused, at 1, 4 and 16 stages, and prints each table and its
/// fit. `ticks` is the number of timed ticks per run.
pub fn run(ticks: Option<&String>) {
    let ticks = ticks.map_or(TICKS, |text| text.parse().expect("TICKS is a number"));
    let (value, backing) = value();
    println!(
        "byte_chain: {} {}, {BYTES}-byte value, {backing}, {ticks} ticks per run",
        std::env::consts::ARCH,
        std::env::consts::OS
    );
    for (label, merge) in [("unfused", false), ("fused", true)] {
        println!("\n== {label} ==");
        println!(
            "{:>6} {:>12} {:>12}",
            "stages", "tick p50 ns", "tick p99 ns"
        );
        let mut points = Vec::new();
        for stages in STAGES {
            let times = timed(stages, merge, ticks, &value);
            println!("{stages:>6} {:>12} {:>12}", p50(&times), p99(&times));
            points.push((f64::from(stages), p50(&times) as f64));
        }
        let (fixed, per_stage) = fit(&points);
        println!("tick p50 fit: {fixed:.0} ns fixed + {per_stage:.1} ns per stage");
    }
    println!("\nvalue copies: none (the value has no Clone; stages hand on the Arc)");
}
