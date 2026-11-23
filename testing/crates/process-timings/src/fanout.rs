//! One CPU producer whose list goes to a CPU reader and to a worker at once: what the CPU reader
//! pays for reading the list in `cpu.wire`, where the worker's copy is written, against reading it
//! in `cpu.host` with no worker in the graph.
//!
//! Three shapes. A `Vec<u8>`, which each reader takes as `&[u8]`; a `list<f32>`, which the CPU
//! reader takes as `&[f32]` and views where its bytes are; and a list of 28-byte records, which it takes as a `ListView` and reads where the bytes are,
//! one record at a time. Four graphs per shape and
//! size, each timed by the tick:
//!
//! - `producer`: the producer alone.
//! - `host`: the producer and the CPU reader, in `cpu.host`.
//! - `worker`: the producer and the worker's reader, through `cpu.wire`.
//! - `fan`: the producer, the CPU reader and the worker's reader.
//!
//! The CPU reader's cost is `host - producer` with no worker, and `fan - worker` beside one.
//!
//! Each shape is run with two producers: one that returns its list (`vec`), and one that writes it
//! through an `OutView` (`view`), which the planner places in the worker's lent memory when a
//! worker reads it, so the producer writes where the worker reads and nothing is copied.
//!
//! For a profiler: `DAEDALUS_FAN_ONLY=shape:producer:graph:bytes` runs that one graph alone, and
//! `DAEDALUS_FAN_WORKER_FIRST=1` puts the worker's reader before the CPU reader in the graph.

use std::path::Path;
use std::time::Instant;

use daedalus::layers::planner::explain::explain_plan;
use daedalus::prelude::*;
use daedalus_backend_process::{ProcessBackend, WorkerSpec};
use daedalus_cpu::CpuBackend;
use daedalus_cpu_author::{ListView, OutView, Refused};

use crate::{median, minor_faults, p99};
use daedalus_abi::counting::counted;

/// A keypoint's shape, as the sweep worker's `Point` declares it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Type, WireForm)]
#[structural]
pub struct Point {
    pub x: f32,
    pub y: f32,
    pub size: f32,
    pub angle: f32,
    pub response: f32,
    pub octave: i32,
    pub class: i32,
}

#[node(id = "fan.floats", inputs(n), outputs(out))]
pub fn floats(n: u32) -> Vec<f32> {
    (0..n).map(|at| at as f32 * 0.5).collect()
}

#[node(id = "fan.floats_view", inputs(n))]
pub fn floats_view(n: u32, mut out: OutView<'_, f32>) -> Result<(), Refused> {
    for (item, at) in out.items_mut(n as usize)?.iter_mut().zip(0..n) {
        *item = at as f32 * 0.5;
    }
    Ok(())
}

#[node(id = "fan.bytes", inputs(n), outputs(out))]
pub fn bytes(n: u32) -> Vec<u8> {
    (0..n).map(|at| at as u8).collect()
}

#[node(id = "fan.bytes_view", inputs(n))]
pub fn bytes_view(n: u32, mut out: OutView<'_, u8>) -> Result<(), Refused> {
    for (item, at) in out.items_mut(n as usize)?.iter_mut().zip(0..n) {
        *item = at as u8;
    }
    Ok(())
}

#[node(id = "fan.bytes_sum", inputs(items), outputs(sum))]
pub fn bytes_sum(items: &[u8]) -> u32 {
    items
        .iter()
        .fold(0u32, |sum, byte| sum.wrapping_add(u32::from(*byte)))
}

#[node(id = "fan.floats_sum", inputs(items), outputs(sum))]
pub fn floats_sum(items: &[f32]) -> u32 {
    items
        .iter()
        .fold(0u32, |sum, v| sum.wrapping_add(v.to_bits()))
}

#[node(id = "fan.points", inputs(n), outputs(out))]
pub fn points(n: u32) -> Vec<Point> {
    (0..n)
        .map(|at| Point {
            x: at as f32,
            response: 0.5,
            class: (at % 7) as i32,
            ..Point::default()
        })
        .collect()
}

#[node(id = "fan.points_view", inputs(n))]
pub fn points_view(n: u32, mut out: OutView<'_, Point>) -> Result<(), Refused> {
    out.reserve_bytes(n as usize * 28)?;
    out.extend((0..n).map(|at| Point {
        x: at as f32,
        response: 0.5,
        class: (at % 7) as i32,
        ..Point::default()
    }))
}

#[node(id = "fan.points_sum", inputs(items), outputs(sum))]
pub fn points_sum(items: ListView<'_, Point>) -> u32 {
    items.iter().fold(0u32, |sum, p| {
        sum.wrapping_add(p.x.to_bits())
            .wrapping_add(p.response.to_bits())
            .wrapping_add(p.class as u32)
    })
}

struct Shape {
    name: &'static str,
    item: usize,
    producer: &'static str,
    /// The producer that writes through an `OutView`.
    view: &'static str,
    reader: &'static str,
    worker: &'static str,
}

const SHAPES: [Shape; 3] = [
    Shape {
        name: "u8",
        item: 1,
        producer: "fan.bytes",
        view: "fan.bytes_view",
        reader: "fan.bytes_sum",
        worker: "sweep.bytes_view",
    },
    Shape {
        name: "f32",
        item: 4,
        producer: "fan.floats",
        view: "fan.floats_view",
        reader: "fan.floats_sum",
        worker: "sweep.floats_view",
    },
    Shape {
        name: "point28",
        item: 28,
        producer: "fan.points",
        view: "fan.points_view",
        reader: "fan.points_sum",
        worker: "sweep.points_view",
    },
];

const SIZES: [usize; 3] = [4 << 10, 64 << 10, 1 << 20];

/// The graph: the host feeds `n` to the producer, and takes `y` from the CPU reader and `z` from
/// the worker's, each when it is there.
fn graph(shape: &Shape, producer: &str, reader: bool, worker: bool) -> GraphBody {
    let mut nodes = vec![format!(r#"{{"id": "{producer}"}}"#)];
    let mut edges =
        vec![r#"{"from": {"node": 0, "port": "n"}, "to": {"node": 1, "port": "n"}}"#.to_owned()];
    let mut ports = vec![r#""n": "u32""#];
    let mut readers = [
        (reader, shape.reader, "items", "y"),
        (worker, shape.worker, "in", "z"),
    ];
    if std::env::var_os("DAEDALUS_FAN_WORKER_FIRST").is_some() {
        readers.reverse();
    }
    for (on, id, input, output) in readers {
        if !on {
            continue;
        }
        nodes.push(format!(r#"{{"id": "{id}"}}"#));
        let at = nodes.len();
        edges.push(format!(
            r#"{{"from": {{"node": 1, "port": "out"}}, "to": {{"node": {at}, "port": "{input}"}}}}"#
        ));
        edges.push(format!(
            r#"{{"from": {{"node": {at}, "port": "sum"}}, "to": {{"node": 0, "port": "{output}"}}}}"#
        ));
        ports.push(if output == "y" {
            r#""y": "u32""#
        } else {
            r#""z": "u32""#
        });
    }
    let text = format!(
        r#"{{"format": "daedalus.graph", "schema_version": 1, "graph": {{
  "nodes": [{{"id": "host", "kind": "host_bridge", "label": "io", "port_types": {{{}}}}}, {}],
  "edges": [{}]}}}}"#,
        ports.join(", "),
        nodes.join(", "),
        edges.join(", ")
    );
    GraphDoc::parse(&text).expect("the graph parses").graph
}

/// The graph loaded, with how many decodes its plan lists.
fn load(program: &Path, graph: &GraphBody) -> (LoadedHost, usize) {
    let mut registry = Registry::new();
    registry
        .register_all(&[
            &BYTES_NODE,
            &BYTES_VIEW_NODE,
            &BYTES_SUM_NODE,
            &FLOATS_NODE,
            &FLOATS_VIEW_NODE,
            &FLOATS_SUM_NODE,
            &POINTS_NODE,
            &POINTS_VIEW_NODE,
            &POINTS_SUM_NODE,
        ])
        .expect("the CPU nodes register");
    let mut workers = ProcessBackend::new();
    workers
        .add_worker_declaring(WorkerSpec::new(program), &mut registry)
        .expect("the sweep worker registers");
    let planned = plan(
        graph,
        &registry,
        &[&CpuBackend, &workers],
        &Config::default(),
    )
    .expect("the graph plans");
    let decodes = explain_plan(&planned).decodes.len();
    let host = LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads");
    (host, decodes)
}

/// The median and 99th percentile tick, in nanoseconds, feeding `n` and taking `outputs`; then
/// this process's page faults per 1,000 ticks and allocations per tick, once warm.
fn time(host: &mut LoadedHost, n: u32, outputs: &[&str], runs: usize) -> (f64, f64, f64, f64) {
    let mut tick = || {
        host.host()
            .feed_inline("n", &n.to_le_bytes())
            .expect("a feed");
        host.step().expect("a tick");
        if let Some(fault) = host.fault_reports().next() {
            panic!("a fault: {}", fault.reason());
        }
        for output in outputs {
            let taken = host.host().take_copy::<u32>(output).expect("a take");
            assert!(taken.is_some(), "{output} has a value every tick");
        }
    };
    for _ in 0..runs / 10 + 4 {
        tick();
    }
    let mut times = Vec::with_capacity(runs);
    for _ in 0..runs {
        let start = Instant::now();
        tick();
        times.push(start.elapsed().as_nanos() as f64);
    }
    times.sort_by(f64::total_cmp);
    let faults = minor_faults();
    let (_, allocations) = counted(|| {
        for _ in 0..runs {
            tick();
        }
    });
    let faults = (minor_faults() - faults) as f64 * 1000.0 / runs as f64;
    (
        median(&times),
        p99(&times),
        faults,
        allocations as f64 / runs as f64,
    )
}

/// Runs the fan-out with the sweep worker at `program`, `runs` timed ticks per graph.
pub fn fanout(program: &Path, runs: usize) {
    let only = std::env::var("DAEDALUS_FAN_ONLY").ok();
    for shape in &SHAPES {
        for (kind, producer) in [("vec", shape.producer), ("view", shape.view)] {
            let graphs = [
                ("producer", graph(shape, producer, false, false), &[][..]),
                ("host", graph(shape, producer, true, false), &["y"][..]),
                ("worker", graph(shape, producer, false, true), &["z"][..]),
                ("fan", graph(shape, producer, true, true), &["y", "z"][..]),
            ];
            let chosen = |name: &str, size: usize| {
                only.as_deref()
                    .is_none_or(|only| only == format!("{}:{kind}:{name}:{size}", shape.name))
            };
            let mut loaded: Vec<(&str, LoadedHost, usize, &[&str])> = graphs
                .into_iter()
                .filter(|(name, _, _)| SIZES.iter().any(|size| chosen(name, *size)))
                .map(|(name, graph, outputs)| {
                    let (host, decodes) = load(program, &graph);
                    (name, host, decodes, outputs)
                })
                .collect();
            for size in SIZES {
                let n = (size / shape.item) as u32;
                let mut line = format!(
                    "fanout shape {} producer {kind} bytes {size} items {n}",
                    shape.name
                );
                let mut medians = [None; 4];
                for (name, host, decodes, outputs) in loaded.iter_mut() {
                    if !chosen(name, size) {
                        continue;
                    }
                    let (ns, tail, faults, allocations) = time(host, n, outputs, runs);
                    let at = ["producer", "host", "worker", "fan"]
                        .iter()
                        .position(|one| one == name)
                        .unwrap_or(0);
                    medians[at] = Some(ns);
                    line.push_str(&format!(
                        " {name}_ns {ns:.0} {name}_p99_ns {tail:.0} {name}_decodes {decodes} {name}_faults_per_1000 {faults:.0} {name}_allocs_per_tick {allocations:.2}"
                    ));
                }
                if let [Some(producer), Some(host), Some(worker), Some(fan)] = medians {
                    line.push_str(&format!(
                        " cpu_reader_alone_ns {:.0} cpu_reader_beside_worker_ns {:.0}",
                        host - producer,
                        fan - worker
                    ));
                }
                if medians.iter().any(Option::is_some) {
                    println!("{line}");
                }
            }
        }
    }
}
