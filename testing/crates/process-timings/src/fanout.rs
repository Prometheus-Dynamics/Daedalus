//! One CPU producer whose list goes to a CPU reader and to a worker at once: what the CPU reader
//! pays for reading the list in `cpu.wire`, where the worker's copy is written, against reading it
//! in `cpu.host` with no worker in the graph.
//!
//! Two shapes. A `list<f32>`, which the CPU reader takes as `&[f32]` and views where its bytes are;
//! and a list of 28-byte records, which it decodes, a cost the plan lists. Four graphs per shape and
//! size, each timed by the tick:
//!
//! - `producer`: the producer alone.
//! - `host`: the producer and the CPU reader, in `cpu.host`.
//! - `worker`: the producer and the worker's reader, through `cpu.wire`.
//! - `fan`: the producer, the CPU reader and the worker's reader.
//!
//! The CPU reader's cost is `host - producer` with no worker, and `fan - worker` beside one.

use std::path::Path;
use std::time::Instant;

use daedalus::layers::planner::explain::explain_plan;
use daedalus::prelude::*;
use daedalus_backend_process::{ProcessBackend, WorkerSpec};
use daedalus_cpu::CpuBackend;

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

#[node(id = "fan.points_sum", inputs(items), outputs(sum))]
pub fn points_sum(items: &[Point]) -> u32 {
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
    reader: &'static str,
    worker: &'static str,
}

const SHAPES: [Shape; 2] = [
    Shape {
        name: "f32",
        item: 4,
        producer: "fan.floats",
        reader: "fan.floats_sum",
        worker: "sweep.floats_view",
    },
    Shape {
        name: "point28",
        item: 28,
        producer: "fan.points",
        reader: "fan.points_sum",
        worker: "sweep.points_view",
    },
];

const SIZES: [usize; 3] = [4 << 10, 64 << 10, 1 << 20];

/// The graph: the host feeds `n` to the producer, and takes `y` from the CPU reader and `z` from
/// the worker's, each when it is there.
fn graph(shape: &Shape, reader: bool, worker: bool) -> GraphBody {
    let mut nodes = vec![format!(r#"{{"id": "{}"}}"#, shape.producer)];
    let mut edges =
        vec![r#"{"from": {"node": 0, "port": "n"}, "to": {"node": 1, "port": "n"}}"#.to_owned()];
    let mut ports = vec![r#""n": "u32""#];
    for (on, id, input, output) in [
        (reader, shape.reader, "items", "y"),
        (worker, shape.worker, "in", "z"),
    ] {
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
            &FLOATS_NODE,
            &FLOATS_SUM_NODE,
            &POINTS_NODE,
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
    for shape in &SHAPES {
        let graphs = [
            ("producer", graph(shape, false, false), &[][..]),
            ("host", graph(shape, true, false), &["y"][..]),
            ("worker", graph(shape, false, true), &["z"][..]),
            ("fan", graph(shape, true, true), &["y", "z"][..]),
        ];
        let mut loaded: Vec<(&str, LoadedHost, usize, &[&str])> = graphs
            .into_iter()
            .map(|(name, graph, outputs)| {
                let (host, decodes) = load(program, &graph);
                (name, host, decodes, outputs)
            })
            .collect();
        for size in SIZES {
            let n = (size / shape.item) as u32;
            let mut line = format!("fanout shape {} bytes {size} items {n}", shape.name);
            let mut medians = [0.0; 4];
            for (at, (name, host, decodes, outputs)) in loaded.iter_mut().enumerate() {
                let (ns, tail, faults, allocations) = time(host, n, outputs, runs);
                medians[at] = ns;
                line.push_str(&format!(
                    " {name}_ns {ns:.0} {name}_p99_ns {tail:.0} {name}_decodes {decodes} {name}_faults_per_1000 {faults:.0} {name}_allocs_per_tick {allocations:.2}"
                ));
            }
            let [producer, host, worker, fan] = medians;
            println!(
                "{line} cpu_reader_alone_ns {:.0} cpu_reader_beside_worker_ns {:.0}",
                host - producer,
                fan - worker
            );
        }
    }
}
