//! The CPU timings a board run records. It runs on boards and by hand, not in the gate:
//! `cargo run --release -p daedalus-cpu-timings -- SECTION [BATCHES] [LIBRARY]`, with the
//! sections `host_round_trip`, `host_idle_step`, `probe_off`, `probe_on` and `plugin_tick`.
//!
//! Each section warms up, then times BATCHES batches of operations and prints the median of the
//! per-operation times of the batches, in nanoseconds. A batch is 1,000 operations, so the clock's
//! own cost is spread over them. `plugin_tick` needs the fixture's library path.
#![allow(clippy::print_stdout, clippy::print_stderr)] // the measurements and usage are this program's output

use std::path::Path;
use std::sync::OnceLock;
use std::time::Instant;

use daedalus_budget_harness::plugin::{plugin_node, plugin_tick};
use daedalus_budget_harness::{Stage, chain, chain_tick};
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost, TickOverheadWindow};
use daedalus_macros::node;
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::{NodeRecord, Registry};
use daedalus_runner::TickProbe;

/// Operations per batch.
const PER_BATCH: u32 = 1_000;
/// Batches before timing starts.
const WARM_UP: u32 = 20;
/// Batches timed when none is given.
const BATCHES: usize = 101;

/// A node that hands its input on: one step of the host's round trip.
#[node(id = "timings.pass", inputs(x), outputs(out))]
fn pass(x: i64) -> i64 {
    x
}

const NODES: [&NodeRecord; 1] = [&PASS_NODE];

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

/// `host.x -> pass -> host.out`: one pass-through step between the host's ports.
fn pass_through() -> Planned {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    for port in ["x", "out"] {
        bridge
            .port_types
            .insert(port.to_string(), "i64".to_string());
    }
    let graph = GraphBody {
        nodes: vec![bridge, NodeDoc::new("timings.pass")],
        edges: vec![edge(0, "x", 1, "x"), edge(1, "out", 0, "out")],
        groups: Vec::new(),
    };
    let mut registry = Registry::new();
    registry.register_all(&NODES).expect("the node registers");
    plan(
        &graph,
        &registry,
        &[&daedalus_cpu::CpuBackend],
        &Config::default(),
    )
    .expect("the graph plans")
}

/// Nanoseconds since the first call. The probe's clock.
fn clock() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

/// Times `batches` batches of `PER_BATCH` calls to `op` after a warm-up, and returns each batch's
/// time per call, in nanoseconds, sorted.
fn per_call(batches: usize, mut op: impl FnMut()) -> Vec<f64> {
    for _ in 0..WARM_UP {
        for _ in 0..PER_BATCH {
            op();
        }
    }
    let mut times = Vec::with_capacity(batches);
    for _ in 0..batches {
        let start = Instant::now();
        for _ in 0..PER_BATCH {
            op();
        }
        times.push(start.elapsed().as_nanos() as f64 / f64::from(PER_BATCH));
    }
    times.sort_by(f64::total_cmp);
    times
}

fn median(sorted: &[f64]) -> f64 {
    sorted[sorted.len() / 2]
}

fn report(section: &str, times: &[f64]) {
    println!(
        "{section} median_ns {:.1} min_ns {:.1} max_ns {:.1} batches {} calls_per_batch {PER_BATCH}",
        median(times),
        times[0],
        times[times.len() - 1],
        times.len()
    );
}

fn round_trip(batches: usize) {
    let mut host = LoadedHost::load(pass_through(), LoadOptions::default()).expect("it loads");
    let mut value = 0_i64;
    let times = per_call(batches, || {
        value = value.wrapping_add(1);
        let io = host.host();
        io.feed_inline("x", &value.to_le_bytes()).expect("a feed");
        io.step().expect("a tick");
        let out = io
            .take_copy::<i64>("out")
            .expect("a take")
            .expect("a value");
        assert_eq!(out, value);
    });
    report("host_round_trip", &times);
}

fn idle_step(batches: usize) {
    let mut host = LoadedHost::load(pass_through(), LoadOptions::default()).expect("it loads");
    let times = per_call(batches, || {
        host.host().step().expect("a tick");
    });
    report("host_idle_step", &times);
}

/// C1's 16-stage unfused chain, one feed and one take a tick, with no probe attached.
fn probe_off(batches: usize) {
    let mut host = chain(16, Stage::Plain, false);
    let mut value = 0_u64;
    let times = per_call(batches, || {
        value = value.wrapping_add(1);
        assert_eq!(chain_tick(&mut host, value), value.wrapping_add(16));
    });
    report("probe_off", &times);
}

/// The same chain with a probe attached to its runner, the window recording every tick. The
/// window's figures for the last batch are printed too: `overhead_ns` is the tick time outside the
/// step bodies.
fn probe_on(batches: usize) {
    let probe: &'static TickProbe<'static> = Box::leak(Box::new(TickProbe::new(clock)));
    let mut host = chain(16, Stage::Plain, false);
    host.host().runner().set_probe(probe);
    let mut window = TickOverheadWindow::new(PER_BATCH as usize);
    let mut value = 0_u64;
    let times = per_call(batches, || {
        value = value.wrapping_add(1);
        assert_eq!(chain_tick(&mut host, value), value.wrapping_add(16));
        window.record(probe);
    });
    report("probe_on", &times);
    let figures = window.report();
    println!(
        "probe_on window ticks {} p50_tick_ns {} p99_tick_ns {} mean_tick_ns {:.1} mean_overhead_ns {:.1}",
        figures.ticks, figures.p50_ns, figures.p99_ns, figures.mean_ns, figures.mean_overhead_ns
    );
}

fn plugin_tick_section(batches: usize, library: &Path) {
    let mut host = plugin_node(library);
    let mut value = 0_u32;
    let times = per_call(batches, || {
        value = value.wrapping_add(1);
        assert_eq!(plugin_tick(&mut host, value), value);
    });
    report("plugin_tick", &times);
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(section) = args.first() else {
        eprintln!("usage: cpu-timings SECTION [BATCHES] [LIBRARY]");
        std::process::exit(2);
    };
    let batches = args
        .get(1)
        .map_or(BATCHES, |text| text.parse().expect("BATCHES is a number"));
    match section.as_str() {
        "host_round_trip" => round_trip(batches),
        "host_idle_step" => idle_step(batches),
        "probe_off" => probe_off(batches),
        "probe_on" => probe_on(batches),
        "plugin_tick" => {
            let library = args.get(2).expect("plugin_tick needs the library's path");
            plugin_tick_section(batches, Path::new(library));
        }
        other => {
            eprintln!("no section {other}");
            std::process::exit(2);
        }
    }
}
