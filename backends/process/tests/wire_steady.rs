//! A CPU output a worker reads costs nothing per tick once warm: its wire form is written over the
//! buffer it was written in last time, once nobody reads that any more. So a graph with a worker
//! makes no more allocations a tick than the same producer and reader with no worker, and touches
//! no new memory: a fresh buffer of a 1 MiB value each tick would be 256 page faults a tick.
//!
//! One test in its own binary, so no other test's allocations or page faults are counted. Page
//! faults are read from `/proc`, so it runs on Linux.
#![cfg(target_os = "linux")]

mod support;

use daedalus::prelude::*;
use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_backend_process::ProcessBackend;
use daedalus_cpu::CpuBackend;
use support::spec;

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

/// 1 MiB of `f32`.
const ITEMS: u32 = 1 << 18;
const TICKS: usize = 100;

#[node(id = "steady.floats", inputs(n), outputs(out))]
pub fn floats(n: u32) -> Vec<f32> {
    (0..n).map(|at| at as f32).collect()
}

#[node(id = "steady.count", inputs(items), outputs(y))]
pub fn count(items: &[f32]) -> u32 {
    items.len() as u32
}

/// The graph: host.n → `steady.floats`, then each chain of node ids after it, the last of each
/// giving one of host's `outputs`.
fn graph(chains: &[&[&str]], outputs: &[&str]) -> GraphBody {
    let mut nodes = vec![r#"{"id": "steady.floats"}"#.to_owned()];
    let mut edges =
        vec![r#"{"from": {"node": 0, "port": "n"}, "to": {"node": 1, "port": "n"}}"#.to_owned()];
    for (chain, output) in chains.iter().zip(outputs) {
        let mut from = (1, "out");
        for id in *chain {
            nodes.push(format!(r#"{{"id": "{id}"}}"#));
            let at = nodes.len();
            let input = if *id == "steady.count" { "items" } else { "in" };
            let output = match *id {
                "steady.count" => "y",
                "fixture.floats_count" => "count",
                _ => "out",
            };
            edges.push(format!(
                r#"{{"from": {{"node": {}, "port": "{}"}}, "to": {{"node": {at}, "port": "{input}"}}}}"#,
                from.0, from.1
            ));
            from = (at, output);
        }
        edges.push(format!(
            r#"{{"from": {{"node": {}, "port": "{}"}}, "to": {{"node": 0, "port": "{output}"}}}}"#,
            from.0, from.1
        ));
    }
    let ports: Vec<String> = outputs.iter().map(|o| format!(r#""{o}": "u32""#)).collect();
    let text = format!(
        r#"{{"format": "daedalus.graph", "schema_version": 1, "graph": {{
  "nodes": [{{"id": "host", "kind": "host_bridge", "label": "io", "port_types": {{"n": "u32", {}}}}}, {}],
  "edges": [{}]}}}}"#,
        ports.join(", "),
        nodes.join(", "),
        edges.join(", ")
    );
    GraphDoc::parse(&text).expect("the graph parses").graph
}

/// This process's minor page faults so far.
fn faults() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("/proc/self/stat");
    let after = stat.rsplit_once(')').expect("a stat line").1;
    after
        .split_whitespace()
        .nth(7)
        .expect("minflt")
        .parse()
        .expect("a count")
}

/// Allocations a tick on this thread, and page faults a tick, once warm.
fn steady(graph: &GraphBody, outputs: &[&str]) -> (f64, f64) {
    let mut registry = Registry::new();
    registry
        .register_all(&[&FLOATS_NODE, &COUNT_NODE])
        .expect("the CPU nodes register");
    let mut workers = ProcessBackend::new();
    workers
        .add_worker_declaring(spec(&[]).env("DAEDALUS_FIXTURE_PROBE", "1"), &mut registry)
        .expect("the worker registers");
    let planned = plan(
        graph,
        &registry,
        &[&CpuBackend, &workers],
        &Config::default(),
    )
    .expect("the graph plans");
    let mut host = LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads");
    let mut tick = || {
        host.host()
            .feed_inline("n", &ITEMS.to_le_bytes())
            .expect("a feed");
        host.step().expect("a tick");
        assert!(host.fault_reports().next().is_none(), "no fault");
        for output in outputs {
            let taken = host.host().take_copy::<u32>(output).expect("a take");
            assert_eq!(taken, Some(ITEMS), "{output}");
        }
    };
    for _ in 0..20 {
        tick();
    }
    let before = faults();
    let (_, allocations) = counted(|| {
        for _ in 0..TICKS {
            tick();
        }
    });
    let touched = faults() - before;
    (
        allocations as f64 / TICKS as f64,
        touched as f64 / TICKS as f64,
    )
}

#[test]
fn a_cpu_output_a_worker_reads_costs_nothing_a_tick_once_warm() {
    let (alone, _) = steady(&graph(&[&["steady.count"]], &["y"]), &["y"]);
    // The worker gives back a count, so only the CPU's output crosses: a large value coming back
    // from this fixture is read off its socket, which is another path.
    let through = graph(&[&["fixture.floats_count"]], &["y"]);
    let fan = graph(&[&["steady.count"], &["fixture.floats_count"]], &["y", "z"]);
    for (name, graph, outputs) in [
        ("through the worker", through, &["y"][..]),
        ("beside the worker", fan, &["y", "z"][..]),
    ] {
        let (allocations, touched) = steady(&graph, outputs);
        assert!(
            allocations <= alone,
            "{name}: {allocations} allocations a tick, against {alone} with no worker"
        );
        assert!(touched < 64.0, "{name}: {touched} page faults a tick");
    }
}
