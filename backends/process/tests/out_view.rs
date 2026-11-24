//! A CPU node that writes its output through an `OutView` writes it in the worker's lent memory:
//! the worker reads it where it was written, so nothing is copied on the way, and once warm a tick
//! allocates nothing, with a CPU reader beside the worker or not.
//!
//! In its own binary, so no other test's allocations are counted. Lent memory is Linux's.
#![cfg(target_os = "linux")]

use crate::support;

use daedalus::layers::planner::explain::explain_plan;
use daedalus::prelude::*;
use daedalus_abi::counting::counted;
use daedalus_backend_process::ProcessBackend;
use daedalus_cpu::CpuBackend;
use daedalus_cpu_author::{OutView, Refused};
use support::spec;

/// 1 MiB of `f32`.
const ITEMS: u32 = 1 << 18;
const TICKS: usize = 100;

#[node(id = "view.floats", inputs(n))]
pub fn floats(n: u32, mut out: OutView<'_, f32>) -> Result<(), Refused> {
    for (at, item) in out.items_mut(n as usize)?.iter_mut().enumerate() {
        *item = at as f32;
    }
    Ok(())
}

#[node(id = "view.count", inputs(items), outputs(y))]
pub fn count(items: &[f32]) -> u32 {
    items.len() as u32
}

/// host.n → `view.floats` → the worker's count → host.y, and with `beside` a CPU count of the
/// same list → host.z as well.
fn graph(beside: bool) -> GraphBody {
    let mut nodes = r#"{"id": "view.floats"}, {"id": "fixture.floats_count"}"#.to_owned();
    let mut edges = r#"{"from": {"node": 0, "port": "n"}, "to": {"node": 1, "port": "n"}},
        {"from": {"node": 1, "port": "out"}, "to": {"node": 2, "port": "in"}},
        {"from": {"node": 2, "port": "count"}, "to": {"node": 0, "port": "y"}}"#
        .to_owned();
    let mut ports = r#""n": "u32", "y": "u32""#.to_owned();
    if beside {
        nodes.push_str(r#", {"id": "view.count"}"#);
        edges.push_str(
            r#", {"from": {"node": 1, "port": "out"}, "to": {"node": 3, "port": "items"}},
            {"from": {"node": 3, "port": "y"}, "to": {"node": 0, "port": "z"}}"#,
        );
        ports.push_str(r#", "z": "u32""#);
    }
    let text = format!(
        r#"{{"format": "daedalus.graph", "schema_version": 1, "graph": {{
  "nodes": [{{"id": "host", "kind": "host_bridge", "label": "io", "port_types": {{{ports}}}}}, {nodes}],
  "edges": [{edges}]}}}}"#
    );
    GraphDoc::parse(&text).expect("the graph parses").graph
}

fn planned(graph: &GraphBody, workers: &mut ProcessBackend) -> Planned {
    let mut registry = Registry::new();
    registry
        .register_all(&[&FLOATS_NODE, &COUNT_NODE])
        .expect("the CPU nodes register");
    workers
        .add_worker_declaring(spec(&[]).env("DAEDALUS_FIXTURE_PROBE", "1"), &mut registry)
        .expect("the worker registers");
    plan(
        graph,
        &registry,
        &[&CpuBackend, &*workers],
        &Config::default(),
    )
    .expect("the graph plans")
}

#[test]
fn the_view_is_written_where_the_worker_reads_it_and_a_tick_allocates_nothing() {
    for beside in [false, true] {
        let mut workers = ProcessBackend::new();
        let planned = planned(&graph(beside), &mut workers);
        let explain = explain_plan(&planned);
        let to_worker = explain
            .edges
            .iter()
            .find(|edge| edge.from_node == 1 && edge.to_node == 2)
            .expect("the edge to the worker");
        assert_eq!(to_worker.written_in.as_deref(), Some("process.lent"));
        assert!(to_worker.transfer.is_empty(), "{:?}", to_worker.transfer);
        // The CPU reader views the numbers where they are: nothing is decoded.
        assert!(explain.decodes.is_empty(), "{:?}", explain.decodes);
        let outputs: &[&str] = if beside { &["y", "z"] } else { &["y"] };
        let mut host = LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads");
        let mut tick = || {
            host.host()
                .feed_inline("n", &ITEMS.to_le_bytes())
                .expect("a feed");
            host.step().expect("a tick");
            let mut good = host.fault_reports().next().is_none();
            for output in outputs {
                good &= host.host().take_copy::<u32>(output).expect("a take") == Some(ITEMS);
            }
            good
        };
        for _ in 0..20 {
            assert!(tick(), "beside = {beside}: a warm-up tick");
        }
        let before = workers.telemetry();
        let mut good = 0;
        let ((), allocations) = counted(|| {
            for _ in 0..TICKS {
                good += usize::from(tick());
            }
        });
        let after = workers.telemetry();
        assert_eq!(
            good, TICKS,
            "beside = {beside}: every tick counts every item"
        );
        assert_eq!(
            allocations, 0,
            "beside = {beside}: a warm tick allocates nothing"
        );
        let passed = after.passed_in - before.passed_in;
        let placed = after.placed_in - before.placed_in;
        assert_eq!(
            passed, TICKS as u64,
            "beside = {beside}: each tick passed as it was"
        );
        assert_eq!(
            placed, passed,
            "beside = {beside}: none copied into lent memory"
        );
    }
}
