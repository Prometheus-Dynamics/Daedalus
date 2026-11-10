//! A host in a loop: feed, step, drain. In the middle the graph is reloaded with one node
//! replaced. The node that did not change keeps its state, and the report says which node
//! started fresh.
//!
//! ```text
//! host.x --> shape --> total --> host.sum
//! ```
//!
//! `total` keeps a running sum. `shape` doubles its input before the reload and negates it after.
//!
//! A node's state is in its step, and a reload keeps the steps that did not change. The CPU
//! backend would merge these two nodes into one step, which would then change as a whole, so the
//! graph asks for `total` to stay a step of its own.

#![forbid(unsafe_code)]

use std::sync::Arc;

use daedalus::prelude::*;
use daedalus_cpu::CpuBackend;

#[node(id = "host.double", inputs(x), outputs(y))]
fn double(x: i64) -> i64 {
    x * 2
}

#[node(id = "host.negate", inputs(x), outputs(y))]
fn negate(x: i64) -> i64 {
    -x
}

/// A node's state lives between ticks, and is the node's alone.
#[derive(Default)]
pub struct Total(i64);

#[node(id = "host.total", inputs(x), outputs(sum), state(Total))]
fn total(x: i64, state: &mut Total) -> i64 {
    state.0 += x;
    state.0
}

const NODES: [&NodeRecord; 3] = [&DOUBLE_NODE, &NEGATE_NODE, &TOTAL_NODE];

/// The graph, with `shape` as the node id of the first stage. A label names a node across
/// reloads: the host matches nodes by it.
fn graph(shape: &str) -> GraphBody {
    let text = format!(
        r#"{{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {{
    "nodes": [
      {{"id": "host", "kind": "host_bridge", "label": "io", "port_types": {{"x": "i64", "sum": "i64"}}}},
      {{"id": "{shape}", "label": "shape"}},
      {{"id": "host.total", "label": "total", "metadata": {{"daedalus.node.fusion": {{"type": "bool", "value": false}}}}}}
    ],
    "edges": [
      {{"from": {{"node": 0, "port": "x"}}, "to": {{"node": 1, "port": "x"}}}},
      {{"from": {{"node": 1, "port": "y"}}, "to": {{"node": 2, "port": "x"}}}},
      {{"from": {{"node": 2, "port": "sum"}}, "to": {{"node": 0, "port": "sum"}}}}
    ]
  }}
}}"#
    );
    GraphDoc::parse(&text).expect("the graph parses").graph
}

fn planned(shape: &str) -> Planned {
    let mut registry = Registry::new();
    registry.register_all(&NODES).expect("the nodes register");
    plan(&graph(shape), &registry, &[&CpuBackend], &Config::default()).expect("the graph plans")
}

/// The graph loaded, with the doubling stage.
pub fn load() -> LoadedHost {
    LoadedHost::load(planned("host.double"), LoadOptions::default()).expect("the plan loads")
}

/// Feeds each value, steps once for each, and drains what came out.
pub fn feed_step_drain(host: &mut LoadedHost, values: &[i64]) -> Vec<i64> {
    let mut out = Vec::new();
    for value in values {
        let io = host.host();
        io.feed_typed("x", Arc::new(*value)).expect("a feed");
        io.step().expect("a tick");
        while let Some(sum) = io.take_copy::<i64>("sum").expect("a take") {
            out.push(sum);
        }
    }
    out
}

/// Replaces the graph with one whose first stage negates. Returns what the reload did.
pub fn reload(host: &mut LoadedHost) -> ReloadReport {
    host.reload(planned("host.negate"))
        .expect("the reload fits")
}

/// Runs the example. Returns the lines it prints.
pub fn run() -> String {
    let mut host = load();
    let mut out = String::new();
    let before = feed_step_drain(&mut host, &[1, 2, 3]);
    out.push_str(&format!("host: doubling, the totals are {before:?}\n"));

    let report = reload(&mut host);
    out.push_str(&format!(
        "host: reloaded: kept the state of {:?}, started {:?} fresh, added {}, removed {}\n",
        report.carried.names(),
        report.fresh.names(),
        report.added.total(),
        report.removed.total(),
    ));

    // The total goes on from 12, now with each input negated.
    let after = feed_step_drain(&mut host, &[1, 2, 3]);
    out.push_str(&format!("host: negating, the totals are {after:?}\n"));
    out
}
