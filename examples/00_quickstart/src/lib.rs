//! The smallest graph: one node, written as a plain function, between a host input and a host
//! output. The host feeds a value, runs one tick, and takes the result.

#![forbid(unsafe_code)]

use std::sync::Arc;

use daedalus::prelude::*;
use daedalus_cpu::CpuBackend;

/// A node is a function. The macro reads its id, its ports and their types from it.
#[node(id = "quickstart.double", inputs(x), outputs(y))]
fn double(x: i64) -> i64 {
    x * 2
}

/// The graph: the host's `x` into the node, the node's `y` back to the host as `doubled`.
const GRAPH: &str = r#"{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {
    "nodes": [
      {"id": "host", "kind": "host_bridge", "port_types": {"x": "i64", "doubled": "i64"}},
      {"id": "quickstart.double"}
    ],
    "edges": [
      {"from": {"node": 0, "port": "x"}, "to": {"node": 1, "port": "x"}},
      {"from": {"node": 1, "port": "y"}, "to": {"node": 0, "port": "doubled"}}
    ]
  }
}"#;

/// Plans the graph, loads it, and runs it once. Returns the line the example prints.
pub fn run() -> String {
    // Every node the program has, in a registry.
    let mut registry = Registry::new();
    registry
        .register_all(&[&DOUBLE_NODE])
        .expect("the node registers");

    // The planner proves the graph against the registry and the backends before anything runs.
    let graph = GraphDoc::parse(GRAPH).expect("the graph parses").graph;
    let planned =
        plan(&graph, &registry, &[&CpuBackend], &Config::default()).expect("the graph plans");

    // A loaded host runs the plan: feed, step, take.
    let mut loaded = LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads");
    let host = loaded.host();
    host.feed_typed("x", Arc::new(21_i64)).expect("a feed");
    host.step().expect("a tick");
    let doubled = host
        .take_copy::<i64>("doubled")
        .expect("a take")
        .expect("a value");
    format!("quickstart: 21 doubled is {doubled}\n")
}
