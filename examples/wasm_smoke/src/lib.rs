//! A fan-out graph (`x + 1` and `x * 2`, summed) under a host, on WebAssembly without threads.
//!
//! On `wasm32-unknown-unknown` this is a module with no imports, and `scripts/wasm-smoke.mjs`
//! calls [`smoke`]. On `wasm32-wasip1` the `daedalus-wasi-smoke` command runs it under
//! `scripts/wasi-smoke.mjs`. The host reads no clock of its own, because the host's clock is what
//! it's given. A panic traps, which fails the run.

use std::sync::Arc;

use daedalus::prelude::{Config, LoadOptions, LoadedHost, plan};
use daedalus_cpu::CpuBackend;
use daedalus_graph::GraphDoc;
use daedalus_macros::node;
use daedalus_registry::{NodeRecord, Registry};

#[node(id = "smoke.inc", inputs(x), outputs(y))]
fn inc(x: i64) -> i64 {
    x + 1
}

#[node(id = "smoke.double", inputs(x), outputs(y))]
fn double(x: i64) -> i64 {
    x * 2
}

#[node(id = "smoke.add", inputs(a, b), outputs(sum))]
fn add(a: i64, b: i64) -> i64 {
    a + b
}

const NODES: [&NodeRecord; 3] = [&INC_NODE, &DOUBLE_NODE, &ADD_NODE];

const GRAPH: &str = r#"{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {
    "nodes": [
      {"id": "host", "kind": "host_bridge", "port_types": {"x": "i64", "sum": "i64"}},
      {"id": "smoke.inc"},
      {"id": "smoke.double"},
      {"id": "smoke.add"}
    ],
    "edges": [
      {"from": {"node": 0, "port": "x"}, "to": {"node": 1, "port": "x"}},
      {"from": {"node": 0, "port": "x"}, "to": {"node": 2, "port": "x"}},
      {"from": {"node": 1, "port": "y"}, "to": {"node": 3, "port": "a"}},
      {"from": {"node": 2, "port": "y"}, "to": {"node": 3, "port": "b"}},
      {"from": {"node": 3, "port": "sum"}, "to": {"node": 0, "port": "sum"}}
    ]
  }
}"#;

/// The graph's outputs summed over 32 ticks fed `0..32`. It should be 1520.
pub fn sum() -> i64 {
    let graph = GraphDoc::parse(GRAPH).expect("the graph parses").graph;
    let mut registry = Registry::new();
    registry.register_all(&NODES).expect("the nodes register");
    let planned =
        plan(&graph, &registry, &[&CpuBackend], &Config::default()).expect("the graph plans");
    let mut loaded = LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads");
    let host = loaded.host();
    (0..32_i64)
        .map(|x| {
            host.feed_typed("x", Arc::new(x)).expect("a feed");
            host.step().expect("a tick");
            host.take_copy::<i64>("sum")
                .expect("a take")
                .expect("a sum")
        })
        .sum()
}

// SAFETY: this is the module's only export under this name, and it takes nothing.
#[unsafe(no_mangle)]
pub extern "C" fn smoke() -> i32 {
    sum() as i32
}
