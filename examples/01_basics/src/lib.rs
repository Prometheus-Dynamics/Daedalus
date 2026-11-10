//! Typed ports, constants, a config struct, one fan-out and one fan-in.
//!
//! ```text
//!            +--> scale (x * factor) --+
//! host.x ----+                         +--> sum (every input) --> clamp (range) --> host.out
//!            +--> offset (x + by) -----+
//! ```
//!
//! `factor` and `by` are constants the graph sets on those nodes. `clamp` takes its limits as a
//! config struct: the graph sets `high`, and `low` keeps its default. The planner checks every type and every
//! constant before anything runs, and the example shows a graph it refuses.

#![forbid(unsafe_code)]

use std::sync::Arc;

use daedalus::prelude::*;
use daedalus_cpu::CpuBackend;
use daedalus_cpu_author::FanIn;

/// A port's type is the parameter's type. A parameter no edge feeds can be a constant.
#[node(id = "basics.scale", inputs(x, factor), outputs(y))]
fn scale(x: f64, factor: f64) -> f64 {
    x * factor
}

#[node(id = "basics.offset", inputs(x, by), outputs(y))]
fn offset(x: f64, by: f64) -> f64 {
    x + by
}

/// A fan-in takes any number of edges, on ports numbered `items0`, `items1` and so on.
#[node(id = "basics.sum", inputs(items), outputs(total))]
fn sum(items: FanIn<'_, f64>) -> f64 {
    items.iter().map(|item| *item).sum()
}

/// A config struct: named settings with defaults and limits, set by the graph, not by an edge.
#[derive(Clone, Default, NodeConfig, Type)]
#[type_key("basics:range")]
pub struct Range {
    #[port(default = 0, min = -1000, max = 1000)]
    pub low: i64,
    #[port(default = 100, min = -1000, max = 1000)]
    pub high: i64,
}

#[node(id = "basics.clamp", inputs(x, config = Range), outputs(y))]
fn clamp(x: f64, range: &Range) -> f64 {
    x.clamp(range.low as f64, range.high as f64)
}

const NODES: [&NodeRecord; 4] = [&SCALE_NODE, &OFFSET_NODE, &SUM_NODE, &CLAMP_NODE];

/// The graph, with `high` as the upper limit `clamp` is given.
fn graph(high: &str) -> String {
    format!(
        r#"{{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {{
    "nodes": [
      {{"id": "host", "kind": "host_bridge", "port_types": {{"x": "f64", "out": "f64"}}}},
      {{"id": "basics.scale", "consts": {{"factor": {{"type": "float", "value": 3.0}}}}}},
      {{"id": "basics.offset", "consts": {{"by": {{"type": "float", "value": 10.0}}}}}},
      {{"id": "basics.sum"}},
      {{"id": "basics.clamp", "consts": {{"config": {{"type": "struct", "value": [
        {{"name": "high", "value": {high}}}
      ]}}}}}}
    ],
    "edges": [
      {{"from": {{"node": 0, "port": "x"}}, "to": {{"node": 1, "port": "x"}}}},
      {{"from": {{"node": 0, "port": "x"}}, "to": {{"node": 2, "port": "x"}}}},
      {{"from": {{"node": 1, "port": "y"}}, "to": {{"node": 3, "port": "items0"}}}},
      {{"from": {{"node": 2, "port": "y"}}, "to": {{"node": 3, "port": "items1"}}}},
      {{"from": {{"node": 3, "port": "total"}}, "to": {{"node": 4, "port": "x"}}}},
      {{"from": {{"node": 4, "port": "y"}}, "to": {{"node": 0, "port": "out"}}}}
    ]
  }}
}}"#
    )
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    registry.register_all(&NODES).expect("the nodes register");
    registry
}

/// One value through the graph.
pub fn through(host: &mut LoadedHost, x: f64) -> f64 {
    let host = host.host();
    host.feed_typed("x", Arc::new(x)).expect("a feed");
    host.step().expect("a tick");
    host.take_copy::<f64>("out")
        .expect("a take")
        .expect("a value")
}

/// The graph loaded, with 40 as the upper limit.
pub fn load() -> LoadedHost {
    let graph = GraphDoc::parse(&graph(r#"{"type": "int", "value": 40}"#))
        .expect("the graph parses")
        .graph;
    let planned =
        plan(&graph, &registry(), &[&CpuBackend], &Config::default()).expect("the graph plans");
    LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads")
}

/// What the planner says about a graph whose `high` is not a number.
pub fn refused() -> String {
    let graph = GraphDoc::parse(&graph(r#"{"type": "str", "value": "plenty"}"#))
        .expect("the graph parses")
        .graph;
    match plan(&graph, &registry(), &[&CpuBackend], &Config::default()) {
        Ok(_) => "planned".to_string(),
        Err(error) => error.to_string(),
    }
}

/// Runs the example. Returns the lines it prints.
pub fn run() -> String {
    let mut host = load();
    let mut out = String::new();
    for x in [4.0, 20.0] {
        // 4 -> 12 and 14 -> 26. 20 -> 60 and 30 -> 90, which the clamp cuts to 40.
        out.push_str(&format!(
            "basics: x = {x} gives {}\n",
            through(&mut host, x)
        ));
    }
    out.push_str(&format!("basics: a text limit is refused: {}\n", refused()));
    out
}
