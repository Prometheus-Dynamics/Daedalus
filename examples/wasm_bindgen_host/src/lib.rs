//! A small graph (`y = gain * x + offset`) driven from JavaScript through [`Pipeline`]'s `push`,
//! `tick` and `take`, on the host's `performance.now()` clock.
//!
//! `scripts/wasm-bindgen-host.mjs` is the driver.

use std::sync::Arc;

use daedalus::prelude::{Config, LoadOptions, LoadedHost, plan};
use daedalus_cpu::CpuBackend;
use daedalus_graph::GraphDoc;
use daedalus_macros::node;
use daedalus_registry::{NodeRecord, Registry};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    /// The host's monotonic clock, in milliseconds.
    #[wasm_bindgen(js_namespace = performance, js_name = now)]
    fn performance_now() -> f64;
}

#[node(id = "host.scale", inputs(x, gain), outputs(y))]
fn scale(x: f64, gain: f64) -> f64 {
    x * gain
}

#[node(id = "host.offset", inputs(x, offset), outputs(y))]
fn shifted(x: f64, offset: f64) -> f64 {
    x + offset
}

const NODES: [&NodeRecord; 2] = [&SCALE_NODE, &SHIFTED_NODE];

fn graph_json(gain: f64, offset: f64) -> String {
    format!(
        r#"{{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {{
    "nodes": [
      {{"id": "host", "kind": "host_bridge", "port_types": {{"x": "f64", "y": "f64"}}}},
      {{"id": "host.scale", "consts": {{"gain": {{"type": "float", "value": {gain:?}}}}}}},
      {{"id": "host.offset", "consts": {{"offset": {{"type": "float", "value": {offset:?}}}}}}}
    ],
    "edges": [
      {{"from": {{"node": 0, "port": "x"}}, "to": {{"node": 1, "port": "x"}}}},
      {{"from": {{"node": 1, "port": "y"}}, "to": {{"node": 2, "port": "x"}}}},
      {{"from": {{"node": 2, "port": "y"}}, "to": {{"node": 0, "port": "y"}}}}
    ]
  }}
}}"#
    )
}

fn js_error(error: impl core::fmt::Debug) -> JsError {
    JsError::new(&format!("{error:?}"))
}

/// The planned graph, driven from JavaScript.
#[wasm_bindgen]
pub struct Pipeline {
    loaded: LoadedHost,
}

#[wasm_bindgen]
impl Pipeline {
    /// Plans the graph with these constants.
    #[wasm_bindgen(constructor)]
    pub fn new(gain: f64, offset: f64) -> Result<Pipeline, JsError> {
        let graph = GraphDoc::parse(&graph_json(gain, offset))
            .map_err(js_error)?
            .graph;
        let mut registry = Registry::new();
        registry.register_all(&NODES).map_err(js_error)?;
        let planned =
            plan(&graph, &registry, &[&CpuBackend], &Config::default()).map_err(js_error)?;
        let loaded = LoadedHost::load(planned, LoadOptions::default()).map_err(js_error)?;
        Ok(Pipeline { loaded })
    }

    /// Whether the value was accepted.
    pub fn push(&mut self, x: f64) -> bool {
        self.loaded.host().feed_typed("x", Arc::new(x)).is_ok()
    }

    /// Runs the graph once, and returns how long it took on the host's clock, in milliseconds.
    pub fn tick(&mut self) -> Result<f64, JsError> {
        let start = performance_now();
        let host = self.loaded.host();
        host.set_clock((start * 1e6) as u64);
        host.step().map_err(js_error)?;
        Ok(performance_now() - start)
    }

    /// The next output, if there is one.
    pub fn take(&mut self) -> Option<f64> {
        self.loaded.host().take_copy::<f64>("y").ok().flatten()
    }
}
