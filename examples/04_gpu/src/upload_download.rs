//! The round trip: the host hands the graph words, one shader node runs on them on the device,
//! and the host takes the result. The planner puts the upload before the node and the readback
//! after it, since the host's memory isn't the device's.

use std::sync::Arc;

use crate::{Words, gpu, load, take_when_ready};
use daedalus::prelude::*;

/// A shader node. The macro reads its bindings from the WGSL: the port `src` feeds the binding
/// `src`, and the node's output is the buffer `dst`.
#[node(id = "gpu.square", inputs(src), outputs(dst), backend = daedalus_wgpu_author(src: Words, dst: Words))]
const SQUARE: &str = "
    @group(0) @binding(0) var<storage, read> src: array<u32>;
    @group(0) @binding(1) var<storage, read_write> dst: array<u32>;
    @compute @workgroup_size(64)
    fn main(@builtin(global_invocation_id) id: vec3<u32>) {
        if id.x < arrayLength(&dst) { dst[id.x] = src[id.x] * src[id.x]; }
    }
";

const GRAPH: &str = r#"{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {
    "nodes": [
      {"id": "host", "kind": "host_bridge", "port_types": {"words": "gpu:words", "squared": "gpu:words"}},
      {"id": "gpu.square"}
    ],
    "edges": [
      {"from": {"node": 0, "port": "words"}, "to": {"node": 1, "port": "src"}},
      {"from": {"node": 1, "port": "dst"}, "to": {"node": 0, "port": "squared"}}
    ]
  }
}"#;

/// The transfers the planner chose for the squared values' way back to the host, planned on the
/// open `gpu` the way the round trip plans.
pub fn readback_route(gpu: &daedalus_wgpu::WgpuBackend) -> Result<Vec<String>, String> {
    let planned = super::planned(GRAPH, &[&SQUARE_NODE], gpu)?;
    let routing = &planned.regions.routing;
    let output = routing
        .host_outputs
        .iter()
        .find(|output| output.port == "squared")
        .ok_or("the squared output has no host route")?;
    Ok(output
        .reader
        .path
        .iter()
        .map(|&step| routing.table[step].id.clone())
        .collect())
}

/// Squares `input` on the GPU and returns what came back.
pub fn squared_on_gpu(input: &[u32]) -> Result<Vec<u32>, String> {
    let gpu = gpu()?;
    let mut loaded = load(GRAPH, &[&SQUARE_NODE], &gpu)?;
    let host = loaded.host();
    host.feed_typed("words", Arc::new(Words::new(input)))
        .map_err(|error| error.to_string())?;
    take_when_ready(host, "squared").map(|words| words.to_vec())
}

/// Runs the round trip and checks it against the CPU. Returns the line the example prints.
pub fn run() -> Result<String, String> {
    let input: Vec<u32> = (1..=1000).collect();
    let on_gpu = squared_on_gpu(&input)?;
    let on_cpu: Vec<u32> = input.iter().map(|x| x * x).collect();
    if on_gpu != on_cpu {
        return Err("the GPU's squares aren't the CPU's".into());
    }
    Ok(format!(
        "upload_download: {} words squared on the GPU match the CPU, the last is {}\n",
        on_gpu.len(),
        on_gpu[on_gpu.len() - 1]
    ))
}
