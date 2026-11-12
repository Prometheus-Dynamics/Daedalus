//! Element-wise functions. Each node is one WGSL function of the element it is given, so a chain
//! of them is merged into a single shader: one pass over the buffer, with nothing written between
//! the nodes.

use std::sync::Arc;

use daedalus::prelude::*;

use crate::{Words, gpu, load, take_when_ready};

/// An element body is `fn apply`, and whatever it calls. It can't see any other element.
#[node(id = "gpu.flip", inputs(c), outputs(out), backend = daedalus_wgpu_author(element, c: Words, out: Words))]
const FLIP: &str = "fn apply(c: u32) -> u32 { return 255u - (c & 255u); }";

#[node(id = "gpu.offset", inputs(c), outputs(out), backend = daedalus_wgpu_author(element, c: Words, out: Words))]
const OFFSET: &str = "fn apply(c: u32) -> u32 { return min(c + 40u, 255u); }";

#[node(id = "gpu.level", inputs(c), outputs(out), backend = daedalus_wgpu_author(element, c: Words, out: Words))]
const LEVEL: &str = "fn apply(c: u32) -> u32 { return select(0u, 255u, c >= 128u); }";

const GRAPH: &str = r#"{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {
    "nodes": [
      {"id": "host", "kind": "host_bridge", "port_types": {"values": "gpu:words", "result": "gpu:words"}},
      {"id": "gpu.flip"},
      {"id": "gpu.offset"},
      {"id": "gpu.level"}
    ],
    "edges": [
      {"from": {"node": 0, "port": "values"}, "to": {"node": 1, "port": "c"}},
      {"from": {"node": 1, "port": "out"}, "to": {"node": 2, "port": "c"}},
      {"from": {"node": 2, "port": "out"}, "to": {"node": 3, "port": "c"}},
      {"from": {"node": 3, "port": "out"}, "to": {"node": 0, "port": "result"}}
    ]
  }
}"#;

/// The same three functions, on the CPU.
pub fn on_cpu(values: &[u32]) -> Vec<u32> {
    values
        .iter()
        .map(|c| 255 - (c & 255))
        .map(|c| (c + 40).min(255))
        .map(|c| if c >= 128 { 255 } else { 0 })
        .collect()
}

/// Runs the chain on the GPU. Returns the result, and how many times a shader ran for it.
pub fn on_gpu(values: &[u32]) -> Result<(Vec<u32>, u64), String> {
    let gpu = gpu()?;
    let mut loaded = load(GRAPH, &[&FLIP_NODE, &OFFSET_NODE, &LEVEL_NODE], &gpu)?;
    let passes =
        |gpu: &daedalus_wgpu::WgpuBackend| gpu.shader_stats().map_or(0, |stats| stats.dispatches);
    let before = passes(&gpu);
    let host = loaded.host();
    host.feed_typed("values", Arc::new(Words::new(values)))
        .map_err(|error| error.to_string())?;
    let result = take_when_ready(host, "result")?.to_vec();
    Ok((result, passes(&gpu) - before))
}

/// Runs the chain and checks it against the CPU. Returns the line the example prints.
pub fn run() -> Result<String, String> {
    let values: Vec<u32> = (0..64 * 64).map(|at| (at * 7) % 256).collect();
    let (result, passes) = on_gpu(&values)?;
    if result != on_cpu(&values) {
        return Err("the GPU's result isn't the CPU's".into());
    }
    if passes != 1 {
        return Err(format!(
            "three element-wise nodes ran as {passes} shaders, not one"
        ));
    }
    let at_top = result.iter().filter(|c| **c == 255).count();
    Ok(format!(
        "element: three element-wise nodes ran as {passes} shader over {} elements, {at_top} are 255, as on the CPU\n",
        result.len()
    ))
}
