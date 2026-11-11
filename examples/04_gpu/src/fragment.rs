//! Per-pixel functions. Each node is one WGSL function of the pixel it is given, so a chain of
//! them is merged into a single shader: one pass over the buffer, with nothing written between
//! the nodes.

use std::sync::Arc;

use daedalus::prelude::*;

use crate::{Words, gpu, load, take_when_ready};

/// A pixel body is `fn apply`, and whatever it calls. It can't see any other pixel.
#[node(id = "gpu.invert", inputs(c), outputs(out), backend = daedalus_wgpu_author(pixel, c: Words, out: Words))]
const INVERT: &str = "fn apply(c: u32) -> u32 { return 255u - (c & 255u); }";

#[node(id = "gpu.brighten", inputs(c), outputs(out), backend = daedalus_wgpu_author(pixel, c: Words, out: Words))]
const BRIGHTEN: &str = "fn apply(c: u32) -> u32 { return min(c + 40u, 255u); }";

#[node(id = "gpu.threshold", inputs(c), outputs(out), backend = daedalus_wgpu_author(pixel, c: Words, out: Words))]
const THRESHOLD: &str = "fn apply(c: u32) -> u32 { return select(0u, 255u, c >= 128u); }";

const GRAPH: &str = r#"{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {
    "nodes": [
      {"id": "host", "kind": "host_bridge", "port_types": {"pixels": "gpu:words", "mask": "gpu:words"}},
      {"id": "gpu.invert"},
      {"id": "gpu.brighten"},
      {"id": "gpu.threshold"}
    ],
    "edges": [
      {"from": {"node": 0, "port": "pixels"}, "to": {"node": 1, "port": "c"}},
      {"from": {"node": 1, "port": "out"}, "to": {"node": 2, "port": "c"}},
      {"from": {"node": 2, "port": "out"}, "to": {"node": 3, "port": "c"}},
      {"from": {"node": 3, "port": "out"}, "to": {"node": 0, "port": "mask"}}
    ]
  }
}"#;

/// The same three functions, on the CPU.
pub fn on_cpu(pixels: &[u32]) -> Vec<u32> {
    pixels
        .iter()
        .map(|c| 255 - (c & 255))
        .map(|c| (c + 40).min(255))
        .map(|c| if c >= 128 { 255 } else { 0 })
        .collect()
}

/// Runs the chain on the GPU. Returns the mask, and how many times a shader ran for it.
pub fn on_gpu(pixels: &[u32]) -> Result<(Vec<u32>, u64), String> {
    let gpu = gpu()?;
    let mut loaded = load(
        GRAPH,
        &[&INVERT_NODE, &BRIGHTEN_NODE, &THRESHOLD_NODE],
        &gpu,
    )?;
    let passes =
        |gpu: &daedalus_wgpu::WgpuBackend| gpu.shader_stats().map_or(0, |stats| stats.dispatches);
    let before = passes(&gpu);
    let host = loaded.host();
    host.feed_typed("pixels", Arc::new(Words::new(pixels)))
        .map_err(|error| error.to_string())?;
    let mask = take_when_ready(host, "mask")?.to_vec();
    Ok((mask, passes(&gpu) - before))
}

/// Runs the chain and checks it against the CPU. Returns the line the example prints.
pub fn run() -> Result<String, String> {
    let pixels: Vec<u32> = (0..64 * 64).map(|at| (at * 7) % 256).collect();
    let (mask, passes) = on_gpu(&pixels)?;
    if mask != on_cpu(&pixels) {
        return Err("the GPU's mask isn't the CPU's".into());
    }
    if passes != 1 {
        return Err(format!(
            "three per-pixel nodes ran as {passes} shaders, not one"
        ));
    }
    let lit = mask.iter().filter(|c| **c == 255).count();
    Ok(format!(
        "fragment: three per-pixel nodes ran as {passes} shader over {} pixels, {lit} are lit, as on the CPU\n",
        mask.len()
    ))
}
