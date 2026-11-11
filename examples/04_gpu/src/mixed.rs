//! A CPU node, a GPU node, and a CPU node again. The planner sees that the three don't share
//! memory, puts an upload before the shader, and has the shader write its result where the
//! next CPU node can read it. The nodes only say what type they take and return.

use std::sync::Arc;

use daedalus::prelude::*;

use crate::{Words, gpu, load, when_ready};

/// On the CPU: the numbers 1 to `count`.
#[node(id = "mixed.ramp", inputs(count), outputs(words))]
fn ramp(count: u32) -> Words {
    Words::new(&(1..=count).collect::<Vec<u32>>())
}

/// On the GPU: each number times three, plus one.
#[node(id = "mixed.affine", inputs(src), outputs(dst), backend = daedalus_wgpu_author(src: Words, dst: Words))]
const AFFINE: &str = "
    @group(0) @binding(0) var<storage, read> src: array<u32>;
    @group(0) @binding(1) var<storage, read_write> dst: array<u32>;
    @compute @workgroup_size(64)
    fn main(@builtin(global_invocation_id) id: vec3<u32>) {
        if id.x < arrayLength(&dst) { dst[id.x] = src[id.x] * 3u + 1u; }
    }
";

/// On the CPU again: their sum.
#[node(id = "mixed.sum", inputs(words), outputs(total))]
fn sum(words: &Words) -> u64 {
    words.to_vec().iter().map(|word| u64::from(*word)).sum()
}

const GRAPH: &str = r#"{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {
    "nodes": [
      {"id": "host", "kind": "host_bridge", "port_types": {"count": "u32", "total": "u64"}},
      {"id": "mixed.ramp"},
      {"id": "mixed.affine"},
      {"id": "mixed.sum"}
    ],
    "edges": [
      {"from": {"node": 0, "port": "count"}, "to": {"node": 1, "port": "count"}},
      {"from": {"node": 1, "port": "words"}, "to": {"node": 2, "port": "src"}},
      {"from": {"node": 2, "port": "dst"}, "to": {"node": 3, "port": "words"}},
      {"from": {"node": 3, "port": "total"}, "to": {"node": 0, "port": "total"}}
    ]
  }
}"#;

/// The whole thing on the CPU.
pub fn on_cpu(count: u32) -> u64 {
    (1..=count).map(|x| u64::from(x * 3 + 1)).sum()
}

/// Through the graph: CPU, GPU, CPU.
pub fn through_the_graph(count: u32) -> Result<u64, String> {
    let gpu = gpu()?;
    let mut loaded = load(GRAPH, &[&RAMP_NODE, &AFFINE_NODE, &SUM_NODE], &gpu)?;
    let host = loaded.host();
    host.feed_typed("count", Arc::new(count))
        .map_err(|error| error.to_string())?;
    // A number is plain data, held in place: `take_copy` reads it.
    when_ready(host, "total", |host, name| {
        host.take_copy::<u64>(name)
            .map_err(|error| error.to_string())
    })
}

/// Runs the graph and checks it against the CPU. Returns the line the example prints.
pub fn run() -> Result<String, String> {
    let count = 5000;
    let total = through_the_graph(count)?;
    if total != on_cpu(count) {
        return Err(format!(
            "the graph gives {total}, and the CPU {}",
            on_cpu(count)
        ));
    }
    Ok(format!(
        "mixed: {count} numbers made on the CPU, scaled on the GPU and summed on the CPU give {total}\n"
    ))
}
