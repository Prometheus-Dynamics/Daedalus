//! A dma-buf handed from a CPU node to a shader node, with the planner choosing the route. The CPU
//! node makes its frame in dma-heap memory; the shader reads it. Where the device imports
//! dma-bufs, the planner imports the frame in place and nothing is uploaded. Where it doesn't,
//! the planner uploads the frame's bytes, and the shader reads the same numbers.

use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::Arc;

use daedalus::prelude::*;
use daedalus_wgpu::{DmaBufBacked, HostBytes};

use crate::{Words, gpu, load, when_ready};

/// Room a frame's dma-heap buffer has past its bytes: two pages, which covers the device's padding.
const HEAP_ROOM: u64 = 8192;

/// A frame: bytes that live in a dma-heap buffer. The buffer is the frame's storage; the bytes
/// are kept too, for a device that has to upload them.
#[derive(Debug)]
pub struct Frame {
    bytes: Vec<u8>,
    /// The dma-buf, when the system dma-heap opens. Without one the frame is bytes only.
    dmabuf: Option<OwnedFd>,
}

/// A frame is a named type with no structure of its own: the derive would need the fd to be
/// `Typed`, which it isn't, so the key is written out.
impl daedalus_types::Typed for Frame {
    const KEY: daedalus_types::TypeKey = daedalus_types::TypeKey::new("gpu:frame");
    const EXPR: daedalus_types::TypeExpr =
        daedalus_types::TypeExpr::Opaque(daedalus_types::TypeKey::new("gpu:frame"));
}

impl Frame {
    /// A frame of `bytes`, written into a dma-heap buffer of its own.
    pub fn heap(bytes: &[u8]) -> Result<Frame, String> {
        // The device pads a buffer's memory by up to a page and 256 bytes (V3D), so the dma-buf is
        // made with room for that. This node has no device to ask what it needs, and the frame's
        // length is kept apart from the dma-buf's.
        // A machine with no usable dma-heap (a CI runner) makes the frame as bytes only: the planner
        // then uploads it, and never imports it.
        Ok(Frame {
            bytes: bytes.to_vec(),
            dmabuf: daedalus_wgpu::dma_heap_fd(bytes, HEAP_ROOM).ok(),
        })
    }
}

/// The frame's bytes, as the byte copy a device that can't import gets.
impl HostBytes for Frame {
    fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// A frame from bytes the device read back. The examples never read a frame back, so this
    /// needs a dma-heap, as `heap` does.
    fn from_bytes(bytes: Vec<u8>) -> Frame {
        Frame::heap(&bytes).expect("a dma-heap buffer for a frame read back")
    }
}

/// The frame's dma-buf, which the device imports in place where it can.
impl DmaBufBacked for Frame {
    fn dmabuf(&self) -> Option<(BorrowedFd<'_>, u64)> {
        self.dmabuf
            .as_ref()
            .map(|fd| (fd.as_fd(), self.bytes.len() as u64))
    }
}

/// On the CPU: a frame of `count` bytes, numbered from 3 in steps of 7.
#[node(id = "frame.make", inputs(count), outputs(frame))]
fn make(count: u32) -> Frame {
    let bytes: Vec<u8> = (0..count).map(|at| (at * 7 + 3) as u8).collect();
    Frame::heap(&bytes).expect("a dma-heap frame")
}

/// On the GPU: each byte of the frame, read as a word, plus one.
#[node(id = "frame.plus_one", inputs(src), outputs(dst), backend = daedalus_wgpu_author(src: Frame, dst: Words))]
const PLUS_ONE: &str = "
    @group(0) @binding(0) var<storage, read> src: array<u32>;
    @group(0) @binding(1) var<storage, read_write> dst: array<u32>;
    @compute @workgroup_size(64)
    fn main(@builtin(global_invocation_id) id: vec3<u32>) {
        if id.x < arrayLength(&dst) { dst[id.x] = src[id.x] + 1u; }
    }
";

const GRAPH: &str = r#"{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {
    "nodes": [
      {"id": "host", "kind": "host_bridge", "port_types": {"count": "u32", "words": "gpu:words"}},
      {"id": "frame.make"},
      {"id": "frame.plus_one"}
    ],
    "edges": [
      {"from": {"node": 0, "port": "count"}, "to": {"node": 1, "port": "count"}},
      {"from": {"node": 1, "port": "frame"}, "to": {"node": 2, "port": "src"}},
      {"from": {"node": 2, "port": "dst"}, "to": {"node": 0, "port": "words"}}
    ]
  }
}"#;

/// The words the shader gives for a frame of `count` bytes: each four bytes of the frame read as a
/// word, plus one.
pub fn on_cpu(count: u32) -> Vec<u32> {
    let bytes: Vec<u8> = (0..count).map(|at| (at * 7 + 3) as u8).collect();
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|word| u32::from_le_bytes(*word).wrapping_add(1))
        .collect()
}

/// The route the planner chose for the frame's way into the shader: the transfers on it, by id.
/// The frame's type key is in each id, so only that route is listed.
pub fn frame_route(gpu: &daedalus_wgpu::WgpuBackend) -> Result<Vec<String>, String> {
    let planned = super::planned(GRAPH, &[&MAKE_NODE, &PLUS_ONE_NODE], gpu)?;
    let routing = &planned.regions.routing;
    let mut ids = Vec::new();
    for reader in routing.readers.values() {
        for &step in &reader.path {
            let id = &routing.table[step].id;
            if id.contains("gpu:frame") && !ids.contains(id) {
                ids.push(id.clone());
            }
        }
    }
    Ok(ids)
}

/// Makes a frame of `count` bytes on the CPU, runs it through the shader on the GPU, and returns
/// the words that came back, with the transfers the frame went through.
pub fn through_the_graph(count: u32) -> Result<(Vec<u32>, Vec<String>), String> {
    through_the_graph_on(&gpu()?, count)
}

/// A device that has only the byte moves for a frame: no import is offered, so the frame is
/// uploaded. Opened the way `gpu` is, and checked the same way.
pub fn gpu_bytes_only() -> Result<daedalus_wgpu::WgpuBackend, String> {
    let backend = daedalus_wgpu::WgpuBackend::open(daedalus_wgpu::Options::default())
        .with_bytes::<Words>()
        .with_bytes::<Frame>();
    backend
        .start()
        .map_err(|error| format!("this example needs a GPU: {error}"))?;
    Ok(backend)
}

/// [`through_the_graph`] on the device `gpu`.
pub fn through_the_graph_on(
    gpu: &daedalus_wgpu::WgpuBackend,
    count: u32,
) -> Result<(Vec<u32>, Vec<String>), String> {
    let route = frame_route(gpu)?;
    let mut loaded = load(GRAPH, &[&MAKE_NODE, &PLUS_ONE_NODE], gpu)?;
    let host = loaded.host();
    host.feed_typed("count", Arc::new(count))
        .map_err(|error| error.to_string())?;
    let words = match when_ready(host, "words", |host, name| {
        host.take::<Words>(name).map_err(|error| error.to_string())
    }) {
        Ok(words) => words,
        Err(error) => {
            // Say which steps failed, and why, so a run that gets no words can be read.
            let faults: Vec<String> = loaded
                .fault_reports()
                .map(|report| format!("{report:?}"))
                .collect();
            return Err(format!("{error}; faults: {faults:?}"));
        }
    };
    Ok((words.to_vec(), route))
}

/// The line the example prints: the frame went through the planner's route, and the shader's
/// words are the CPU's.
pub fn run() -> Result<String, String> {
    let count = 4096;
    let (words, route) = through_the_graph(count)?;
    if words != on_cpu(count) {
        return Err("the frame's words on the GPU aren't the CPU's".into());
    }
    Ok(format!(
        "dmabuf: a {count}-byte frame from the CPU reached the shader by {}, and the words are the CPU's\n",
        route.join(", ")
    ))
}
