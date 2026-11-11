# GPU nodes

The GPU is a backend, `daedalus-wgpu`, like any other. A node whose body is a shader is written
with the same `#[node]`, naming the backend's authoring crate:

```rust
#[node(
    id = "demo.scale",
    inputs(src),
    outputs(dst),
    backend = daedalus_wgpu_author(src: Samples, dst: Samples),
)]
const SCALE: &str = include_str!("scale.wgsl");
```

A shader has no Rust signature, so the backend's arguments give each port its type.
[`backends/wgpu-author/README.md`](../backends/wgpu-author/README.md) is the reference for
everything a shader node can declare, and [`backends/wgpu/README.md`](../backends/wgpu/README.md)
for opening a device.

## Three levels

Pick the lowest that does the job.

| Level | You write | What the backend can do with it |
|---|---|---|
| Per-pixel function | One WGSL function, `apply`, of a pixel | Merge it with its neighbours into one shader, one pass |
| Whole shader | A compute shader with its own bindings | Share one submit with the other nodes of its region |
| Your own recording | Rust that records into the encoder | Order it, and nothing more |

Per-pixel bodies that feed each other, with nothing else reading the values between them, become
one shader. A second reader of a value, or a node that is not a per-pixel body, ends a chain.

## Opening a device

```rust
let gpu = WgpuBackend::open(options);   // finds and opens its own device, on first use
let gpu = WgpuBackend::adopt(handle);   // is given a device someone else set up
let planned = plan(&graph, &registry, &[&gpu, &CpuBackend], &Config::default())?;
```

The core never sees a device. Planning opens nothing: the device is opened the first time
something runs on it.

## A CPU body beside a GPU body

Give a node both, with the same id and ports. The planner then picks, so the graph runs on a
machine with no GPU, and a node goes to the GPU only when that crosses between backends less
often. A graph can force a backend for a node.

## Where the data is

A value on the device is in the backend's own memory space. The planner puts an upload where a
CPU value feeds a GPU node and a download where a GPU value feeds a CPU node or leaves the graph,
and none between two GPU nodes. Memory that is already somewhere the device can import, such as
a dma-buf, is imported and not copied: the type's owner describes the memory, and a transfer
registered for that type does the import.

A value that goes back to the host has two routes. `wgpu.readback` copies it into host-mapped
memory, which the host reads uncached, and `wgpu.readback.heap` copies it into dma-heap memory,
which the host reads through its cache: on the CM5 a 16 MiB readback costs about 12 ms that way
against about 17 ms into host-mapped memory. The heap route is offered only by a device that is
open, imports dma-bufs, and whose system dma-heap opens. The planner reads that from the open
device, and never opens one to ask. A plan made before the device opens is correct and uses the
host-mapped route alone. Planning on an open device prefers the heap route, since it costs less.
The planner takes the cheaper route, so a backend that cannot import dma-bufs gets the
host-mapped readback without any change to the graph.

## Waiting

A GPU step does not block the tick. It submits and returns a fence, the runner polls the fence,
and the nodes that read its outputs run once it is ready. Each wait is bounded.

## Failure

A lost device is a fault of the region's step, reported with every node the region covers. The
backend opens the device again and the region runs again: it is not turned off.

## What it costs

- A region of whole-shader nodes is one queue submit, however many nodes it has, and one pass a
  node.
- A chain of merged per-pixel bodies is one pass and one pipeline.
- On Vulkan, a region whose nodes are all shaders that bind storage and uniform buffers is
  recorded with Vulkan itself, into a command buffer the region uses again. A warm step of it
  allocates nothing, and the lavapipe lane holds it to that.
- A region that is given a texture, draws to one, or holds a recording body goes through wgpu's
  encoder, which allocates when it records a pass. So does every region off Vulkan.

The counted budgets are checked on Mesa's software Vulkan in the `GPU` workflow
(`scripts/gpu-lavapipe.sh`). Timings are board runs.
