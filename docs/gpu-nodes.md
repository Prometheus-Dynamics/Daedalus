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
| Element-wise function | One WGSL function, `apply`, of one element of its input | Merge it with its neighbours into one shader, one pass |
| Whole shader | A compute shader with its own bindings | Share one submit with the other nodes of its region |
| Your own recording | Rust that records into the encoder | Order it, and nothing more |

Element-wise bodies that feed each other, with nothing else reading the values between them, become
one shader. A second reader of a value, or a node that is not an element-wise body, ends a chain.

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

Daedalus does not judge who made a dma-buf: one it is given is imported in place when the device can take it, and whoever hands it over answers for its contents being readable by a device. A dma-buf with less memory behind it than the device needs for the buffer is refused with both sizes, and is not copied.

A type whose values are dma-buffers says so once: it implements `DmaBufBacked` and the backend is given `with_dmabuf::<T>()`. While the open device imports dma-bufs the planner takes that import over the upload of the type's bytes (its `HostBytes`), which is the route a device that can't import gets.

A node can also say its output is in the dma-buf space (`memory(out = "dmabuf")`, with the `daedalus-dmabuf` backend in the plan). The same `with_dmabuf::<T>()` imports from there, and a device that can't import gets the value handed to the host and uploaded. See [foreign-values.md](foreign-values.md).

A value the host or a CPU node reads is written by its shader straight into memory the host
maps, so no readback copy is planned at all. The backend offers two such places: dma-heap memory,
which the host reads through its cache, and host-mapped memory, which it reads uncached. The
planner places each GPU output in whichever of the device's spaces costs its readers least, and
the region's step allocates the output there. An output only other shaders read stays in device
memory. Dma-heap placement is offered only by a device that is open, imports dma-bufs, and whose
system dma-heap opens. The planner reads that from the open device and never opens one to ask, so
a plan made before the device opens places host-read outputs in host-mapped memory.

The host then reads the bytes where they are. A type that is bytes underneath implements
`HostBytes::from_mapped` to keep the `MappedBytes` it is given, and reads the shader's output with
no copy: the only cost left is the cache sync after the device's write. `MappedBytes` holds the
buffer, so the region writes its next output into another one until the host lets go. A type
that doesn't implement it gets its bytes copied out into a `Vec`, as before.

On the CM5, for a 16 MiB result, the shader's write costs the same into either memory (8.46 and
8.47 ms). Getting the bytes to the host after that costs 0.61 ms in place. A readback into
dma-heap memory followed by a copy out costs 7.96 ms, and the route through host-mapped memory
16.0 ms. Reading every byte once costs 2.1 to 2.2 ms in place in all, against 5.2 to 5.6 ms for
copying out and then reading. The record is `testing/board-runs/read-in-place.md`.

Values a region doesn't write itself, such as an input handed on, still take the readback routes:
`wgpu.readback` into host-mapped memory and `wgpu.readback.heap` into dma-heap memory.

## Waiting

A GPU step does not block the tick. It submits and returns a fence, the runner polls the fence,
and the nodes that read its outputs run once it is ready. Each wait is bounded.

## Failure

A lost device is a fault of the region's step, reported with every node the region covers. The
backend opens the device again and the region runs again: it is not turned off.

## What it costs

- A region of whole-shader nodes is one queue submit, however many nodes it has, and one pass a
  node.
- A chain of merged element-wise bodies is one pass and one pipeline.
- On Vulkan, a region whose nodes are all shaders that bind storage and uniform buffers is
  recorded with Vulkan itself, into a command buffer the region uses again. A warm step of it
  allocates nothing, and the lavapipe lane holds it to that.
- A region that is given a texture, draws to one, or holds a recording body goes through wgpu's
  encoder, which allocates when it records a pass. So does every region off Vulkan.

The counted budgets are checked on Mesa's software Vulkan in the `GPU` workflow
(`scripts/gpu-lavapipe.sh`). Timings are board runs.

## Sharing the GPU with other processes

Several processes can share one GPU, and a host may have to take turns on it. The backend takes an
optional lease for that: `backend.set_lease(Some(lease))`, where `lease` implements `GpuLease`
(`acquire` and `release`).

- `acquire` is called before the device is given work, when none is in flight from this process.
  `release` is called when the last of that work has finished. However many submissions ran in
  between, the calls come in pairs.
- Everything the backend gives the device counts: a region's submit, a chain recorded with Vulkan
  itself, an upload, a readback and a clear.
- The backend doesn't know who else wants the GPU or how the turn is decided. That is the host's,
  and what the hook does is the host's too. An `acquire` that waits holds up the submit that asked,
  so a hook that must wait should bound it.
- `set_lease(None)` takes the lease away. Work still in flight is released first, so the calls
  stay paired.
- With no lease set, a submit pays one load of a flag and nothing else.
- Backends that share one device share its lease.

