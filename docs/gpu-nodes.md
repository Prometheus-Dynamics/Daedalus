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

### A process that isn't root

On a board the GPU's programs usually don't run as root. Such a process needs to be able to open
the GPU's device nodes, and for Vulkan that is more than the render node. V3D's driver on the
CM5 opens `/dev/dri/card0` and `/dev/dri/card1` as well as `/dev/dri/renderD128`. The render node
is open to everyone there and the card nodes belong to group `video`, so the process has to be in
`video`: `render` alone, or nothing, leaves it with no adapter. When no adapter is found and the
user is refused a node under `/dev/dri`, opening fails with `OpenError::NoAccess` naming the nodes.

Give the process a home or a cache directory of its own too (`HOME` or `XDG_CACHE_HOME`): the
driver keeps its shader cache there, and turns it off when it can't write.

Such a process can't open a dma-heap either, where the heaps are root's. It still imports a
dma-buf it is handed: see "Where the data is".

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

Whether a device imports dma-bufs is found by trying: when the device opens, one small buffer from the system dma-heap is imported. A driver can list the extensions and still refuse every dma-buf it didn't make itself, as NVIDIA's does, and such a device is planned as one that can't import: its values are uploaded. A refused import is an error and never more. Vulkan leaves a refused fd with the caller, but a driver may close it anyway, and not every time, so the backend closes a refused fd only if it is still open on the same file.

A dma-buf can hold several values: the planes of one frame, or buffers handed out from a pool. A type says where its value starts with `DmaBufBacked::offset` (zero unless it says). The memory imported is the dma-buf's, and the device buffer is bound at that offset in it, so a value at an offset is imported in place like any other. A device binds a buffer only at a multiple of an alignment of its own: a value that starts somewhere else is copied, and that says nothing about what the device imports.

An import is kept by the step that made it, and found again when the same buffer comes round with the same offset and length: a producer that cycles through a few buffers imports each once, and the device reads whatever the producer has written since. What is kept is counted per import step, so per edge that imports, not per device or per graph, and one import is one value: a pool of eight buffers of two planes is sixteen. `Options::kept_imports` sets the bound, 64 unless it says otherwise. Past it the import made longest ago is let go: a step downstream that still holds that buffer keeps it, and the value is imported again the next time it comes, at the cost of one more import and nothing else. A bound of 0 keeps nothing and imports every value as it comes. Set the bound to at least the number of values the producers on one edge cycle through, or each round imports again.

This relies on one thing from the driver: that a dma-buf can be imported more than once, and each import is counted on its own. Values at different offsets in one buffer are separate imports of the same dma-buf, and so is a value imported again while a buffer from its earlier import is still held. Vulkan's dma-buf import allows this, and the memory is freed when the last import and the last fd are gone. RADV and llvmpipe are checked for it by `backends/wgpu/tests/import_offset.rs`, and V3D by a board run (`testing/board-runs/J6.md`).

A producer may hand out dma-bufs whose fd only allows reading, so that a reader can't map the buffer and write it. A type says so with `const READ_ONLY: bool = true` on its `DmaBufBacked`. Not every device imports one: a software renderer maps what it imports for writing and refuses it. So a device is tried with a read-only fd too when it opens (`CapsRecord::dma_read_only`), and the import of a read-only type is listed only where that worked. Elsewhere the value is copied. The read-only fd stops a host mapping from writing; it doesn't stop a device that imported the buffer.

The trial at open needs a buffer, and so a dma-heap the process can open. On a board the heaps are root's, and a consumer there isn't root: it can't try anything. Its answer is then unknown, not no, and the import is planned all the same. The first dma-buf it is handed is the trial. If the device takes it, it is imported in place and every later one is too. If the device refuses it, that value's bytes are copied to the device by the same step, so nothing faults, the answer is remembered, and the next plan takes the copy. A value the device can't import for a reason of its own, as one with too little memory behind it, is copied without changing the answer.

A host that knows can say so before the first value, so that one isn't special: `backend.assume_dmabuf_import(read_only, imports)`, or `backend.try_dmabuf_import(fd, read_only)` with a buffer the producer supplies. `backend.dmabuf_import(read_only)` gives the answer so far, `None` while nothing has tried. `Options::no_heap_access` opens a device as such a process would, on any machine.

### On a machine with no dma-heap, and nothing else installed

Daedalus needs nothing beside itself to run. It depends on no device service, no camera stack
and no allocator of anyone else's, and the gate fails a manifest that adds one. A dma-heap is
not needed either: a machine without one, or a process that may not open it, is a normal
machine, and nothing faults there. What changes is only whether a value that is a dma-buf is
used in place or copied, and a host can ask which, in words:

```rust
println!("{}", backend.dmabuf_status(false));
```

| It says | Because | What a person can do |
|---|---|---|
| the device imports a dma-buf in place | it was tried when it opened, or with the first one handed over | nothing |
| copied, the device has no dma-buf import | the driver lacks it, or this isn't Linux | another driver or device |
| copied, the device refused one of this kind | a driver that takes only its own buffers, or not a read-only fd | hand over the other kind of fd, or accept the copy |
| copied, the host turned imports off | `Options::no_dma_heap` | turn it back on |
| not known yet, there is no dma-heap under `/dev/dma_heap` | the kernel has none, or a container isn't shown it | nothing is needed: the first dma-buf handed over decides |
| not known yet, this user may not open the dma-heap | the heaps are root's, and this process isn't | the same. Don't run as root for this: have the producer hand a buffer to `try_dmabuf_import`, or say with `assume_dmabuf_import` |

A value that has no dma-buf at all, as one made on a machine where no heap opens, gives its
bytes (`DmaBufBacked::bytes`) and is copied from them. Only a value with neither is a fault,
and its reason says so. `Storage::new_dma_heap`, which allocates from the heap itself, is the
one call that needs a heap: without one it returns an error, and `Storage::new` is what to use.

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

