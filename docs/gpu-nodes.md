# Writing GPU nodes

How a library ships a node with a GPU implementation and a CPU one, and how the parts in
`daedalus::gpu` fit together. The worked example is `examples/07_gpu/fragment_node.rs` (bin
`gpu_fragment_node`). It is a 3x3 box filter whose device is a per-node config value, it runs in
CI on the CPU, and the same binary runs on the GPU when built with `gpu-wgpu`.

## Choosing the device

The consumer chooses the device; Daedalus never picks one on cost. There are two levels:

- **Per node:** a config value the node reads (the example's `device` const input), or the
  node's `compute` affinity (`cpu_only`, `gpu_preferred`, `gpu_required`), set with
  `add_handle_with_compute` / `node_with_compute`. A node that keeps one id and switches inside
  its handler (as Eidos does) reads its own config. A CPU and a GPU node pair use
  `#[node(fallback = "...")]`. A node that switches inside, with CPU-typed ports, keeps
  `compute = cpu_only`, so no GPU segment or transfers are planned. It names its config value
  with `#[node(device_from = "config.backend")]` (`<port>.<field>...`). `explain_plan()` then
  reports the instance's device from that const value: `"gpu"` gives GPU when the engine has a
  GPU backend, and otherwise the reason is `config backend = gpu, no GPU backend: CPU fallback`.
- **Per graph:** the engine's GPU backend (`EngineConfig::gpu(GpuBackend::Device)`, with
  `with_planner_gpu(true)` for GPU-resident ports). Without one, `fallback` nodes run their CPU
  node.

`explain_plan()` reports each node's resolved `device` and `device_reason`. A CPU fallback names
the GPU node it replaced, and a `gpu_preferred` node without a GPU says it runs on the CPU. When
a GPU is requested but missing, the node should fall back to its CPU path and report it, as the
example does. On boards without a compute-capable GPU (armv7 Flame, riscv64 K230D) the build
has no wgpu at all, and `ci.sh cross` checks that.

## State on the engine's device

Build GPU resources once and keep them in the node's `state(T)`. Create them on the device the
engine uses, `ctx.gpu.as_ref()?.wgpu()`, so other GPU nodes and the transfers can share them.
Dropping the state, as `Restart` or a reload does, frees them.

| Need | Type |
|---|---|
| A buffer for the node's lifetime (TSDF volume, cost volume) | `GpuStorage` (`new`, `with_data`, `write`, `ensure_size`) |
| Scratch buffers whose size changes per frame | `GpuStoragePool` (`take` / `give`, size classes, a cap, stats) |
| Images: inputs and render targets | `GpuTexture` (`write`, `start_read`, `read_into`) |
| A fragment kernel | `FragmentPass` (+ `FragmentBinding`, `FragmentRects`) |
| Your own pipelines | `ctx.gpu.wgpu()` → `(&Device, &Queue)` |

## Fragment passes or compute

On tile-based GPUs, write dense per-pixel kernels as fragment passes. On the CM5's V3D (Mesa
v3dv), `gpu_bench` measured:

| 1280x800 box 3x3 | ms |
|---|---|
| compute, packed-u32 storage buffer | 36.4 |
| compute, storage image | 30.6 |
| compute, sampled texture | 11.7 |
| **fragment pass** | **2.8** |
| CPU, separable, 4 threads | 1.8 |

For the 512-FLOP kernel the fragment pass reaches 19.3 GFLOP/s against 15.6 on the four A76
cores, and storage-buffer compute 3.4. Simple filters stay on the CPU; ALU-heavy dense work
(plane-sweep cost, TSDF updates) is real offload. The same WGSL runs on Mali and desktop GPUs.

`FragmentPass::new(gpu, wgsl, &[target formats])` compiles your
`@fragment fn fs(@builtin(position) pos: vec4<f32>) -> ...`. Daedalus supplies the vertex
stage. Bindings, all in `@group(0)`:

- input textures first, read with `textureLoad(src, vec2<i32>(pos.xy), 0)`; `texture_2d<f32>`,
  or `texture_2d<u32>` / `texture_2d<i32>` for integer formats;
- then `GpuStorage` parameter buffers (`var<storage, read>`).

The pass writes one render target per `@location` (multiple render targets). r8, rgba8, r32f,
rg32f, r8uint, rgba16uint and rg32uint are tested. Every binding the shader declares must be
used.

- **Cost.** On the Raze (V3D), `FragmentPass` costs the same as a hand-written fragment
  pipeline: box3x3 r8 2.83 ms against 2.84 ms, box5x5 7.05 ms against 7.06 ms (`gpu_bench`).
- **Bind once.** `pass.bind(&inputs, &buffers)` returns a `FragmentBinding`; keep it with the
  textures and encode with `encode_bound` / `run_bound`. `encode` / `run` bind each call (one
  validation round trip, about 1 µs).
- **Draw part of a target.** Write rectangles (`[x, y, w, h]` in target pixels) into a
  `FragmentRects`, then call `encode_rects(encoder, &binding, &rects, &targets, load)`. It is
  one instanced draw; with `load` the rest of the target is kept.
- **Many passes per tick.** Encode them into one `wgpu::CommandEncoder` and submit once
  (`GpuKernels::encode`, `FragmentPass::encode_bound`). Each encoder and submission costs wgpu
  time and allocations (about 29 allocations per submit). On an RX 6800 XT, 8 small kernels cost
  0.37 ms per frame as 8 submits and 0.15 ms as one (`gpu_bench [batching]`). On V3D the same
  frame is GPU-bound: 1.30 ms as 8 submits and 1.32 ms as one, against 22 µs to record and
  submit an empty dispatch (p50, `[dispatch-latency]`), so there the per-submit cost is a small
  share. `GPU_BENCH_SECTIONS=batching,allocation` runs only the named sections.
- **Dispatch floor.** One small dispatch (a 64 KiB copy) costs the same CPU time through
  `GpuKernels` as hand-written wgpu with its pipeline and bind group made up front. On the Raze
  (V3D, p50), recording and submitting takes 23.3 µs raw, 23.0 µs with `encode` and 23.2 µs with
  `run`. Submit to done takes 173 µs raw and 186-187 µs through `GpuKernels`: its kernel also
  binds the status and auxiliary buffers. The allocations match too: 29 per dispatch raw and with
  `encode`, 31 with `run` (its `GpuTicket`; `tests/gpu_allocations.rs`). Reported by
  `gpu_bench [dispatch-floor]`.

Compute stays available through `daedalus::gpu::shader` dispatch, or your own pipelines.

## Conversions on the device

`GpuKernels::new(gpu)` runs the `daedalus-kernels` ops on GPU-resident `GpuStorage`, bit for bit
like the CPU kernels: RGB/RGBA/gray layouts, LE f32, binary16→f32, f32→unorm8, byte copies,
hex and base64 (`GpuKernel`). `run(kernel, &src, &dst, n)` submits and returns a ticket;
`encode` adds the pass to your encoder (with optional timestamps). `base64_decode` also reports
the decoded length and invalid input. `copy_with_engine` copies on the copy engine. Use them when
the data is already on the GPU: convert before a readback, or after a decode, instead of moving
the bytes back to the CPU. `docs/backend-matrix.md` records the measured pass time against the
CPU kernel per adapter. On an RTX 4060, every op over 4 MiB takes 0.005-0.2 ms on the GPU against
0.2-10 ms on the CPU. The choice is the caller's, per device.

## Buffers without copies

- **Unified memory (V3D, Mali, integrated, lavapipe):** `GpuStorage::new_host_mapped` puts the
  buffer in host-visible memory, mapped for its whole life. `write_mapped` and `read_mapped` copy
  in and out in place, with no staging buffer and no queue copy. The GPU sees the bytes on the
  next submit, and the CPU reads results once the ticket completes. On discrete GPUs it is the
  same API over the bus (slower CPU writes).
- **Read results back from `GpuStorage::new_host_cached`, not `new_host_mapped`.** Host-mapped
  memory is usually uncached or write-combined: fine for CPU writes, slow for CPU reads.
  Reading 16 MiB took 640 ms from host-mapped memory on an RX 6800 XT and 17 ms from
  host-cached. v3dv has a single memory type (`DEVICE_LOCAL | HOST_VISIBLE | HOST_COHERENT`, no
  `HOST_CACHED`). There, with the `gpu-dmabuf` feature, `new_host_cached` makes a system dma-heap
  buffer that the GPU writes through an import and the CPU reads through its own cached mapping,
  with `DMA_BUF_IOCTL_SYNC` around each `read_mapped`/`write_mapped`. On the Raze that reads
  16 MiB in 3.6 ms instead of 13.1 ms from the uncached mapping (a plain memcpy takes 2.9 ms),
  and 1 MiB in 0.30 ms instead of 0.63 ms (`gpu_bench [readback]`). Without dma-heap access it
  falls back to host-mapped memory.
- **No zero-init:** `GpuStorage::new_uninit` skips wgpu's lazy clear on first use, for buffers
  the caller fully writes first. `gpu_bench`'s `[allocation]` section reports what that clear
  costs, next to `vkCreateBuffer`, `vkAllocateMemory` and wgpu's `create_buffer` under each
  memory-hint mode.
- **What an allocation costs on V3D** (Raze, 2026-11-01):
  - `vkAllocateMemory` grows with size, about 0.35 ms per MiB (5.5 ms for 16 MiB, 0.28 ms for
    1 MiB); `vkCreateBuffer` is free.
  - With `MemoryUsage` hints each large buffer is its own allocation: a 16 MiB `GpuStorage::new`
    costs 5.8-6.4 ms. With `Performance`, wgpu sub-allocates from blocks it already made
    (0.005 ms).
  - The first use adds the zero-init clear: 2.3 ms for 16 MiB, against 0.47 ms with
    `new_uninit`.

  So on V3D, allocate at setup and reuse (`state(T)`, `GpuStoragePool`): never allocate per
  tick.

## Never wait in a tick

A tick submits and returns. Its results arrive on a later tick:

- `GpuTicket::submit(gpu, commands)` (or `pass.run_bound`) returns a ticket;
  `ticket.try_complete()` runs one non-blocking poll (about 0.3 µs on the CM5).
- `GpuInFlight<T>` caps the outstanding submissions. `push` hands back the ticket and value
  when full (skip or drop the frame), `pop_completed` returns finished values in order, and
  `drop_oldest` implements latest-wins.
- Readback into your own buffer: `storage.start_read(offset, len)` or `texture.start_read()`
  queues the copy into a reused staging buffer. `readback.try_read_into(&mut out)` returns
  `None` until the data is there. A readback queued after a pass carries its result.
- `wait()` and `read_into` block; use them for setup, shutdown and tests only.

The example does all of this. It returns `Option<Vec<u8>>`, emitting nothing on a tick whose
frame is still in flight, and is fed empty frames at the end to collect the last results.

## Timing

`GpuTimer::new(gpu)` is kept in state. Pass `timer.render_writes()` / `compute_writes()` to the
pass (or call `FragmentPass::run_timed`), submit with `timer.submit(encoder)`, and poll
`try_elapsed()`. With timestamp queries (V3D, Mali, desktop) the result is GPU execution time.
Otherwise it falls back to submit-to-completion, and `GpuTimeSource` says which you got. Report
it with `ctx.duration_metric("gpu", time)`.

## Camera frames

- Zero copy into a storage buffer, imported once per dmabuf: keep a `GpuFrameImports` in the
  node's state and call `imports.import(&frame, timeout)` with the `SharedFrame` of an
  External-residency input (a Styx `FrameLease`). A camera's pooled dmabufs are each imported on
  first sight, keyed by inode, and reused after that (`stats()` counts hits, imports and
  evictions). The returned `GpuFrameInput` holds the storage, each plane's offset and stride,
  and the frame itself: keep it until the GPU work reading the frame completes. The import waits
  for the dmabuf's implicit write fence. A producer that wrote the frame with the CPU ends its
  access first (`DMA_BUF_IOCTL_SYNC`, `SYNC_END`). The lower level is
  `GpuStorage::import_dmabuf(gpu, fd, offset, len, fence, timeout, label)` for `LINEAR`
  dmabufs. The dmabuf needs the driver's buffer padding past the last plane: 256 B on v3dv
  (a 307200-byte frame needs 307456 bytes), none on RADV, NVIDIA or lavapipe. That is a Vulkan
  rule (a buffer binds only to memory covering its whole requirement), so a shorter dmabuf
  cannot be imported whole, by clamping or otherwise. `GpuStorage::dmabuf_import_len(gpu, len)`
  and `GpuFrameImports::required_len(end)` give the size to allocate. vb2 rounds V4L2 `MMAP`
  buffers up to whole pages, so the padding is usually there already. It is not when
  `sizeimage` is a whole number of pages (640x480 GREY): then request a larger buffer
  (`VIDIOC_CREATE_BUFS` with a larger `sizeimage`). Styx pads its dma-heap frames. Import takes
  16-29 µs per frame on the CM5.
- Your own SPIR-V on Daedalus's device (feature `gpu-vulkan-raw`, Linux):
  `GpuContextHandle::raw_vulkan()` gives the `ash` instance, physical device, device, queue
  and queue family; `GpuStorage::raw_vk_buffer()` gives a storage's (or imported frame's)
  `VkBuffer`, offset and size, for your own descriptor sets. Record dispatches with
  `gpu.submit_raw(|device, cmd| ...)`. Daedalus submits them on its queue, in order with its
  own work and between full memory barriers, and returns a `GpuTicket`. Never submit on the
  queue yourself: wgpu serializes its submissions behind a lock nothing outside it can take.
  Keep everything the commands use alive until the ticket completes. `daedalus_gpu::raw_vulkan`
  documents the whole contract. Tested on v3dv under the validation layer with synchronization
  validation: a raw `ash` pipeline reads an imported dma-heap frame in place. Daedalus enables
  8-bit storage-buffer access where the device has it (all of v3dv, RADV, NVIDIA and lavapipe
  do): check `RawVulkan::storage_buffer_8bit` before creating a pipeline that reads 8-bit
  samples. `timestamp_valid_bits` and `timestamp_period_ns` are for timestamp queries recorded
  inside `submit_raw`. For a frame loop, `RawSubmitter` records into a ring of reused command
  buffers, each with a fence, and allocates nothing per submit: `RawSubmitter::new(&gpu)`, then
  `unsafe submitter.submit(|device, cmd| ...)` returns a `RawTicket` (`try_complete`, `wait`). It
  takes the same submit lock as every Daedalus submit. Raw commands do not see `queue.write_buffer`
  data (wgpu flushes that with its own next submit): call `gpu.flush_uploads()` after such writes
  (once at setup for `GpuStorage::with_data`), or upload inside your own recording.
- Into a sampled texture without a shader: import as a storage buffer as above, then
  `texture.copy_from_storage(encoder, &storage, 0, stride)` (or `upload_from_storage`). This is
  `vkCmdCopyBufferToImage`, which v3dv runs on its texture formatting unit (TFU) for a whole
  frame from offset 0: 0.49 ms at 640x480 XRGB and 0.71 ms at 1280x720, against 2.9 and 8.6 ms
  for a shader copy. A stride that is not a multiple of 256 bytes (640-byte GREY rows) is
  folded instead: `k` rows side by side are 256-aligned (`k = 2` for 640), so one direct copy
  fills a wider intermediate texture, and a small fragment pass unfolds it: 0.43 ms for a
  640x480 GREY frame on the CM5, against 0.27 ms with a 256-aligned stride. That needs the source
  to cover the last folded row, which a camera frame's padding does. Row-by-row re-pitching (21 ms
  at 640x480 on v3dv) remains only for strides whose fold would be too wide. Frames allocated
  with 256-aligned strides stay on the direct path. The PiSP writes only `LINEAR`
  or SAND128, and v3dv samples only UIF, so this is the fast way to sample a CM5 camera frame.
- Into a sampled image: `GpuContextHandle::import_dmabuf` with a DRM format modifier the GPU can
  sample. On v3dv that is UIF only; `LINEAR` images are render/transfer only, so they need a copy.
- Otherwise upload: `GpuTexture::write` or `GpuStorage::write` from the mapped frame (0.26 ms
  for 640x480 XRGB on the CM5).

## Transfers as plan edges

Keep GPU data on GPU-typed ports between nodes so the plan can see the transfers. A
`#[device]` upload/download that creates real GPU resources takes the context:
`fn upload(v: &Cpu, gpu: &GpuContextHandle) -> Result<Dev, TransportError>`. Assert
`explain_plan().device_round_trips().is_empty()` in the pipeline's test to catch CPU→GPU→CPU
chains. A transfer done inside a handler is invisible to the plan.

## Startup

Let the engine keep the Vulkan pipeline cache: `EngineConfig::gpu(GpuBackend::Device)
.with_gpu_pipeline_cache(dir)` (or `DAEDALUS_GPU_PIPELINE_CACHE=dir`). When the engine creates
its device it opens the cache file for the adapter in `dir` and installs it, so `FragmentPass`,
shader dispatch, and the pipelines a node builds from `ctx.gpu` in its first tick all use it. A
node building its own pipelines passes `pipeline_cache_for(device)` as the descriptor's
`cache`. Each graph saves the cache when it grew: after its 1st, 2nd, 4th, ... 256th tick, on
`HostGraph::save_pipeline_cache()`, and on drop, so a process that is killed keeps what its
first ticks built. `HostGraph::pipeline_cache_status()` reports the file, the bytes loaded and
saved, and why there is no cache (no Vulkan pipeline cache on the device, an unwritable
directory). The driver drops data from another driver version.

Memory hints decide how the device allocates: wgpu's `Performance` hints make a large block on
the first allocation of each memory type, a 42 ms tick for the first readback buffer on the
CM5. The engine's default (`GpuMemoryHints::Auto`) uses `MemoryUsage` on integrated GPUs (V3D,
Mali) and `Performance` on discrete ones. Choose with `EngineConfig::with_gpu_memory_hints`,
`GpuOptions::memory_hints`, or `DAEDALUS_GPU_MEMORY_HINTS=performance|memory`.

Outside an engine, `GpuPipelineCache::open(gpu, dir)` does the same by hand: `install()` it
before building passes and `save()` (or `save_if_grown()`) it after. On the CM5's V3D, 12
fragment pipelines build in 5.7 ms instead of 52 ms with the cache loaded (Mesa's own shader
cache off); on desktop 3 ms instead of 82 ms.

## Limits and portability

`ctx.gpu.capabilities().compute` reports workgroup and storage limits and the enabled shader
features (f16, int64, int64 atomics, subgroups, timestamps). Staying within
`GpuComputeCaps::WEBGPU_DEFAULTS` (16 KiB workgroup storage, 256 invocations, 8 storage
buffers / 4 storage textures per stage) runs unchanged on V3D, Mali and desktop. V3D has no
int64, f16 or f64 and no 64-bit atomics; its storage-image atomics are r32uint/r32sint only.
Packed `u32` and integer textures port everywhere.

## Testing

- Unit and graph tests skip without an adapter (`ctx.wgpu()` is `None`), so CI runs them
  CPU-only.
- The CPU path is the reference: compare GPU results to it, bit-exact with integer formats.
- `./scripts/ci.sh pi` runs the GPU hardware tests on a CM5, and `gpu_bench` reports the
  device's budget (`crates/gpu/README.md`).
