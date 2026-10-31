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
  `#[node(fallback = "...")]`.
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

- **Bind once.** `pass.bind(&inputs, &buffers)` returns a `FragmentBinding`; keep it with the
  textures and encode with `encode_bound` / `run_bound`. `encode` / `run` bind each call (one
  validation round trip, about 1 µs).
- **Draw part of a target.** Write rectangles (`[x, y, w, h]` in target pixels) into a
  `FragmentRects`, then call `encode_rects(encoder, &binding, &rects, &targets, load)`. It is
  one instanced draw; with `load` the rest of the target is kept.
- **Many passes per tick.** Encode them into one `wgpu::CommandEncoder` and submit once.

Compute stays available through `daedalus::gpu::shader` dispatch, or your own pipelines.

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

- Zero copy into a storage buffer: `GpuStorage::import_dmabuf(gpu, fd, offset, len, fence,
  timeout, label)` for `LINEAR` dmabufs. The dmabuf needs the driver's buffer padding, 256 B on
  v3dv; Styx pads its frames. Import takes 16-29 µs per frame on the CM5.
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

`GpuPipelineCache::open(gpu, dir)` loads the Vulkan pipeline cache for the adapter.
`install()` it before building passes and `save()` it after; the driver drops data from another
driver version. On desktop, 12 fragment pipelines build in 3 ms instead of 82 ms with the cache
loaded.

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
