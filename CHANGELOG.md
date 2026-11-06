# Changelog

All notable changes to this workspace should be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [Unreleased]

The workspace is versioned `3.0.1-dev` (from `3.0.0`): dev builds carry a `-dev` pre-release
patch version, and only the patch moves until a release is agreed. A git dependency on Daedalus
with `version = "3.0.0"` no longer resolves; drop the `version` requirement and pin the commit.
Plugins installed through the Rust ABI must be built against the same Daedalus version as their
host (`PluginInfo::daedalus_version` is compared exactly), so rebuild them with the host. A
plugin built from a 3.0.0 dev commit with plugin ABI 10 (before 11) still installs, through the stable ABI
(stable ABI 2) when it exports stable handlers; one built from the `v3.0.0` tag has an older
plugin ABI and does not load.

### Added

- Shared-library surface, first version: `daedalus-surface` (`crates/surface`, a `cdylib` and
  `rlib`) is a C ABI over the in-process graph runtime, on `lemnos-abi` (pinned at `cf3a4ac`). It
  has the version handshake (`daedalus_surface_version`, `daedalus_surface_features`), host
  create and release, plugin install by path through the stable handler path, schemas, graph
  compile from a `daedalus.graph` document, per-frame push, tick and take of scalars and
  `SharedBytes`, and port, explain and telemetry JSON. The header is `crates/surface/include/
  daedalus_surface.h`. `daedalus-host` (`crates/host-wrapper`) is the safe Rust wrapper: it loads the
  library by path, checks the handshake and layout hash, and returns RAII host and graph handles.
  `crates/host-wrapper/tests/shared_vs_static.rs` runs one graph statically and through the surface
  and asserts identical outputs; a warm scalar frame through the surface allocates nothing.
- Dma-buf frames across the shared-library surface (Linux; `NOT_SUPPORTED` elsewhere):
  `daedalus_surface_push_frame` pushes a `lemnos-abi` `BufferDesc` (one dma-buf per plane, with an
  optional sync-file acquire fence) into a `daedalus:frame` port without copying pixels;
  `daedalus_surface_take_frame` takes a frame out as a lease whose descriptors are duplicated and
  owned by the caller; `daedalus_surface_frame_release` releases the lease. Descriptor ownership
  follows `BufferDesc::OWNED` (borrowed for the call, or taken over and closed on any failure). The
  acquire fence is attached to each dma-buf as a write fence; a syncobj fence is refused. A taken
  frame keeps its `handle().data()` identity. `daedalus-host` gives `Graph::push_frame(port,
  &Frame, acquire)` and `Graph::take_frame(port)` (a `FrameLease` that closes its descriptors and
  releases the frame on drop). A `frame` port is reported as kind `frame` in the port table (it was
  `other`). The tests (`crates/host-wrapper/tests/frames.rs`, `crates/surface/src/frames/tests.rs`)
  check a round trip (the same dma-buf inode, the same pixels), fd counts over many frames, stale
  leases, a panic inside a call, and descriptor ownership.
- Surface ABI 1.1 (additive): ports by index. `daedalus_surface_graph_input_index` and
  `daedalus_surface_graph_output_index` resolve a name to its position in the port table once;
  `daedalus_surface_push_{i64,f64,bool,bytes}_at` and
  `daedalus_surface_take_{i64,f64,bool,bytes}_at` take the index; `daedalus_surface_push_batch` pushes an array of `daedalus_push_entry` under one
  lock and one panic guard, checking every entry before pushing any. The surface interns port names,
  so the engine's port ids are static and a push no longer bumps a reference count. A warm bytes
  push reuses the port's payload allocation as well as its vector (`PayloadSlot`), so it allocates
  nothing (it allocated the engine's payload box before; the vector was already reused).
  `daedalus-host` reads the port table at compile, so `Graph::push_i64("step", ..)` resolves the
  index in the caller and calls by index; `Graph::input` / `Graph::output` return `InputPort` /
  `OutputPort` for the `*_at` calls, and `Graph::push_batch(&[Push])` hands its slice to the
  surface as is. `take_bytes` no longer zero-fills the caller's buffer (it writes into the spare
  capacity). `daedalus-host` requires surface 1.1.
- Stable ABI 3: a plugin's const inputs are sent to its node instance once, on create and again
  when a graph patch changes them (`HostGraph::apply_patch`), instead of with every call. The
  plugin exports `daedalus_plugin_stable_extension` (`StableExtension`, `SetConstsFn`); a plugin
  without it, and every stable ABI 2 plugin, still installs and gets its consts per call. Strings
  in consts are decoded once. Stable-path constants cost the host no allocation, and a
  `String` const costs the plugin only what its handler allocates.
- `dylib_stable_consts` checks a patched const against the static install, per tick.
- Custom metrics a stable plugin node records reach the host node's telemetry, the same as a
  static node's, and the plugin's context carries the node's and graph's metadata. The host
  delivers a services table (`services::HostServices`, `SetServicesFn`) once per instance, at
  create, through the version-4 extension; the plugin forwards the metrics a call recorded after
  the call, and nothing is forwarded when none were. `dylib_stable_telemetry` checks it against the
  static install (it needs the `metrics` feature).
 for synchronization validation. The `sync_hazard_control` example
  (`crates/gpu/examples`, feature `gpu-vulkan-raw`) records two `vkCmdFillBuffer` writes to one
  buffer with no barrier, a deliberate write-after-write hazard, and `scripts/ci.sh vvl` fails
  unless the layer reports a `SYNC-HAZARD` message for it. The example is deliberately wrong and
  nothing depends on it.

- CI runs the transport library's tests under Miri (`scripts/ci.sh miri`, the `miri` job).

- **CUDA and Vulkan order a shared buffer with timeline semaphores.** `interop::SharedSemaphore`
  (a Vulkan timeline semaphore exported as an opaque fd) is imported into CUDA as a
  `CudaSemaphore` (`CudaBackend::import_timeline_semaphore`): the CUDA stream waits for and
  signals values without a host wait. `RawSubmitter::submit_synced` sets and waits the values on
  the raw queue. Test: `cuda_interop.rs::a_timeline_semaphore_hands_a_buffer_between_vulkan_and_cuda`
  on the RTX 4060. Host-fence against semaphore round trips are measured in
  `docs/benchmarks-log.md` (release build: about 198 µs against 216 µs median in runs 2 and 3).
- **A wgpu node, a CUDA node and a wgpu node share one buffer through the planner.**
  `interop::SharedValue` gives one Vulkan allocation a `wgpu::Buffer` view, a CUDA import and a
  timeline semaphore. The planner's device edges are cost-0 `SharedView` adapters
  (`share.gpu_to_cuda`, `share.cuda_to_gpu`), so the plan has no transfer step. Test:
  `crates/runtime/tests/it/executor_shared.rs`, three ticks on the RTX 4060, values checked, transfer
  counters at 0. Fixed on the way: a wgpu view built with `from_raw` destroyed the buffer on drop;
  `from_raw_externally_owned` is used instead.
- **Windows builds and tests.** The shared sources that were symlinks are copies
  (`scripts/check-shared-sources.sh` keeps them identical), so a Windows checkout needs no
  `core.symlinks`; checkouts keep LF line endings; CI runs clippy and the workspace tests on
  `windows-latest`. See docs/development.md, "Windows".
- **MCU firmware on a board (`examples/mcu_board`).** The blink graph on a Raspberry Pi Pico
  through `rp2040-hal` (ADC in, GPIO out). It reports tick time and the stack high-water mark
  per mode over defmt-RTT. The loop is generic over a `Board` trait and tested on the host.
  Written, untested on a board.
- **`daedalus-accelerator`: the accelerator interface.** `Accelerator` (object safe): load a
  program, allocate, upload or import (dmabuf with an acquire fence) buffers, submit after waits,
  and complete through a `Fence` that can export a `sync_file`. Interface only; see
  docs/accelerator-interface.md.
- **MCU chunked plan-blob receive.** `daedalus_mcu::loaded::receive` (feature `receive`):
  `encode_chunk` frames a blob with a CRC-32 per chunk, and `BlobReceiver` assembles it into a
  buffer or a flash partition (`BlobSink`), ignoring duplicates and reporting gaps. See
  docs/mcu.md, "Chunked receive".
- **Per-executor port buffer pools without `std`.** A `no_std` build gives each executor its own
  port buffer pool instead of one process-wide pool, so graphs ticking on several threads allocate
  no more per tick than one thread alone. With `std` the pool stays per thread.
- **NVIDIA CUDA backend (`gpu-cuda`).** `daedalus_gpu::CudaBackend` (`GpuBackendKind::Cuda`,
  chosen with `preferred_backend = Cuda`): the driver API loaded at runtime, so no toolkit is needed
  to build; device memory with `upload`/`download`; a PTX `scale_f32` kernel and `CudaFence`.
  CUDA device memory is a different port type from wgpu's: the `…@cuda` key
  (`GpuBackendKind::device_type_suffix`) against `…@gpu`. `#[device]` transfers keyed `@cuda` run
  through the executor's context (`GpuContextHandle::cuda`). Tests skip without a device.
  Also: CUDA images (2D cuArrays, `create_image`/`upload_texture`/`read_texture`, R8, RG8, RGBA8,
  RGBA16F); CUDA↔Vulkan buffer sharing (`daedalus_gpu::interop::SharedBuffer`, an opaque fd
  imported with `CudaBackend::import_opaque_fd`, zero copies at the handoff, the device UUID
  checked); and planner selection by neighbour: a bare `Residency::Gpu` request lands on the
  device key of its producer or its one consumer, with no transfer (`docs/gpu-nodes.md`). Not
  yet: dmabuf import, which the driver refuses (`CUDA_ERROR_UNKNOWN`), and a graph-level handoff.
- **One GPU device per process.** `daedalus_gpu::shared_context` returns the process's wgpu device,
  opened on first use and closed with its last handle; `process_context` finds it without opening
  one, and `devices_created` counts the devices opened. Engines with `GpuBackend::Device` take their
  device there, so engines in one process (and a library such as Styx's GPU ISP that takes it there)
  share one device, queue and submit lock. Before, each engine opened its own.
- **`HostGraph::recycle_telemetry` / `OwnedExecutor::recycle_telemetry`.** Hand back the
  `ExecutionTelemetry` a tick returned: the next serial tick records into its buffers, so a
  steady-state tick allocates nothing for telemetry at every `MetricsLevel` up to `Hardware`
  (3 allocations per tick at `Timing` and above before, on a 21-stage graph). `drive_blocking`
  does it after each `on_outputs` call, and the frame-bench harness after each tick. A group or
  edge no run touched is left out of `group_metrics`/`edge_metrics`.
- **`daedalus-vglite`: the K230's VGLite GPU through `libvg_lite`, loaded at runtime**
  (written, untested on board). It covers resize (bilinear either way; area only at 2x, as a
  half-phase bilinear blit, since VGLite has no averaging filter), crop, flips and quarter
  turns, fill, blend (SRC, SRC_OVER and DST_OVER, straight alpha only) and palette expansion,
  each between byte buffers or dmabufs. It is the matrix's fixed-function `vglite` variant on
  those `daedalus.image.*` ops.
  - **Lines:** `line_bytes` / `line_dmabuf` draw a 1-wide stroke with square caps through the
    pixel centres. They are the cell of a new op, `daedalus.image.line` (reference:
    Bresenham, as Eidos's plain line), and the one fixed-function line route on the target
    SoCs.
  - **Memory:** the kernel driver maps only physically contiguous dmabufs correctly. Byte
    buffers and dmabufs marked `Backing::Scattered` go through a contiguous staging buffer, and
    each call returns the bytes it copied.
  - **Caps:** `daedalus_rga::caps` gains `Route::Vglite`, `Staging` (when a route copies
    frames), and `FixedOp::Line`. The K230D entry is the VGLite route, with every op
    `Unmeasured`.
  - **Board script:** `scripts/board.sh k230d` runs the VGLite tests.
- **`RawSubmitter`, raw submits without allocation.** An opt-in ring of primary command buffers,
  each with a fence, on Daedalus's queue (`gpu-vulkan-raw`, Linux): `RawSubmitter::new(&gpu)`,
  then `unsafe submit(record)`, which records between the same full barriers as `submit_raw` and
  returns a `RawTicket` (`try_complete`, `wait`). A steady-state submit and wait allocate nothing:
  17 allocations per frame through `submit_raw` before, 0 after. Every Daedalus `queue.submit`
  takes the context's submit lock, and so do the `on_submitted_work_done` and `map_async` calls
  that flush pending writes, so a raw submit never overlaps one. While a `RawSubmitter` exists,
  code outside Daedalus submits on its device only through `GpuContextHandle::submit` or under
  `GpuContextHandle::submit_lock`. `GpuContextHandle::flush_uploads()`
  submits pending `write_buffer` data (`GpuStorage::with_data` included), which raw commands
  otherwise do not see.
- **Shared nodes for the MCU profile.** `#[daedalus_mcu::node(.., runtime = "feature")]` makes one
  function both the device node and a full-runtime `#[daedalus::macros::node]`, with the same id,
  ports, types, optional inputs, conditional outputs, fire mode and state, so one node crate serves
  both (`examples/mcu_shared`). The runtime side is opt-in: without the feature the crate is
  `no_std` and names no runtime item. Shared nodes cannot take `&Ctx`, return `Result` or be
  adapters. The full runtime's `#[node]` now accepts a plain return value (a typed node returning
  `T`, `Option<T>` or a tuple): its signature analysis already accepted one, but the handler only
  compiled `Result` returns.
- **`daedalus-mcu` fan-in and user adapters** (compiled, tunable and loaded modes). Several edges
  into one plain input now plan: the input takes the oldest value of the first edge, in graph
  order, that holds one, and the other values are dropped, as the full runtime does (`FanIn<T>`
  stays unsupported). `#[daedalus_mcu::adapt(id, cost)]` declares a plain `no_std` conversion
  between two port types; the planner uses it like any adapter, inlined in compiled code and as a
  library entry in loaded mode. The build functions (`compile_document`, `plan`, `library`, ...)
  take the device's adapters. Plan blobs move to format version 3 (fan-in inputs, an adapter per
  edge, adapters in the library hash): version 2 blobs are rejected. Several edges into a host
  output stay a build error. `examples/mcu_blink` adds a fan-in graph with a user adapter, tested
  natively in all three modes; the blink firmware's compiled size is unchanged.
- **`daedalus-mcu` `f64` feature** (default): conversions to and from `f64` for parameters,
  constants and widening edges. Without it no `f64` soft-float is linked: an `f64` parameter does not
  compile, a loaded plan converting to or from `f64` fails to load (`LoadError::Type` or
  `Malformed`), and `f64` updates to `f32` parameters still convert (`f64` to `f32` now narrows in
  integer arithmetic, bit-identical to `as f32`). The workspace dependency turns default features
  off; the blink tunable firmware shrinks by 588 B (M4F) / 464 B (M0+).
- **`pipeline_cache_path_for(device)`**: the file of the pipeline cache installed for a device, so
  a node can keep what it measured once per device next to it.

- **`daedalus_rga::Librga`: the RGA through the vendor's `librga`, loaded at runtime** (written,
  untested on board). It reaches what the mainline V4L2 driver does not: RGA3 on vendor kernels,
  gray input and Y-only output. It offers format conversion and scaling between dmabufs or byte
  buffers, and `rgb_to_rgba`, `rgba_to_rgb`, `gray_to_rgba` and `rgba_red`. The FFI declarations
  follow airockchip/librga's headers (Apache-2.0, commit 2b32edc); nothing links the library. The
  matrix's RGA gray and red cells go from a masked gap to untested.

- **8-bit storage-buffer access on Daedalus's Vulkan device.** `storageBuffer8BitAccess` and
  `uniformAndStorageBuffer8BitAccess` are enabled at device creation where the device supports
  them (`VK_KHR_8bit_storage` below Vulkan 1.2), for SPIR-V kernels that read 8-bit samples.
  `RawVulkan` reports them (`storage_buffer_8bit`, `uniform_and_storage_buffer_8bit`), along
  with the queue's `timestamp_valid_bits` and `timestamp_period_ns` for timestamp queries
  recorded in `submit_raw`. Tested on v3dv (with the validation layer), RADV, NVIDIA and
  lavapipe with a precompiled GLSL kernel reading `uint8_t` storage. `gpu-vulkan-raw` now
  implies `gpu-dmabuf`.

- **RGA fill, mask ROP and blend through librga.** `Librga::fill_bytes`, `fill_dmabuf`,
  `fill_mask_bytes` and `rectangle_bytes` (`imfill`; a box is four fills),
  `rop_mask_bytes` and `rop_mask_dmabuf` (`imrop`, new `Rop::{And, Or, Xor}`) and `blend_bytes`
  and `blend_dmabuf` (`imblend`, new `BlendMode::{SrcOver, Src, DstOver}`, straight or
  premultiplied). 8-bit masks go to the engine as RGBA8888 at a quarter of the width (exact for
  fills and bitwise operations); unaligned masks and rectangles are `Unsupported`. The entry
  points load as optional. `caps::FixedOp` gains `Fill`, `MaskRop` and `Blend` (librga; RGA3
  blend only). Ops `daedalus.image.fill`, `mask_rop` and `blend` with scalar references
  (`reference::Blend`). Mainline has none: Linux 7.2 stores `V4L2_CID_BG_COLOR` but never
  programs it. librga's C API draws no lines. `IM_ROP_XOR` is 0xf6, which in ROP3 terms is
  pattern OR (src XOR dst); the board test decides whether it is plain XOR. Written, untested
  on board.

- **RGA flip and rotate on the mainline driver.** `Rga::transform` and `transform_bytes` set
  the context's `V4L2_CID_HFLIP`, `VFLIP` or `ROTATE` control (Lemnos's
  `M2mSession::set_control`) before streaming; a driver that refuses or does not keep the value
  makes the call `Unsupported`. Each transform gets its own cached session. `caps` lists flip and
  rotate on the V4L2 route, and the registry gains their `rga-v4l2` rows. vim2m checks both flips
  exactly and refuses a turn (it has no rotate control). Lemnos is locked at 01939e9.

- **RGA crop, flip and rotate.** `Rga::crop` and `crop_bytes` on the mainline driver (crop
  selection, unscaled; a full-frame rectangle is a copy) and `Librga::crop_bytes`, `crop_dmabuf`,
  `transform_bytes` and `transform_dmabuf` (`imcrop`, `imflip`, `imrotate`) with the new
  `Transform` (`FlipHorizontal`, `FlipVertical`, `Rotate90`, `Rotate180`, `Rotate270`, clockwise
  assumed until a board measures it). An output that is not the rectangle's or the turn's size is
  `InvalidInput`. `caps::FixedOp` gains `Crop`, `Flip` and `Rotate`; mainline lists crop only
  until Lemnos's session can set `V4L2_CID_HFLIP`, `VFLIP` and `ROTATE`. Ops `daedalus.image.crop`,
  `flip` and `rotate` with scalar references. vim2m checks a full-frame copy; the board tests
  are written, untested on board. The librga stand-in tests keep their last-call records per
  thread, so parallel tests no longer race on them.

- **RGA palette and colour key through librga.** `Librga::palette_bytes` and `palette_dmabuf`
  (`impalette`, a `BPP8` source and a 256-entry RGBA8888 table), and `color_key_bytes`,
  `color_key_dmabuf` and `color_key_mask` (`imcolorkey` `INVERTED` from RGBX8888 onto a cleared
  RGBA8888 frame: the in-range pixels come out opaque, so the alpha is the mask). New
  `RgaFormat::{Rgbx8888, Bpp8}`. The two entry points load as optional: a librga without one makes
  that call `Unsupported`. `caps` gains `FixedOp::{Palette, ColorKey}` and `UnitCaps::ops`
  (librga on RGA2 only; exactness `Unmeasured`). New ops `daedalus.image.palette` and
  `daedalus.image.color_key_mask` with scalar references and a `librga` row. That `INVERTED`
  keeps the in-range pixels is from its name; the board test decides. Written, untested on
  board.

- **RGA resize for Eidos's pyramids, and a capability descriptor.** `daedalus-rga` gains
  `Filter::{Area, Bilinear}` with `Rga::resize`, `resize_bytes` and `resize_luma` (8-bit luma
  through NV12, since the mainline driver has no gray format) and `Librga::resize_dmabuf` and
  `resize_bytes` (`IM_INTERP_AVERAGE`, `IM_INTERP_LINEAR`). Area is an integer downscale whose
  trailing partial block is cropped; what a route cannot do as asked is `Unsupported`, never a
  substitute (mainline cannot downscale bilinearly). `caps::caps(soc)` describes RK3566, RK3568,
  RK3576, RK3588, RK3562 and K230D: units, routes, size and scale limits, luma cost, and each
  filter's exactness, all `Unmeasured` until a board run. New ops `daedalus.image.resize_area`
  and `daedalus.image.resize_bilinear` with scalar references (`reference::resize_area`,
  `reference::resize_bilinear`, OpenCV `INTER_LINEAR` on 8-bit data) and `rga-v4l2` and
  `librga` fixed-function rows; vim2m checks the plumbing, the board tests are written, untested
  on board.
- **`Op::only_on(backends, reason)` in the acceleration inventory.** For a fixed-function node
  whose CPU kernels and callers are another repo's: the other backends' columns, and `wired`
  without call sites, are n/a, and the reason is listed with the op. `Op` gains the `only` and
  `only_reason` fields.

- **`Status::Deferred` in the acceleration inventory.** For a cell that is possible but
  postponed (waiting on a board, a vendor toolchain or an SDK), with its reason. It renders as
  **deferred**, has its own count in the totals, and `accel-matrix --release` still fails on it,
  as it does on gaps. An `Op::could_use` candidate that cannot be done is marked with
  `Kernel::missing(op, backend, Status::Impossible(reason)).with_extensions(exts)`; this is now
  documented and tested. Adding the variant breaks exhaustive matches on `Status`.

- **Core-tuned kernel variants get their own matrix column.** Mark a variant with
  `Kernel::with_core("a55")` and `accel-matrix render` gives it a column of its own
  (`neon64[a53]`, `neon64+dotprod[a55]`), placed after the shared one. The column passes only on
  the variant's own evidence (`Evidence::variant("a55")`), and the shared column ignores it.
  Plain `with_variant` variants (GPU compute and fragment forms) still share the backend's
  column. The registry dump carries `"core"`; older dumps read as `false`.

- **NEON32 CRC-32** (`crc32::neon_vmull_p8`). ARMv7 has neither CRC instructions nor a 64-bit
  carry-less multiply. Each 64x64 multiply of the PCLMULQDQ folding is built from eight
  `vmull.p8` (8x8-bit polynomial multiplies), combined by Horner's rule over the bytes. It
  passes conformance under qemu (Cortex-A7) at every length from 0 to 300 bytes plus 1 KiB to
  1 MiB. This closes the last gap in the backend matrix (112 pass, 20 impossible, 2 untested:
  the RGA cells, which need the board). `crc32()` stays on slice-by-8 on ARMv7 until the
  kernel is measured faster on an ARMv7 core. None is available: the Raze kernel has no
  AArch32 support. `--test conformance crc32_throughput -- --ignored` measures it.
- **`new_host_cached` gives cached memory on devices without a host-cached memory type**
  (v3dv): a system dma-heap buffer imported for the GPU and mapped cached for the CPU, with
  `DMA_BUF_IOCTL_SYNC` around each CPU access (`gpu-dmabuf`). On the Raze, reading 16 MiB back
  takes 3.6 ms instead of 13.1 ms (memcpy: 2.9 ms), and 1 MiB 0.30 ms instead of 0.63 ms. It
  passes the validation layer with no messages.
- **`gpu_bench` sections `[dispatch-floor]` and `[readback]`, and memory types in
  `[allocation]`.** `[dispatch-floor]` compares one dispatch through `GpuKernels` with
  hand-written wgpu. On V3D the CPU cost is equal (23 µs record and submit, p50), and so are the
  allocations (29, plus 2 for `run`'s ticket). `[readback]` compares the dma-heap readback with
  the host-cached one. `[allocation]` lists the Vulkan memory types and a memcpy baseline.

- **Raw Vulkan access for external SPIR-V kernels** (feature `gpu-vulkan-raw`, Linux,
  `daedalus_gpu::raw_vulkan`). `GpuContextHandle::raw_vulkan()` returns the `ash` entry,
  instance, physical device, device, queue, queue family and queue index. `GpuStorage::raw_vk_buffer()`
  returns the `VkBuffer`, offset and size, imported camera frames included.
  `GpuContextHandle::submit_raw` records commands into a wgpu command buffer between full memory
  barriers, submits it on Daedalus's queue and returns a `GpuTicket`. Daedalus does every
  submission because wgpu serializes its own submissions behind a lock that outside code cannot
  take. The module docs give the contract (who submits, barriers, waiting, lifetimes). Tested on
  the CM5 under the validation layer: a raw `ash` compute pipeline reads an imported dma-heap
  frame in place, with no validation messages. `scripts/ci.sh vvl` runs these tests.
- **`GpuStorage::dmabuf_import_len` / `GpuFrameImports::required_len`**: the dmabuf size an
  import needs, which is the frame plus the driver's buffer padding. That padding is 256 B on
  v3dv and none on RADV, NVIDIA and lavapipe. A shorter dmabuf cannot be imported: Vulkan binds a
  buffer only to memory covering its whole requirement.

- **`GpuTexture::copy_from_storage` / `upload_from_storage`**: copy a frame held in a
  `GpuStorage` (rows `stride` apart, for example an imported camera dmabuf) into a sampleable
  texture with `vkCmdCopyBufferToImage`, which v3dv runs on its TFU for whole-frame copies: 0.49 ms
  at 640x480 and 0.71 ms at 1280x720 on the CM5, 6-12x faster than a shader copy. Strides that
  aren't multiples of 256 are folded (`k` rows per 256-aligned intermediate row, one direct copy,
  then an unfold fragment pass; 0.43 ms against 0.27 ms direct for 640x480 GREY on the CM5), with
  row-by-row re-pitching only when the fold would be too wide.
  Unaligned rows and offsets, and short buffers, are refused.
- **`#[node(device_from = "<port>.<field>")]`** (`NODE_DEVICE_FROM_META_KEY`). For nodes that
  choose CPU or GPU through a config value and keep CPU-typed ports, the engine reports each
  instance's device from its const config: `explain_plan()` shows `gpu` / `config backend = gpu`
  with a GPU backend and `cpu` / `config backend = gpu, no GPU backend: CPU fallback` without
  one. `compute` is unchanged, so no GPU segment, transfers or crossing edges are planned.
- **GPU node guide** (`docs/gpu-nodes.md`) and the `gpu_fragment_node` example: a 3x3 box filter
  whose device is a per-node config value. Its GPU path keeps a fragment pass, textures and a
  binding in node state, submits without waiting and emits results on later ticks. It falls back
  to the CPU with a message. The CPU path runs in `ci.sh smoke`, and both paths match the CPU
  reference.
- **`GpuStoragePool`**: per-frame scratch `GpuStorage` from power-of-two size classes, with a
  cap on idle bytes and stats (`created`, `reused`, `dropped`, `reuse_rate`). In a 1000-frame
  test with varying sizes it stays under its cap and reuses over 90% of takes.
- **`FragmentPass` bindings, rectangles and integer formats.** `bind` → `FragmentBinding` is
  validated once and reused by `encode_bound` / `run_bound`. `FragmentRects` + `encode_rects`
  draw a list of rectangles in one instanced draw, optionally keeping the targets' contents
  (`LoadOp::Load`). Integer textures (`texture_2d<u32>`, rgba16uint, rg32uint targets) are
  tested. The built-in vertex stage now draws per-instance rectangles; a full-target draw is one
  rectangle.
- **On-disk pipeline cache: `GpuPipelineCache`, `pipeline_cache_for`.** It loads and saves
  wgpu's Vulkan pipeline cache per adapter. Once installed, Daedalus's shader-dispatch and
  `FragmentPass` pipelines use it. The driver validates the data, so a driver change starts a
  fresh cache. The backend requests `PIPELINE_CACHE` when available. On desktop, 12 fragment
  pipelines build in 3 ms instead of 82 ms with the cache reloaded.
  `save_if_grown` writes the file only when new pipelines were built.
- **Acceleration inventory: `daedalus-accel`** (no dependencies, `no_std`). Kernels register
  per op and backend in const `Registry` tables:
  - backends: scalar, portable SIMD, x86 SSE / AVX2, NEON64, NEON32, RVV, GPU (with variants)
    and fixed-function;
  - ISA extensions as their own columns (`neon64+dotprod`);
  - each entry is implemented, a gap, impossible, or not data-parallel, with a reason.

  `conform()` records CI evidence. The `accel-matrix` generator renders
  `docs/backend-matrix.{md,csv}`, checks the kernels' vector instructions in the disassembly, and
  gates on failures, regressions and unregistered SIMD/GPU code. Eidos and Styx use the same
  crate.
- **`daedalus-kernels`**: Daedalus's data-parallel CPU kernels, vectorized per ISA and proven by
  `scripts/ci.sh accel` (69 cells pass, the rest impossible with reasons or planned GPU gaps):
  - RGB/RGBA/gray conversions, LE f32, binary16 and unorm8 readback conversions;
  - byte and row copies (explicit SSE2/AVX2/NEON intrinsics, and NEON32/RVV inline asm, since
    musl's ARMv7 and RISC-V `memcpy` is scalar);
  - hex, and standard base64.

  Targets: x86_64 SSE2 with SSSE3/SSE4.1 and AVX2 chosen at runtime, AArch64 NEON, ARMv7 NEON
  (`+neon`) and RVV (`+v`), the last two checked at VLEN 128, 256 and 512.
- **The kernels run in the product.** The GPU crate's RGB/gray upload and download conversions
  (`DynamicImage` RGB8/Luma8 included, also in `#[shader]` bindings), the r32float / rgba16float
  / gray readback conversions and every row re-pitch and readback copy go through
  `daedalus-kernels`. So do base64 for `Value::Bytes` in JSON and on the FFI wire, and the
  package digest hex. The `base64` and `half` dependencies are gone. Each op lists its call
  sites, and the matrix's `wired` column checks them: a proven kernel that nothing calls is a
  gap, and the old scalar code coming back fails the gate.
- **`GpuKernels` / `GpuKernel`**: every `daedalus-kernels` op as a compute kernel on
  GPU-resident `GpuStorage` (`kernels.wgsl`), bit-exact with the CPU kernels (integer-only
  binary16 and unorm8 conversions, `firstLeadingBit` for subnormals). There is also
  `copy_with_engine` (`copy_buffer_to_buffer`). The conformance tests record each op's GPU pass
  time against the CPU kernel per adapter in the matrix. No op is "impossible" on the GPU any
  more; the caller selects per device.
- **`GpuDeviceProfile`** (`GpuContextHandle::device_profile()`): the adapter's family, from its
  vendor id and name. Families are Mali Bifrost and Valhall, VideoCore, Adreno, PowerVR,
  Apple, desktop and software. The profile also gives tile-based or immediate-mode
  (`prefer_fragment_passes`), the warp width and a default workgroup size. Mali G610/G52 follow
  ARM's documented architecture; the board runs confirm them (`docs/testing.md`, board
  checklist). `drm_format_mod_arm_afbc` / `is_arm_afbc` cover the AFBC modifiers that Rockchip
  blocks produce.
- **`daedalus_kernels::crc32`**: CRC-32 (IEEE) with x86 PCLMULQDQ folding and the AArch64 CRC
  instructions, chosen at runtime, and RVV Zvbc folding (`vclmul`) in `+v,+zvbc` builds, and
  slice-by-8 elsewhere. It checksums MCU plan blobs in
  `daedalus-mcu-build`. The device keeps its 1 KiB-free bitwise CRC for flash.
- **Portable SIMD kernels** (`daedalus_kernels::portable`, `core::simd`): every op in portable
  SIMD, built only with `--cfg daedalus_portable_simd` on the pinned nightly
  (`nightly-2026-10-15`) in `ci.sh accel`. They give the inventory's portable_simd column real
  evidence. The product builds on stable without them; a cfg rather than a cargo feature keeps
  `--all-features` stable builds working.
- **Fragment forms of the GPU kernels** (`GpuKernels::encode_fragment` / `run_fragment`): each
  fragment of an `R32Uint` target computes one output word with the same WGSL word functions as
  the compute form (`kernels_common.wgsl`), then the target is copied into the destination
  buffer. This is the form for tile-based GPUs (V3D, Mali), where fragment passes beat compute.
  It covers every op but base64 decode, bit-exact, and is recorded as the `fragment` variant.
- **`daedalus-rga`**: Rockchip's RGA 2D engine through the V4L2 mem2mem interface
  (`rockchip-rga`). It scales, crops (`VIDIOC_S_SELECTION`) and converts pixel formats (RGB,
  BGR, ABGR/XBGR, GREY, NV12/NV21, I420). It works dmabuf in and dmabuf out with no CPU copy
  (`Rga::blit`), or on byte buffers (`Rga::convert`). Sessions are cached per configuration. The
  ioctl structs are size-checked against the kernel ABI. Tested end to end against the kernel's
  `vim2m` (conversion, scaling, `dma_heap` dmabufs); on a board it is written, untested. This is
  the binding Daedalus and Styx share, and it depends only on libc. RGB↔RGBA are registered as
  fixed-function `rga-v4l2` cells.
- or multi-planar, with ioctl struct
  sizes checked against the kernel ABI. It is tested end to end against the kernel's `vim2m`.
  `v4l2::Rga` drives Rockchip's RGA (`rockchip-rga`) for RGB↔RGBA. That part is written, untested
  on board; gray needs librga.
- **CRC-32 on the GPU** (`GpuKernels::crc32`): table-driven CRCs of 4 KiB chunks on the device,
  combined on the host with the new `daedalus_kernels::crc32_combine` (zlib's combine). The
  result is bit-exact and measured.
- **Zero-copy GPU input and output:**
  - `GpuFrameImports` imports `SharedFrame`s (External residency, a Styx `FrameLease`) as
    storage buffers, once per dmabuf (keyed by inode, LRU-bounded). It waits on the implicit
    write fence, and gives each plane's offset and stride. The `GpuFrameInput` handle holds the
    frame's lease.
  - `GpuStorage::new_host_mapped` gives persistently mapped host-visible buffers
    (`write_mapped` / `read_mapped`, no staging copy: the UMA path). `new_host_cached` is the
    same in cached memory, for readback: 17 ms instead of 640 ms to read 16 MiB on RADV.
  - `GpuStorage::new_uninit` skips wgpu's zero-init clear.
  - `gpu_bench [allocation]` times `vkCreateBuffer`, `vkAllocateMemory`, `create_buffer` per
    memory-hint mode and the first-use clear separately.
  - The kernel measurements now report encode-to-result time (every copy included) next to the
    pass time.
  - A repeated `GpuKernels` dispatch allocates nothing beyond raw wgpu (`tests/gpu_allocations`):
    - bind groups and parameter buffers are cached per dispatch shape;
    - one status buffer is shared;
    - no pass labels (wgpu copies them per pass);
    - the layout states each binding's minimum size, which removes wgpu's per-dispatch late size
      checks.

    `GpuFrameImports` compares plane dmabufs by inode, so Styx's NV12 with dup'd plane fds is one
    import. `GpuFrameInput` keeps plane offsets inline, with no per-frame allocation.
- **Hot-path allocations** (the audit in `docs/development.md`):
  - `tick_direct_payload` caches its route per port pair, and the route's single-node part is
    `Arc`-shared: 5 allocations per call down to 1, the output.
  - `drain_payloads_into` drains into a reused vector.
  - The per-pop size estimate skips its lock when no size inspector is registered.
  - The allocation trace attributes `<daedalus_…>` trait-impl frames.
- The storage-to-texture copy is registered too: the direct copy (the TFU on V3D) and per-row
  copies as fixed-function, the fold-and-unfold pass as GPU.
- **GPU memory hints: `GpuOptions::memory_hints` / `EngineConfig::with_gpu_memory_hints`
  (`GpuMemoryHints`).** wgpu's `Performance` hints allocate a large block on the first
  allocation of each memory type (a 42 ms tick for the first readback buffer on the CM5, 3.6 ms
  on RADV; 0.23 ms with `MemoryUsage`). The default, `Auto`, now uses `MemoryUsage` on
  integrated, virtual and CPU adapters and `Performance` on discrete GPUs;
  `DAEDALUS_GPU_MEMORY_HINTS=performance|memory` overrides `Auto`.
  `WgpuBackend::memory_hints()` reports the resolved choice.
- **The engine owns the pipeline cache: `EngineConfig::with_gpu_pipeline_cache(dir)`**
  (`CacheSection::gpu_pipeline_dir`, `DAEDALUS_GPU_PIPELINE_CACHE`, engine feature `gpu-wgpu`).
  The engine opens and installs the cache when it creates its GPU device, so pipelines a node
  builds in its first tick from `ctx.gpu` hit it. Each `HostGraph` saves a grown cache after its
  1st, 2nd, 4th, ... 256th tick, on `HostGraph::save_pipeline_cache()`, and on drop.
  `HostGraph::pipeline_cache_status()` / `Engine::pipeline_cache_status()` report the file,
  bytes loaded and saved, and why no cache is in use; loads and saves are logged.
- **`GpuStorage::import_dmabuf`**: zero-copy import of a `LINEAR` dmabuf (a camera frame) as a
  storage buffer, with an optional acquire-fence wait. The import is refused with the size it
  needs when the dmabuf lacks the driver's buffer padding (256 B on v3dv). Import costs
  16-29 µs per frame on the CM5.
- **GPU time per pass: `GpuTimer`, `GpuTimed`, `GpuTimeSource`.** Timestamp queries at the
  start and end of a compute or render pass, resolved and read back without waiting
  (`try_elapsed`). Without timestamp support it falls back to submit→completion time.
  `FragmentPass::run_timed` times a fragment pass. Nodes report the result with
  `ctx.duration_metric`.
- **`#[device]` transfers with the GPU context.** An upload function may take
  `&GpuContextHandle` as a second argument, with its download taking it too. The executor
  provides its context while it adapts an edge, so real uploads create buffers on the engine's
  device and stay plan edges. Also added: `PluginRegistry::register_typed_device_transport_with_gpu`
  and `daedalus_runtime::with_current_gpu`. Without a context the transfer returns
  `TransportError::Unsupported`. The one-argument form is unchanged.
- **Resolved device per node in `explain_plan()`.** `RuntimeNodeExplanation` gains `device`
  (`NodeDevice::Cpu`/`Gpu`) and `device_reason`. The engine records them in node metadata
  (`daedalus.device`, `daedalus.device_reason`, and `daedalus.fallback_from` on a CPU fallback).
  The consumer's choice (per-node `compute`, the engine's GPU backend) is what decides, and every
  fallback is visible: a CPU fallback names the GPU node it replaced, and a `gpu_preferred` node
  on the CPU says so. The `Display` node line now reads `(<compute>, on <device>)`.
- **`RuntimePlanExplanation::device_round_trips()`** lists CPU→GPU→CPU chains across nodes
  (`DeviceRoundTrip`: the upload edge, the nodes on the device, the download edge), so a test can
  assert that a GPU pipeline has none.
- **Fragment-pass nodes: `FragmentPass` and `GpuTexture`.** A full-screen fragment pass reads
  `GpuTexture` inputs (`textureLoad`) and `GpuStorage` parameters, and writes one or more render
  targets. The tested formats are r8, rgba8, r32f and rg32f. It returns a `GpuTicket`.
  `GpuTexture` is a persistent 2D texture with `write` and polled or blocking readback through
  one reused staging buffer. On V3D this is the GPU path that beats the CPU: 10-13x faster than
  compute. A graph test checks a two-target pass against its CPU pair; `gpu_bench` compares the
  helper with a hand-written pass (equal on desktop).
- **Readback into caller buffers.** `GpuStorage::start_read(offset, len) -> GpuReadback`
  copies into a reusable staging buffer without waiting. `GpuReadback::try_read_into(&mut [u8])`
  delivers the data on a later tick, and `GpuStorage::read_into` is the blocking form. No buffer
  is allocated per download; `staging_buffers_created()` stays at one for sequential readbacks.
- **`GpuTicket` and `GpuInFlight<T>`**: non-blocking GPU completion. A node submits on one
  tick, keeps the ticket in its state, and on a later tick `try_complete()` (one non-blocking
  poll) tells it the work finished, so no tick waits on a fence. `GpuInFlight` caps the number of
  outstanding submissions, pops results in submission order and can drop the oldest. A graph
  test checks that the result arrives on a later tick, and that no tick takes more than a
  quarter of the GPU work's time.
- **`GpuStorage`**: a persistent, sized storage buffer on the engine's device that a node keeps in
  its `state(T)` across ticks (Eidos TSDF volume, cost-volume scratch). It provides `new`
  (zeroed), `with_data`, `write`, `ensure_size`, `buffer()` for the node's own pipelines, and
  `binding()` for Daedalus dispatches. A graph test keeps one buffer for 100 ticks.
- **`GpuError::InvalidInput`** for caller-supplied sizes and ranges (breaking: a new variant).
- **Compute limits and features in `GpuCapabilities::compute`** (`GpuComputeCaps`,
  `GpuComputeFeatures`). It reports workgroup storage, invocations and size, the storage buffer
  binding size, and storage buffers and textures per stage. It also reports which of f16, f64,
  int64, int64 atomics, subgroups and timestamp queries are enabled. The wgpu backend now
  requests these optional features whenever the adapter has them (before, it requested none).
  `GpuComputeCaps::WEBGPU_DEFAULTS` gives the portable baseline, and `fits_workgroup` checks a
  workgroup size against the limits. `gpu_probe` prints all of it.
- **`GpuContextHandle::wgpu()`** returns the engine's wgpu device and queue (`None` on a
  non-wgpu backend). A node can build pipelines and persistent buffers on the same device the
  other GPU nodes and the transfers use, instead of opening its own.
- **`gpu_bench` example** (`daedalus-gpu`, `gpu-dmabuf`). It reports the compute limits and
  the Vulkan features wgpu does not surface (64-bit and f16 types, atomics, timestamps, storage
  image formats). It measures dispatch latency, kernel throughput against the CPU on 1 thread
  and on every core, and the per-frame cost of a `LINEAR` dmabuf camera frame reaching a compute
  kernel. That cost is measured two ways: as a storage buffer imported with
  `VK_EXT_external_memory_dma_buf` (zero-copy), and as an image import plus a copy.
- **Drop stale const inputs when refreshing a stored graph.** `Graph::refresh_ports_with` (and
  `PluginRegistry::refresh_graph_ports_with`) take a `RefreshPolicy`; with `drop_stale_consts`, a
  const input on an input port the node's declaration no longer has, and no edge uses, is dropped
  with the port, and `NodePortRefresh::dropped_consts` lists the (port, value) pairs. The default
  (`refresh_ports`) still keeps such ports for the planner to reject (`PortExtra`). Hit by HeliOS
  when Eidos folded node knob ports into constants.
- **Refresh stale port lists of stored graphs.** `Graph::refresh_ports(&CapabilityRegistry)`,
  `GraphDocument::refresh_ports` and `PluginRegistry::refresh_graph_ports(&mut Graph)` add the
  ports a node's declaration gained since the graph was saved (with their `inputs.<port>.*` /
  `outputs.<port>.*` metadata) and drop the ones it lost unless an edge or const input uses
  them; they return a `PortRefreshReport` of what changed. Before this, a plugin adding a port to
  a node made every stored graph using that node fail to plan with "graph is stale; regenerate
  ports from registry", with no API to do so (hit by HeliOS when Eidos added `map`/`save`/
  `saved_map` to `mapping.live_map`).

- **`HostGraph::set_frame_overhead_sampling(every)`**: record frame-path overhead for one tick in
  `every` (default 1, every tick). Recording costs about 13 µs per tick of the 22-node AprilTag
  graph on a CM5; HeliOS keeps it on with its metrics, so sampling 1 in 16 saves nearly all of it
  while the window's percentiles still describe typical ticks. Additive, kept across
  `enable_frame_overhead` and `reset_frame_overhead`.

- **Owned frames across plugins, both ways (`SharedFrame`):**
  `daedalus::transport::SharedFrame` is an owned, cheaply cloned `daedalus:frame` handle: a
  `#[node]` input and output on the Rust-ABI and stable plugin paths, kept in node state,
  returned, or made by the node (`SharedFrame::new(Arc<impl FrameSource>)`); `as_view()` gives
  the `FrameView` existing code reads. A separately built (stable-ABI) plugin gets a camera
  frame from the host and returns one without copying pixels; the owner is released exactly
  once whether the plugin drops the frame during the call, keeps it, returns it, panics or is
  reloaded (the facade's `dylib_frames` test, with a drop counter, plus a memfd-backed
  dma-buf-style frame on Linux). Frames a stable-ABI plugin makes keep its library loaded.
  `NodeIo::push_frame` and `Payload::foreign_local` back it; `SharedFrame` implements
  `FrameSource` so a node can retag a frame's metadata without touching its pixels. A 1080p NV12
  frame in and back out costs about 1.5 µs per tick through the stable path (0.9 µs statically),
  where a `Vec<u8>` copy of it costs about 1.4 ms (`--test dylib_frames_overhead`).
- **`daedalus:frame` v3: colorimetry and capture metadata.** `FrameColorimetry` (ITU-T H.273
  primaries, transfer and matrix plus `ColorRange`, with libcamera-named presets) and keyed
  metadata (`frame_meta`: exposure, analogue/digital gain, color temperature, frame duration,
  sensor id, a vendor key range) through three accessors appended after v2's. `FrameSource`
  gains defaulted `colorimetry` / `metadata_u64` / `metadata_bytes`, so v2 providers compile
  unchanged. `daedalus-gpu`: `ExternalFrameDescriptor` carries the colorimetry, and
  `from_shared_frame` builds an import that keeps the frame (the producer's buffer) alive.
- **Benchmarks on `dev`:** `.github/workflows/bench.yml` runs on pushes to `dev` as well as
  `main` (same path filters). Its baseline is the head commit of the newest earlier run on the
  same branch, benched again in the same job: the first dev runs compared against the previous
  run's artifact and flagged 15-45% slowdowns on every bench, including after a docs-only
  commit, because hosted runners differ in speed between jobs.
- **`daedalus_gpu::driver_lock()` and `with_driver_lock(f)`** (`gpu-wgpu`): the process-wide
  lock that serializes Daedalus's Vulkan instance, adapter and device creation. A host that
  creates wgpu/Vulkan instances itself takes it around that setup. It is not reentrant; see
  "Host-created Vulkan instances" in `crates/gpu/README.md`.
- **Daemon protocol version.** `ping` answers `pong` with `protocol_version` (1, the new
  `daedalus_daemon::service::PROTOCOL_VERSION`), so a client can check the protocol before use.
  The graph document's `schema_version` and its bump policy are documented in
  `docs/node-authoring.md`.
- **Port display metadata: `label`, `unit`, `group` and `advanced` on a port.** `#[port(...)]` on a `NodeConfig` field and `port(...)` on a node input accept a human `label`, a `unit`, a `group` (stage or section) and an `advanced` flag (`advanced = true`, or a bare `advanced`). They are recorded as the node declaration's metadata keys `inputs.<port>.label`, `.unit`, `.group` and `.advanced` (a boolean), next to `inputs.<port>.description`, and reach the registry and catalog JSON with the node. All are optional; existing declarations compile and carry the same metadata as before. `docs/node-authoring.md` describes them, and the `port_metadata` integration test checks them on a config and on a registered node.
- **`daedalus_data::json::to_plain_json_with(value, PlainJsonOptions)`** and
  `PayloadInspection::to_json_with`: plain JSON with rendering options. `BytesEncoding::Base64`
  renders `Value::Bytes` as one standard, padded base64 string instead of an array of byte
  numbers, about a third of the size and much cheaper to parse for large buffers (point clouds,
  meshes). The default options render exactly what `to_plain_json` does, which is unchanged.
- **`NodeError::InputDropped(String)`** (code `input_dropped`, `NodeError::is_input_dropped`): a
  node returns it when this tick's input is unusable (e.g. a camera frame that failed while its
  rows were being written). It never fails the tick, under any `NodeRecovery` and on direct host
  routes: the node's outputs for the tick are dropped, it is not retried, it stays out of
  `ExecutionTelemetry::errors`, and it is counted in the new `RecoveryTelemetry::dropped_inputs`.
  The stable plugin ABI carries it as `status::INPUT_DROPPED` (6); hosts that predate it read it
  as a failure.
- Hot graph reload for host graphs (`daedalus-engine`): `HostGraph::reload`, `reload_document`
  and `reload_with` swap a recompiled graph in between ticks onto the same host bridges. Node
  state and managed resources carry over for node instances whose key and node type are
  unchanged; host ports the new graph lacks drop their queued and held values; the frame-overhead
  window continues. A compile error leaves the running graph untouched. Returns a
  `HostGraphReload` report. `ExecutionDomain::reload_graph` reloads a member in place.
  Runtime: `OwnedExecutor::{state_store, set_state_store}`, `StateStore::successor_for_reload`,
  `HostBridgeHandle::retain_ports`, `HostBridgeManager::is_same`. See the Hosts section of
  `docs/node-authoring.md`.

- **frame-bench hardware counters per tick**: the `apriltag_mirror` example reports, per tick,
  instructions, cycles, L1D and L1I read misses, and dTLB read misses (LLC read misses where the
  PMU has them; each event degrades to `n/a` when the kernel refuses it). `MIRROR_TOUCH_KB` pauses
  the counters for its own cache walk, so the numbers are the runtime's and the handlers' non-touch
  work; `MIRROR_COUNT_TOUCH=1` counts the walk too.

- **frame-bench instruction profile**: `MIRROR_PROFILE=1` on the `apriltag_mirror` example samples
  the calling thread's instruction pointers and call chains (`perf_event_open` with `PERF_SAMPLE_IP`
  and `PERF_SAMPLE_CALLCHAIN`, user space only) over the measured ticks, and prints the hottest
  symbols (self, inclusive and leaf-with-caller), resolved through `/proc/self/maps` and `nm`.
  Call chains need frame pointers (`RUSTFLAGS="-C force-frame-pointers=yes"`); without them the
  callers are not reliable.

### Fixed

- A dmabuf import that the driver refuses no longer double-closes its fd on NVIDIA's proprietary
  driver. NVIDIA 595.104.02 closes the fd inside a refused `vkAllocateMemory`, against the spec,
  and the import then closed it again: a debug abort (`IO Safety violation`), and in release a
  close of a number another thread may have reused. The refused fd is now released without a
  close on that driver (`settle_refused_fd`, also in the `gpu_bench` frame import). Other drivers
  are unchanged. The dmabuf tests skip with the driver's reason when NVIDIA refuses an import.
- `scripts/ci.sh vvl` pins the Vulkan loader to RADV when `VK_ICD_FILENAMES` is unset, since the
  loader can otherwise pick the NVIDIA card here.
- **The detector frame's allocation budget holds on every run.** `graph_frame_allocations` read
  9.05 per frame on one parallel row in CI (run 41777030660, `Parallel/Hardware`), and 11 on an
  adaptive row under load. A pooled worker telemetry built its node table, its in-flight slots,
  its payload texts and its lifecycle buffers the first time its worker met them, and which worker
  runs which segment varies; an adaptive frame that went parallel after warm-up created its
  telemetries inside the measured window. Now the pool's telemetries are created and sized to the
  plan when the executor is prepared (`prewarm_worker_pool`, which `HostGraph::prepare` calls), a
  parallel run keeps one per worker call, and each run's absorb gives every pooled telemetry the
  union of the payload texts and reserves the run's record count. No budget changed. Measured
  under load on the test binary: 200 of 200 runs pass (before, 10 of 200 failed on the adaptive
  rows).

### Removed
- `crates/daedalus/assets/*.wgsl`: six shaders no code referenced since 0.1.0.

- Runtime: `NodeError::retryable` and `ExecuteError::retryable`, which nothing called. Recovery
  policies (`NodeRecovery`) decide retries from node metadata instead.
- The Styx camera examples (`04_async/styx_camera_graph`, `07_gpu/gpu_node` and its
  `contrast_boost.wgsl` shader), the `daedalus-plugins-framelease` example plugin, and the `styx`
  dependency with its `styx-camera-example` and `gpu-styx` features. Styx owns its Daedalus
  integration (`styx_core::daedalus`, Styx's `daedalus` feature). CI no longer installs
  `libcamera-dev`, and `Cargo.lock` no longer contains Styx's crates.
- The Rayon worker pool and its `executor-pool` feature (facade, engine, runtime, daemon
  and `engine-full`'s pool), plus the workspace `rayon` dependency. `RuntimeMode::Parallel` and
  `Adaptive` use the parked worker threads that `threads` enables, and the engine's prewarm
  (`prewarm_worker_pool`) starts those. `engine-full` is `engine` + `metrics`. Measured on one
  tree with and without the pool (frame-bench, 15 and 12 interleaved runs, medians): a flat
  detector in parallel mode took 7.54 µs parked against 7.70 µs on Rayon, and a 16-way fan-out of
  no-op stages took 31.6 µs against 51.7 µs. The facade's stripped release binary is 1.5% smaller
  (a clean build of the same example is about 2.6 s faster), and `cargo tree` loses rayon,
  rayon-core, crossbeam-deque, crossbeam-epoch and either. See "Choosing a runtime mode" in
  `docs/development.md`.
- Public API review (`docs/api-review.md`): the public functions below had no caller in the
  workspace (tests, examples and benches included), in the HeliOS, Styx and Eidos repositories,
  or in `docs/`, and are removed. Replacements where one exists: `channels::policy::build_bounded`,
  `build_unbounded` and `build_broadcast` (the whole `daedalus-core` `channels::policy` module) are
  `bounded_with_behavior`, `unbounded_with_behavior` and `broadcast_with_behavior`;
  `Engine::build_runtime_plan` is `daedalus_runtime::build_runtime`; `RunnerPool::ffi_telemetry` and
  `set_ffi_telemetry` are `RunnerPool::with_ffi_telemetry`. The rest have no replacement.
  - `daedalus-core`: `channels::policy::build_bounded`, `channels::policy::build_broadcast`, `channels::policy::build_unbounded`.
  - `daedalus-data`: `descriptor::DescriptorBuilder::gpu_hints`, `prelude::DescriptorId::namespaced`, `typing::TypeRegistry::lookup_type_by_rust_name`, `typing::TypeRegistry::register_type_capability`, `typing::TypeRegistry::snapshot_by_rust_name`.
  - `daedalus-engine`: `Engine::build_runtime_plan`.
  - `daedalus-ffi-host`: `RunnerPool::ffi_telemetry`, `RunnerPool::set_ffi_telemetry`.
  - `daedalus-gpu`: `Backing::shared_arc`, `Compute::as_cpu`, `Compute::as_gpu`, `Compute::is_gpu`, `GpuContextHandle::alloc_download_buffer`, `GpuContextHandle::skipped_summary`, `shader::BindingSpec::storage_texture`, `shader::BufferOut::write_bytes_no_readback`, `shader::ShaderRunOutput::storage_r32f`, `shader::ShaderRunOutput::storage_rgba16f_image`.
  - `daedalus-registry`: `prelude::CapabilityRegistry::replace_serializer`, `prelude::CapabilityRegistry::serializer_decl`, `prelude::NodeFire::write_to_metadata`.
  - `daedalus-runtime`: `ExecutionContext::record_frame_scratch_bytes`, `ExecutionContext::record_warm_cache_bytes`, `ExecutionTelemetry::is_lifecycle_enabled`, `ExecutionTelemetry::record_edge_gpu_transfer`, `FrameOverheadWindow::latest_mut`, `HostBridgeHandle::drain_arcs`, `MetricsLevel::is_hardware`, `NodeHandle::alias_id`, `NodeIo::from_single_input`, `NodeIo::get_typed_mut`, `NodeIo::push_arc_as_default`, `NodeIo::push_as_default`, `NodeIo::push_payload_default`, `PortHandle::node_alias_id`, `PortHandle::port_id`, `Profiler::export_json`, `Profiler::telemetry_mut`, `RuntimeEdgePolicy::is_latest_only`, `RuntimeResources::record_frame_scratch_bytes`, `RuntimeResources::record_warm_cache_bytes`, `StateStore::set_typed`, `StreamGraph::export_profile_json`, `StreamGraph::profiler_snapshot`, `graph_builder::GraphBuilder::node_port`, `io::CowArcMut::as_arc`, `plugins::PluginInstallContext::manifest_mut`, `plugins::PluginRegistry::install_and_freeze`, `plugins::PluginRegistry::take_runtime_transport`, `plugins::PluginRegistry::take_transport_capabilities`.
  - `daedalus-transport`: `Payload::cache_current`, `Payload::resident_arc`.
- `crates/rga` (`daedalus-rga`) and `crates/vglite` (`daedalus-vglite`): the RGA and VGLite
  bindings moved to Lemnos, and both crates are deleted from this workspace. Nothing in this
  repo depended on them except `crates/kernels`' fixed-function rows. Styx builds on
  `daedalus-rga` and must move to `lemnos-drivers-rga` (Lemnos cf3a4ac) too.

### Changed

- `daedalus::dylib::STABLE_ABI_VERSION` is 4 (was 2). The stable path installs versions
  `STABLE_ABI_MIN_VERSION` (2) to 4 (`stable_abi_supported`); `StableHandlers` is unchanged, so
  `PLUGIN_ABI_VERSION` stays 11. Only a version-4 plugin's `daedalus_plugin_stable_extension` is
  read, so a version-3 plugin's consts go per call.
- **Plain values are held inline in the payload (public API change).** `Payload::plain` holds a
  `Copy` value of at most 32 bytes with alignment at most 8 in the payload, instead of in an `Arc`:
  the detector's `Stats`, `Roi`, `Track`, `Report`, `Histogram` and `FrameInfo` outputs now
  allocate nothing. The node and adapter macros route their outputs through it (a hidden
  `OutputProbe`): a `Copy` output is inline, any other is `Payload::owned` as before. A plain value
  is unique (a clone copies it) and shares no storage (`shares_storage` is false for it); `get_arc`
  on one allocates a fresh `Arc`. A `Payload` is 208 bytes (184 before), so Rust-ABI plugins are
  rebuilt against this host. The detector frame (`graph_frame_allocations`) is 0 per frame in
  serial, parallel and adaptive modes at every metrics level (8 before).
- `daedalus_runtime::collections::FastHashMap` is built on the module's `HashMap`, so `hashbrown`
  is no longer in the runtime's public API with `std`. `daedalus_data::convert::SharedConverterGraph`
  is removed: nothing used it, and it put `parking_lot` in the public API.
- **Lifecycle records share their port and payload text (public API change).**
  `DataLifecycleEvent` and `DataLifecycleRecord` hold `port: Option<PortId>` (was `Option<String>`)
  and `payload: Option<IdStr>` (was `Option<String>`), so recording an event clones a shared name
  rather than allocating. `IdStr` is `daedalus_transport::IdStr`, a `&'static str` or shared text
  that derefs to `str`. Reads through `as_deref()`, `as_str()` or `Deref` keep working; constructing
  one needs `PortId::new(..)` and `IdStr::new(..)` (or `IdStr::from_static(..)`) where a `String` was
  assigned before. The JSON text is unchanged (`"port": "in"`, `"payload": "Payload(i64)"`), and so
  are `AdapterPathReport` and the adapter-path fields, which keep their `String`s. An executor builds
  a payload's text once per type key and keeps it in the telemetry (not part of its value).

- **Telemetry recycling covers `Detailed` and `Hardware`, and the no_std smoke graph runs at the
  default metrics level.** The per-edge metrics and the in-flight transport table are reused
  across ticks: `edge_metrics` keeps its edges zeroed (a `BTreeMap` cleared each tick freed its
  leaves) and drops the ones no run touched, and the in-flight table is a dense slot table.
  Host outputs are drained inside the tick, before its telemetry is taken, so the queue waits
  of the outputs a tick hands to the host are in that tick's telemetry (they were reported one
  tick late, or dropped once telemetry was recycled). `examples/nostd_smoke` no longer pins
  `MetricsLevel::Off`; its tests assert zero allocations per tick at `Off`, `Basic`, `Timing`,
  `Detailed` and `Hardware`, on one thread and on four, with telemetry recycled. Allocations per
  tick on that graph, recycled and not: `Basic` and `Timing` 0 and 1 (the node-metrics table),
  `Detailed` and `Hardware` 0 and 4 (that table, two edge leaves and the in-flight leaf), and
  `Profile` and `Trace` 7 and 12 (13 at `Trace`); the next entries bring those to 0. The
  detector frame budget (`graph_frame_allocations`) is 9 at every level up to `Hardware` in serial
  mode, with the telemetry recycled, so its `Basic` term of 1 is gone. A parallel fan-out at
  `Basic` is fixed in the entry below.
- `daedalus` (`dylib-plugins`) on musl: no `libloading` and no `-ldl`. musl executables are
  static and cannot `dlopen`, and Rust's self-contained musl ships no `libdl` to link, so a
  plugin crate or host test for `aarch64-unknown-linux-musl` failed to link (`unable to find
  library -ldl`). There `PluginLibrary` loads nothing and reports
  `PluginLibraryError::Load` ("dynamic plugin loading is unavailable on musl"); the export side
  (`export_plugin!`) is unchanged. The facade's tests that load a `cdylib` are off on musl.
- `daedalus-gpu`: the queue submit lock belongs to the GPU context instead of a process-wide
  static, so a plugin `cdylib` (its own copy of `daedalus-gpu`, with its own statics) holding the
  host's `GpuContextHandle` takes the host's lock. It is a `std` mutex, which waits on the mutex
  word itself; a `parking_lot` mutex parks waiters in a table that each copy of the crate has
  separately. New: `GpuContextHandle::submit(commands)` (a submit under the lock, for wgpu users
  sharing the device, such as Eidos's direct `queue.submit` calls), `GpuContextHandle::submit_lock()`
  (`SubmitLock` / `SubmitGuard`, to hold around other submitting calls), and
  `GpuBackend::wgpu_submit_lock`. `GpuTicket::submit_on` is crate-private; `GpuTicket::submit`
  covers it.
  `daedalus-gpu`'s `plugin_submit` test checks it across a real `cdylib`
  (`crates/gpu/tests/plugin`, a fixture crate): the plugin sees the host's lock, its submit
  waits while the host holds it, and the host's `RawSubmitter` runs alongside the plugin's wgpu
  uploads, submits and readbacks; `ci.sh vvl` runs it under the validation layer.
- `daedalus-gpu`: the dma-buf basics come from Lemnos (`lemnos-linux`, feature `dma`, a git
  dependency on Lemnos `dev`, Linux only): the system dma-heap behind host-cached storage on
  devices without host-cached memory (v3dv), `DMA_BUF_IOCTL_SYNC` around its CPU access, the
  dmabuf size query of imports, and `export_dmabuf_fence`. The Vulkan imports stay here. The
  package lane packages without a lockfile (`--exclude-lockfile`): `lemnos-linux` is not on
  crates.io yet, so publishing needs Lemnos published first. The lock file pins Lemnos at 2610455:
  borrowed dmabufs go through `DmaBufRef` (no descriptor duplicate per frame), and the frame
  identity and the CPU acquire-fence wait are Lemnos's too.
- `daedalus-rga`: the V4L2 mem2mem session is Lemnos's `M2mSession` (feature `v4l2-m2m`); the
  public API (`Rga`, `Layout`, `PixelFormat`, `Rect`, `find_m2m_device`) is unchanged. A crop the
  device does not take is still an error.

- **Breaks `ci.sh semver` reports against `v3.0.0` that had no entry of their own.**
  `ServiceResponse::Pong` is a struct variant with `protocol_version`. Struct literals need the new
  fields of `RuntimeSection` (`frame_overhead_window`, `blocking_backpressure`,
  `stream_error_policy`, `node_fusion`), `EngineConfig` (`gpu_memory_hints`),
  `ApplyPolicyOwnedArgs` (`stamp_enqueue`), `StableInput` (`port`, `fanin`) and
  `RuntimeEdgeExplanation` (`fused`); `ExecutionContext` has a private `node_state` field, so it
  cannot be built by a literal. `FramePlane` has no lifetime parameter any more.
  `StableValue::handle` takes 2 parameters. New variants renumber `PluginLibraryError` and
  `daedalus-mcu`'s `LoadError`, so numeric casts of them change. The features `base64`
  (`daedalus-data`), `half` (`daedalus-gpu`), `executor-pool` (`daedalus-engine`) and `rayon`
  (`daedalus-runtime`) are gone.
- **Stable-ABI plugin calls stop allocating per call on both sides.** The host and the plugin keep
  their `Arena` (now `Arena::clear`, which keeps its buffers and boxes) and call buffers across
  calls. The plugin drains its outputs into the pooled port list (`NodeIo::drain_outputs`) and
  reuses its handler io's environment (`NodeIo::shared_env` / `with_env`). Decoded inputs, and the
  host's decoded scalar outputs, are built in reused allocations (`transport::PayloadSlot`). A tick
  through a stable plugin now allocates no more on the host than the static install does: 2, 2 and 1
  allocations for scalars, `SharedBytes` and `SharedFrame` against 3, 2 and 1, where it was 3, 4
  and 3. The plugin side allocates only its handler's output payload, where it was 5, 6 and 4
  (`--test dylib_stable_allocations`, counted in the plugin by `examples/plugins/stable_abi_counted`).
  `NodeIo::take_owned` clones a shared input without first boxing it on the refused move.
  `SharedBytes::default()` no longer allocates. A failed or panicking call no longer leaves the
  host's input list or the plugin's decoded inputs behind: a frame the plugin held when it panicked
  is released.

- **Tiled dmabuf imports take 1 to 4 memory planes, whatever the format's plane count.**
  `ExternalFrameDescriptor::validate` required at least one plane per format plane, so NV12 in
  v3dv's Broadcom UIF layout (one memory plane on a Raspberry Pi 5 / CM5) was rejected before the
  backend's check against the modifier's advertised memory plane count. LINEAR still takes exactly
  one plane per format plane. Found by the first `ci.sh pi` run on a CM5 (V3D 7.1, Mesa 26.1.8):
  there, LINEAR XRGB8888 and NV12 are render/transfer only, not sampleable, so the
  linear dma-heap hardware tests now skip on such devices instead of failing, as does the tiled
  NV12 test when no modifier imports (v3dv's `vkCreateImage` rejects the explicit layout of its
  own `UIF` NV12 export).
- **`daedalus:frame` is now v3 (interface version 3, new layout hash); no ABI version
  changes.** `STABLE_ABI_VERSION` stays 2 and `PLUGIN_ABI_VERSION` stays 11: frames already
  crossed the stable ABI as foreign handles, and no descriptor, handler or `StableValue`
  layout changed. Plugins built against frame v2 keep working through the stable path (the host
  lends them its frames under the v2 identity, `frame_handle_to_v2`, and reads theirs through
  v3, `frame_handle_from_v2`); the Rust-ABI path needs a rebuilt plugin anyway. Code that
  compares `FrameInterface::info()` or `VERSION` against 2, or builds a `FrameVTable` by hand,
  must update; `FRAME_INTERFACE_V2` / `FrameInterfaceV2` name the old version.
- **`ForeignView::from_borrow` is now `from_payload`** (it receives the payload, so owned views
  such as `SharedFrame` can retain the frame). Only custom view types implement it.
- The stable boundary flags handles a plugin created and carried in a foreign payload as
  `HANDLE_FROM_SENDER` (before, only owner payloads its provider lent), so the host keeps the
  plugin loaded while such a handle may be released.

- **Breaking: dynamic plugins `PLUGIN_ABI_VERSION` 11** (rebuild every Rust-ABI plugin). Boundary
  contracts hash a schema text into `LayoutHash::for_schema`; that text was the `Debug` output
  of `TypeExpr`, which moves whenever the derived `Debug` does. It is now
  `TypeExpr::canonical_label()`, a hand-written, pinned rendering (`scalar:i64`, `list<...>`,
  `struct{"a":...}`, ...), so the layout hash of a type changes only when its shape does. The
  golden test `canonical_label_is_pinned` guards it. Every boundary layout hash changes once, so
  rebuild plugins with this host.
- Planner: equal-cost adapter paths tie-break deterministically. The search prefers the path
  with fewer adapter steps, then the one whose adapter ids sort first, lexicographically. It used
  to prefer the greatest id sequence, an artifact of the max-heap it pops from. Only the choice
  between equal-cost routes changes; the cost of the chosen route does not. The test
  `adapter_search_breaks_ties_by_path_order_on_many_adapters` now expects `a_000_in`, and the new
  `adapter_search_equal_cost_prefers_smaller_id_not_greater` and
  `adapter_search_equal_cost_prefers_fewer_steps` pin the rule.
- Engine: without a GPU backend (`GpuBackend::Cpu`), a node declared with
  `#[node(fallback = "...")]` runs its CPU fallback in place of the GPU node. Before, the GPU
  handler ran on CPU data and failed.
- Runtime: `NodeIo::take_owned_or_clone` takes a `Clone` input by value and clones it when the
  executor still shares it. Under `NodeRecovery::Retry` the executor keeps a copy of each input,
  so a plain `take_owned` on a retried node fails on its first attempt; a by-value consumer that
  uses `take_owned_or_clone` gets its input again on every retry.
- Runtime: `push_direct_edge` and `pop_direct_edge` take the executor core, edge table and slot
  access instead of `&mut Executor<H>`, so their bodies are not instantiated per handler type.
- Runtime: the serial executor's edge, fused-edge, host-bridge, predicate and recovery helpers take
  a handler-free `SerialView` of the executor instead of `&mut Executor<H>`, so each is compiled
  once rather than once per handler type. The handler-calling loop, `retry` and `run_handler` stay
  generic.
- Runtime: `RuntimeNode::metadata` is an `Arc<BTreeMap<String, Value>>`, so building an executor
  shares each node's metadata with its execution context instead of copying it. Only a node whose
  label or bundle is not already in its metadata still gets a copy of the map. The serialized
  `RuntimePlan` is unchanged. Code that builds a `RuntimeNode` wraps its map in `Arc::new`.
- Runtime: an edge push that evicts a backlog adds every evicted item to the edge's `drops`
  and pressure count (it counted one). A `KeepOldest` coalesce edge that refuses an arrival in
  its window records `EdgePressureReason::CoalesceReject` (`coalesce_reject`,
  `EdgePressureMetrics::coalesce_reject`) instead of `coalesce_replace`.
- Runtime: the executor builds its node table in one pass (one copy of the plan's nodes instead
  of a `Vec` copy plus its `Arc` copy).
- Runtime: `OwnedExecutor` holds its `Arc<RuntimePlan>` and reads edges, edge transports,
  segments, schedule order and GPU entry/exit lists from it, instead of deep-copying them at build.
- Engine: `HostGraph::frame_overhead` reads edge labels built once when recording starts, and
  `last_frame_tick` no longer allocates a per-edge vector it does not return.
- Runtime: multicam sync checks each pushed payload against a clone of the type index outside the
  bridge lock, and keeps each camera ring in a `VecDeque`, so a group commit pops its oldest
  frame in O(1) instead of shifting the ring.
- Engine: `HostGraph::tick_selected` caches the demand slice and masks of the last sink set and
  reuses them while the same sinks repeat. The telemetry demand summary is built only with
  metrics on (`OwnedExecutor::metrics_level`). Metrics-off ticks drop about half their overhead
  in the `tick_selected` micro-bench.
- Transport: `FeedOutcome::Replaced` carries `dropped`, the number of queued payloads the push
  evicted (`PushOutcome::Replaced::dropped`), instead of implying one. Host bridge stats and
  per-port stats count every evicted payload: `replaced` and the drop-reason counters grow by the
  evicted count (a `LatestOnly` push over a three-item backlog adds three), where they added one.
- Runtime: edge overflow warnings look the label up before allocating it, and the default
  `<prefix>_edge_<idx>` label is built on the stack. `ApplyPolicyOwnedArgs::warning_label` is
  `Option<Arc<str>>`.
- Runtime: `StateStore` keeps JSON and native values under one lock, so `set` and `set_native`
  take one write lock and `set` allocates a key only on first insert.
- `daedalus-frame-bench`: the detector mirror is a synthetic marker-detector bench. Its marker
  enum is `Marker` (`grid_4x4`, `grid_6x6`, `ring_16`, `ring_36`, replacing the dictionary names),
  its graphs are named after those markers, and the graph shape is unchanged.
- **GPU: the aarch64 Linux Vulkan-only default is gone.** `wgpu` backend selection now falls
  back to all backends on every target when `WGPU_BACKEND` is unset, so CM5 and other aarch64
  Linux hosts that want Vulkan must select it. Either set `WGPU_BACKEND=vulkan`, or set the new
  `GpuOptions::backends: Option<GpuBackends>` field to `GpuBackends::VULKAN`. Precedence is
  `WGPU_BACKEND` (when it names a backend), then `GpuOptions::backends`, then every backend.

- **FFI bytes travel as base64.** `BytePayload` serializes as `{"data":"<base64>","encoding":"base64"}`
  and the Rust, Python, Node and Java SDKs emit that form. Every decoder still accepts the older JSON
  number array (`"encoding":"raw"`). C++ keeps its pointer and length ABI and is unchanged.
- **Persistent-worker invokes encode and read once.** Requests are serialized into a reused buffer
  and written in one call, responses are read into a reused line buffer, and the byte counts come
  from those encodings instead of a second serialization. The pool derives the runner key once per
  call. `BackendRunner::invoke_measured` and `invoke_batch_measured` report sent and received bytes
  (defaults estimate them without buffering).
- **Breaking:** `FfiBackendTelemetry::runner_restarts` is removed. Nothing incremented it: the
  FFI runner pool rejects automatic restart policies. Node restarts are counted in
  `ExecutionTelemetry::recovery().restarts`.
- `StreamWorkerConfig` gained an `on_error` field (build it with `StreamWorkerConfig::default()`
  and the `with_*` setters), and `ExecutionTelemetry`/`TelemetryReport` gained
  `backpressure_deferrals` and `recovery`.
- **Breaking: dynamic plugins `PLUGIN_ABI_VERSION` 10 and `STABLE_ABI_VERSION` 2** (rebuild every
  plugin; a host refuses older plugins with `PluginLibraryError::AbiMismatch`). `StableHandlers`
  grew `create` (one plugin-side instance per host node instance, returned as an opaque handle,
  so `invoke` does no instance-map lookup, lock or context clone) and `unload` (drop the plugin's
  runtime before its library closes). `StableInput` and the output callback name ports by index
  into the schema node's ports (`StableInput { port, fanin, value }`, `NO_FANIN`), resolved once
  instead of searched and UTF-8-validated per call; the host caches each node instance's input
  indices by position and reuses its input and output buffers, and scalars decode without a
  `Value` detour. A `HANDLE` value's `scalar` carries `tag::HANDLE_FROM_SENDER` when the sender's
  build created it.
- **Dynamic plugin libraries close when unused.** `PluginLibrary` holds its library by
  reference count instead of leaking it: it closes once the `PluginLibrary`, its stable installs
  (and the graphs compiled from them) and the `SharedBytes` it produced are gone. A Rust-ABI
  install, or a plugin-created foreign handle reaching the host through another port type, pins
  it for good. Plugin ids, `PluginLibrary::info()`, boundary types, crate builds and foreign
  interface keys are host copies (interned once per distinct string), no longer leaked per load
  or pointing into the library.

- **Breaking: `daedalus:frame` v2** (`FrameInterface` version 2, new vtable layout hash; v1
  plugins are refused with `PluginLibraryError::ForeignInterfaceMismatch`, and a registry
  holding one version refuses the other with `PluginError::ForeignInterfaceConflict`;
  `FRAME_INTERFACE_V1` names the retired identity). Plane metadata no longer touches pixel
  memory: the vtable splits `plane_fd` / `plane_offset` / `plane_stride` / `plane_len` /
  `plane_mapping` from `plane_data` (begin CPU access: map lazily, sync for reads) and
  `plane_end_cpu_access`, so an fd-only consumer (a GPU dma-buf importer) makes the provider
  neither `mmap` nor `DMA_BUF_IOCTL_SYNC`. Offsets, strides and lengths are `u64`. New
  `format_kind` (`FrameFormatKind::{Unknown, Pixel, Bayer, Compressed}`) and per-plane
  `PlaneMapping::{Cached, Uncached, WriteCombined, Unmapped}`; `MIPI_FORMAT_MOD_CSI2_PACKED`
  and `FrameView::is_csi2_packed` disambiguate libcamera's CSI-2 modifier from MediaTek's (same
  vendor byte). `FrameSource::plane` returns metadata (`FramePlane` lost its lifetime and
  `data`), CPU bytes come from `plane_data` + `end_cpu_access`; consumers read
  `FrameView::plane_bytes(i)` / `cpu_planes()` (a `PlaneBytes` guard that ends the access on
  drop) and `planes()` is metadata only. `ExternalFrameDescriptor::from_frame_view` (Linux,
  `daedalus-gpu`) builds a dma-buf import from a `FrameView` without CPU access. Spec,
  migration table and a lazily mapped dma-buf provider sketch in
  `docs/foreign-frame-interface.md`.
- `daedalus-frame-bench`: memfd and dma-heap frames are mapped on the first CPU access instead
  of at creation, and count it (`SyntheticFrame::map_count`, `cpu_access_count`); tests prove a
  metadata/fd-only consumer never maps and a CPU consumer maps each buffer once and begins one
  access per frame and plane.
- **Dynamic plugins: `PLUGIN_ABI_VERSION` 9** (rebuild every plugin): the descriptor gained a
  `crate_builds` entry point (`CrateBuildTable`).
- **Boundary type conflicts name the crate.** `PluginLibraryError::BoundaryTypeConflict` groups
  the conflicting keys by the crate defining the plugin's type
  (`RustTypeIdentity::defining_crate`, innermost non-std crate of generics) and says that crate
  resolved differently in the plugin's build; it gained `crate_builds`, `same_crate_builds` and
  `stable_compatible`. When no node port uses a conflicting key it suggests
  `install_into_as(InstallPath::Stable)`; it never falls back to the stable path on its own.
- **Foreign providers no longer allocate per frame.** The provider's `View` adapter retypes the
  owner payload under the interface key with the provider attached
  (`Payload::provide_foreign::<O, I>`: the same typed storage seen through a provider vtable)
  instead of building a `ForeignHandle` payload, so feeding the owner type (e.g. Styx's
  `FrameLease`) into `FrameView<'_>` / `ForeignRef<'_, I>` nodes costs no allocation and no
  reference-count change per consumer edge (it was one 88-byte allocation per edge per tick;
  the adapter's p50 went from about 220 to 130 ns). Nodes borrow the owner value through the
  vtable (`Payload::foreign_borrow` -> `ForeignBorrow<'_>`, `PayloadStorage::foreign_borrow`);
  the retyped payload keeps its provider when forwarded and still answers `get_ref::<O>()`.
  Handles are built only at the stable plugin boundary (`Payload::to_foreign_handle`) and for
  owners in boundary storage. `ForeignView::from_handle` became `from_borrow(ForeignBorrow)`,
  and `ForeignRef` no longer exposes `handle()` (it may not have one). `daedalus-frame-bench`
  asserts zero runtime allocations for the owner feed at 1/4/16 stages, fanned out to 1/4/16
  consumers (`compile_frame_fanout`), and for typed `FrameView` + `&T` nodes.
- Typed nodes returning `Result<(), _>` without outputs no longer push a unit payload to a
  nonexistent `out` port (two allocations per call). `Payload::resident` builds no cache key
  when the payload has no cached residents.
- **Tick-cost pass** (detector-shaped frame bench: instructions per frame 66 000 -> 33 100,
  `graph_overhead` p50 / p99 5.3 / 10.3 -> 3.5 / 4.9 µs on x86, zero node allocations; numbers
  and a CM5 estimate in docs/development.md "Detector-shaped frame"):
  - **Payload storage is the value's own `Arc`.** `Payload::shared`/`owned`/`shared_with`
    (without a bytes estimate) retype the value's allocation as payload storage instead of
    wrapping it, so returning an `Arc`'d output allocates nothing and an owned payload is one
    allocation (the serial metrics-off detector graph frame: 31 -> 14 allocations, the host
    round trip 4 -> 2). `PayloadStorage` lost `type_key()` (the payload carries it), and a
    payload's storage strong count now includes other holders of the value
    (`is_storage_unique` is false while the producer keeps its `Arc`).
  - **Direct slots for more edges.** An edge that alone feeds its target port uses a direct slot
    for the default buffer-all policy (every payload kept in order, one inline), latest-only,
    coalescing and bounded-of-one-drop-oldest, also when its source port fans out or it has an
    adapter path (adapters run when the consumer collects, also from a slot; direct host routes
    used to skip them). Bounded edges stay queues under a graph `BackpressureStrategy`. A
    replaced slot payload records the policy's pressure event; direct-slot edges record waits at
    `Detailed`, as queues do. `explain_plan` reports the executor's own choice.
  - **Per-node contexts and state slots.** The executor builds each node's `ExecutionContext`
    and `NodeIo` environment once (rebuilt when the state store, capabilities, GPU, coercers,
    type index or clock change). Node state lives in a per-node `NodeStateSlot`;
    `ExecutionContext::take_node_state` / `set_node_state` use it without a map lookup, and
    `ExecutionContext::new` builds a context (it gained a private field). Generated handlers
    keep their state and decoded configs/constants as one tuple in the slot.
  - **Shared const inputs.** A node's const inputs are one list (`NodeConstInputs`, now
    `Vec<NodePort>`, copy-on-write on patch) each call's `NodeIo` reads after its edge inputs;
    `NodeIo::inputs()` returns an iterator over both. `ConfigCache` skips its per-port check
    while the list is unchanged.
  - `OwnedExecutor::run_in_place` borrows its core instead of snapshotting it each tick;
    `ExecutionTelemetry::warnings`/`errors` are `Vec`s. Handler dispatch and host bridge ports
    hash with an unseeded Fx hasher (`collections::FastHashMap`). `Clock::platform()` is a
    `const fn`.

- **Smaller `#[node]` expansions** (what every plugin crate compiles per node: 20% fewer
  expanded lines and 18% less build CPU for a 50-node crate). Generated node types keep
  `boundary_contracts_for` only when the node function is generic (it duplicated
  `boundary_contracts_in` otherwise); call `boundary_contracts_in` on the others. A missing
  required input fails through the new `NodeError::missing_input(port)` (same
  `InvalidInput("missing <port>")` error).
- `declare_plugin!` no longer needs `paste` in the calling crate (it goes through
  `daedalus-nodes`); plugin crates can drop that dependency.
- **Planner: malformed declaration JSON is reported.** A node's metadata entry or a port's
  constant that is not valid JSON emits `DiagnosticCode::InvalidDescriptorMetadata` /
  `InvalidConstInput` (the constant is not applied) instead of being dropped silently.
  `DiagnosticSpan::pass` is a `Cow<'static, str>` (serializes as before).
- **Registry: `RegistryError` stores a conflict once.** `conflict_key()` and `conflict_kind()`
  read the payload; `with_conflict_key` / `with_conflict_kind` are removed.
- **Planner performance:** edge adapter search runs over a per-run `AdapterIndex`
  (`CapabilityRegistry::adapter_index`, `resolve_adapter_path_indexed`) with predecessor-linked
  paths; the same chosen paths and tie-breaks, about 6x faster planning on a 3,300-adapter
  registry (debug test profile: 1.17 s to 0.19 s per plan). Port types are borrowed, structured
  transport keys are memoized per run, and the fire-mode lint indexes incoming edges.
- **Dead APIs removed:** `daedalus_core::channels::policy::{build_async, build_newest, EdgePolicy,
  ChannelAsync}`, `bounded_with_metrics_and_behavior`, and `daedalus_data::NamedTypeRegistry::
  resolve_opaque`. `PushOutcome::Replaced` is now `{ old, dropped }`. `IdStr::new` takes
  `impl AsRef<str>`, so borrowed text is copied with one allocation.
- Public API review (`docs/api-review.md`): these items are no longer public API. Nothing in the
  workspace outside its own module, nor the HeliOS, Styx or Eidos repositories, calls them. The
  hidden ones stay callable (semver-public) under `#[doc(hidden)]` and are not for applications;
  the `pub(crate)` ones are internal, and the test-only ones exist only under `cfg(test)`.
  Hidden:
  - `daedalus-core`: `metrics::InMemoryMetrics::into_sink`, `metrics::metrics_sink`.
  - `daedalus-data`: `json::decode_value`, `json::encode_value`, `proto::to_proto_type`, `schema::to_json_schema_string`, `schema::type_to_json_schema`.
  - `daedalus-engine`: `DomainGraphStats::mean_run`, `DomainStats::forward_time`, `Engine::compile_runner`, `Engine::plan_plugin_registry`, `ExecutionDomain::has_pending_input`, `HostGraph::bind_payload_output`, `diagnostics::render_error`, `diagnostics::render_plan`, `diagnostics::render_runtime`.
  - `daedalus-ffi-cpp`: `cpp_plugin_package`.
  - `daedalus-ffi-node`: `node_plugin_package`.
  - `daedalus-ffi-python`: `python_plugin_package`.
  - `daedalus-gpu`: `AcquireFenceWait::has_timeout`, `Compute::into_gpu`, `GpuBlockInfo::for_format`, `active_backend`, `shader::GpuState::new_stateful`, `shader::ShaderContext::dispatch_by_name`, `shader::ShaderRunOutput::storage_rgba8_image`, `shader::ShaderRunOutput::texture_handle`, `shader::ShaderRunOutput::texture_r32f`, `shader::ShaderRunOutput::texture_r32f_gray_u8`, `shader::ShaderRunOutput::texture_r32f_image`, `shader::ShaderRunOutput::texture_rgba16f`, `shader::ShaderRunOutput::texture_rgba16f_image`, `shader::ShaderRunOutput::texture_rgba8`, `shader::ShaderRunOutput::texture_rgba8_image`, `shader::TextureOut::from_input`, `shader::TextureOut::from_input_ctx`, `shader::max_inflight_submissions_per_device`, `shader::set_max_inflight_submissions_per_device`, `upload_bytes`.
  - `daedalus-mcu-build`: `plan_specs`.
  - `daedalus-nodes`: `bundle_demo::install_bundle`.
  - `daedalus-planner`: `PluginRequirement::check_version`.
  - `daedalus-runtime`: `PayloadInspection::to_display_value`, `RuntimePlan::active_nodes_for_sinks`, `StateStore::get_checked`, `alloc_probe::current_scope`, `const_coerce::deserialize_value`, `graph_builder::GraphBuilder::connect_from_nested`, `graph_builder::GraphBuilder::connect_handles`, `graph_builder::GraphBuilder::connect_ports`, `graph_builder::GraphBuilder::connect_to_nested`, `graph_builder::GraphBuilder::edge_metadata`, `graph_builder::GraphBuilder::node_metadata_map`, `graph_builder::GraphBuilder::node_metadata_map_by_id`, `graph_builder::GraphBuilder::single_node_io`, `graph_builder::GraphBuilder::single_node_ports`, `graph_builder::GraphCtx::node_as`, `graph_builder::GraphScope::connect_policy`, `plugins::PluginRegistry::register_smart_adapter`, `plugins::PluginRegistry::register_typed_transport_adapter`.
  - `daedalus-transport`: `PressurePolicy::is_single_slot`.
  Now `pub(crate)` (or test-only):
  - `daedalus-core`: `clock::TickClock::default_start`.
  - `daedalus-data`: `named_types::NamedTypeRegistry::export_policy_for`, `typing::TypeRegistry::has_type_capability`, `typing::TypeRegistry::register_type_capabilities`, `typing::TypeRegistry::snapshot_type_capabilities`, `typing::TypeRegistry::type_capabilities`.
  - `daedalus-engine`: `Engine::execute_scoped`.
  - `daedalus-ffi-core`: `PluginLockfile::from_package`, `package_platform_dir`.
  - `daedalus-ffi-cpp`: `CppPointerLengthAbi::bytes_view`, `cpp_in_process_backend_config`.
  - `daedalus-ffi-host`: `DecodedInvokeResponse::events_at_level`, `DecodedInvokeResponse::value_output`, `DecodedInvokeResponse::wire_output`, `HostInstallPlan::from_schema`.
  - `daedalus-ffi-node`: `node_worker_backend_config`.
  - `daedalus-ffi-python`: `python_worker_backend_config`.
  - `daedalus-gpu`: `GpuContextHandle::alloc_upload_buffer`.
  - `daedalus-registry`: `prelude::NodeExecutionKind::from_metadata_value`, `prelude::NodeExecutionKind::write_to_metadata`.
  - `daedalus-runtime`: `DemandSlice::active_edge_count`, `DemandSlice::active_node_count`, `ManagedByteBuffer::clear_live`.
  - `daedalus-transport`: `BoundaryStorage::is_taken`, `Payload::boundary_shared`.

### Added

- `conformance` feature on `daedalus-ffi-core` (canonical fixture specs and generators, off by
  default) and `daedalus-ffi-host` (the fixture harness). SDK crates enable it in dev-dependencies.
- Node SDK: `wireImage` and `wireHandle`; `fromWire` decodes `image` and `handle` kinds.
- `daedalus-ffi-host` exports `InvokeWireBytes`.
- Java SDK: `Wire` decodes base64 bytes; `uint` values above `Long.MAX_VALUE` are documented as
  the negative `long` with the same bits (print with `Long.toUnsignedString`).
- Node failure recovery: `NodeRecovery::{Fail, SkipFrame, Retry { max, backoff }, Restart}`
  per graph (`Executor`/`OwnedExecutor::with_recovery`, `EngineConfig::with_recovery`) or per
  node (`with_node_recovery`, `try_set_node_recovery`, or `daedalus.node.recovery` metadata,
  `NODE_RECOVERY_META_KEY`). `Restart` resets the node's state, resources and custom metrics
  (`StateStore::reset_node`); recovering policies catch handler panics. Recoveries are counted
  in `ExecutionTelemetry::recovery()` (`RecoveryTelemetry`, boxed in the `recovery` field and
  `None` until a recovery happens) and in the telemetry report.
- Stream workers keep running after a failed tick with `StreamErrorPolicy::Continue`
  (`StreamWorkerConfig::with_error_policy`, `EngineConfig::with_stream_error_policy`; default
  `Stop` keeps the old behaviour); `StreamWorkerDiagnostics::tick_errors` counts failed ticks.
  `StreamGraph::map_executor` configures a stream graph's executor.
- Blocking backpressure for bounded queue edges: `with_blocking_backpressure(true)`
  (`EngineConfig::with_blocking_backpressure`) defers a producer whose bounded output queue is
  full to a later tick instead of dropping or erroring; `ExecutionTelemetry::backpressure_deferrals`
  counts deferrals. See "Blocking Backpressure" in `docs/runtime-diagnostics.md`.
- Parallel scheduling priority: `with_node_priority` / `try_set_node_priority` or
  `daedalus.node.priority` metadata (`NODE_PRIORITY_META_KEY`) orders ready segments (highest
  first, FIFO among equals) without allocating per frame.
- **Dynamic plugin hot reload** for development loops: `PluginLibrary::reload` loads the
  rebuilt file (through a private copy; `PluginLibrary::load_reloadable` loads that way from the
  start) and swaps every stable install to it through a generation-counted slot;
  `HostGraphReloadExt::reload_plugin` does it between two ticks of a graph, so all its nodes
  switch at that tick boundary. Node state resets across a reload (never migrated); the old
  build is released, then unloaded and closed, once its node instances and buffers are gone
  (`PluginLibrary::live_generations`, `generation`). A rebuild that changes the installed
  interface (plugin name, node ids or ports, dependencies, foreign interfaces) is refused with
  `PluginLibraryError::ReloadIncompatible`, and Rust-ABI installs with
  `PluginLibraryError::NotReloadable`. `docs/dynamic-plugins.md` ("Reloading"); the facade's
  `dylib_reload` test rebuilds `examples/plugins/stable_abi` between loads.
- **`SharedBytes`** (`daedalus::SharedBytes`, type key `daedalus:shared_bytes`): an immutable,
  reference-counted byte buffer that crosses the stable plugin boundary by reference both ways
  (as a `daedalus:bytes` foreign handle, `dylib::BytesInterface`): the plugin reads the host's
  buffer in place and returns slices of it or buffers of its own without a copy; a buffer the
  plugin produced keeps that build loaded while the host holds it. `Vec<u8>` ports still copy
  once (no shared allocator). `docs/dynamic-plugins.md` ("Zero-Copy Bytes").

- **Node fusion.** Chains of single-consumer CPU nodes run as fused units: a fused edge (its
  source port feeds only it, its target port has no other producer, a direct slot without
  adapters, CPU-only non-host-bridge ends, a consumer that fires on any input and runs right after
  the producer) hands the payload straight to the consumer instead of through its slot, and each
  unit is one segment for the parallel and adaptive schedulers. Node calls, errors, skips and
  per-node metrics are unchanged (`crates/runtime/tests/fusion_equivalence.rs` compares fused and
  unfused runs over random DAGs). On by default; `EngineConfig::with_node_fusion(false)`
  (`DAEDALUS_NODE_FUSION=0`), `Executor`/`OwnedExecutor::with_node_fusion(false)` or node metadata
  `daedalus.node.fusion = false` (`NODE_FUSION_META_KEY`) turn it off. `explain_plan()` shows
  `RuntimeEdgeExplanation::fused` / `fusion_block` (`FusionBlock`) and
  `RuntimePlanExplanation::fused_units`; the frame-overhead report counts `fused_handoffs` (per
  tick and per edge), `EdgeMetrics::fused_handoffs` at `Detailed`. Detector-shaped frame bench:
  instructions per frame 33 100 -> 32 300, `graph_overhead` p50 3.35 -> 3.04 µs; 16-stage chain
  43 000 -> 38 200 instructions (x86_64, "Node fusion" in `docs/development.md`).
- **Execution domains: shared upstream graphs across graphs** (`daedalus_engine::ExecutionDomain`).
  A domain owns several separately compiled `HostGraph`s on one thread and links host outputs
  to host inputs (`link(from, port, to, port, LinkMode)`): one `tick()` runs each graph with
  pending input once, upstreams first, and feeds every linked input an `Arc` clone of the output
  (no copy, no allocation per tick), so a camera's preprocessing runs once per frame for every
  detector graph. Link modes `Latest` (default, latest-only input), `All` and `Held`; domain
  inputs fan one push out to several graphs (`route_input`, `push`, `push_payload`); graphs are
  added, removed, linked and unlinked between ticks without recompiling; a failing graph keeps
  its error (`last_error`) and only the graphs downstream of it are skipped for that tick, their
  fed inputs cleared. `explain()` shows tick order, the fan-out links (types, zero-copy) and
  shared nodes; `stats()` counts runs, failures, skips and the runs, node runs and time sharing
  avoided; `enable_frame_overhead` / `frame_overhead()` give each graph's report plus domain
  totals. `GraphBuilder::shared_input` (host-bridge metadata `HOST_SHARED_INPUTS_KEY`) declares
  a host input whose payload other graphs share, so the planner gives by-value consumers a copy
  as it does for held inputs.
- **Structural sharing**: `ExecutionDomain::load_shared(engine, registry, graphs)` (and
  `load_shared_documents`) finds the node subgraphs several graphs compute identically from the
  same host inputs (exact structural equality: node id, constants, metadata, wiring, host input
  types and policy) and compiles them once into a `shared` upstream graph linked to the rest of
  each graph (`daedalus_planner::split_shared_upstream`). Only nodes marked deterministic and
  side-effect free are shared: `#[node(shareable)]` or node metadata
  `NODE_SHAREABLE_META_KEY` (`"daedalus.node.shareable"`). The frame-bench detector's mask prep
  and quads stages are `shareable`.
- `daedalus-frame-bench`: shared-preprocessing domains (`compile_shared_detectors`,
  `compile_structural_detectors`, `compile_separate_detectors`, `run_domain_bench`) and the
  `shared_detectors` example; tests assert preprocessing runs once per frame, outputs equal
  separate detectors', and no copies or runtime/node/host allocations per frame.
- **Multiple cameras into one graph** (`daedalus_runtime::host_bridge::multicam`, re-exported
  by `daedalus-engine`). `MultiCamera::synchronized(host, ports, SyncConfig { window, timeout,
  partial, max_buffered, stamp })` returns a thread-safe `SynchronizedCameras` feeder: camera
  threads `push(camera, payload)` (or `push_stamped`), frames are matched by stamp
  (`frame_timestamp` / `frame_sequence` read `daedalus:frame` metadata without CPU access,
  `source_timestamp::<Owner>` / `source_sequence::<Owner>` the owner frame type, or any
  `fn(&Payload) -> Option<u64>`) after per-camera offsets (`set_offset`, clock skew), buffered
  out of order in a preallocated sorted buffer per camera, and each group is committed to the
  camera ports under one bridge lock (a tick sees it whole). An incomplete group resolves when a
  missing camera skipped the frame, its `timeout` elapses or a buffer is full, by
  `PartialPolicy::{Drop, TickPartial, HoldLast}`; a newer group supersedes an untaken one, and
  late frames are refused. `MultiCamera::independent(host, ports, IndependentConfig { trigger,
  max_age })` holds each camera's latest frame and ticks per arrival of the trigger cameras
  (`IndependentCameras`). Both implement `CameraFeed` (`next_deadline`, `poll_timeout`,
  `expire`); `HostGraph::drive_cameras_blocking` and `HostGraph::tick_ready_cameras` (Linux)
  service the deadlines, so a stalled camera resolves without another arrival. Telemetry:
  `MultiCameraStats` (complete/partial/dropped/superseded groups, timeouts, overflows, late and
  dropped frames, max skew) with `last_group: CameraGroup` (contributing and reused cameras as a
  `CameraSet`, anchor stamp, skew). Pushing, grouping and committing allocate nothing and copy
  no frame. Layout recipe in "Multiple Cameras" in `docs/node-authoring.md`.
- **Triggering held inputs**: `HostBridgeHandle::set_triggering_held_input(port)` makes a held
  port that also triggers ticks: a push leaves it pending until a tick takes the value, so a
  burst of pushes is one tick on the newest value, and the value stays held for later ticks.
- **Held host inputs.** A held input keeps its last pushed value across ticks: every tick
  delivers it to the port's consumers (an `Arc` clone, no copy or allocation) until a push
  replaces it or `clear_input` drops it, so frame ticks see context such as resource state or an
  IMU sample without the host re-pushing it. Declare it with `GraphBuilder::held_input(port)`
  (host bridge metadata `daedalus.host_held_inputs`, `HOST_HELD_INPUTS_KEY`; carried by graph
  documents) or `HostGraph::set_held_input` / `HostBridgeHandle::set_held_input`
  (`is_input_held`, `clear_input`). Held pushes never wake inbound waiters, so they never trigger
  a tick on their own; a held value counts as present for required inputs and `fire = "all"`.
  The planner branches held inputs for consumers taking them by value, as for fan-out.
- **Atomic multi-port pushes.** `HostGraph::batch()` / `HostBridgeHandle::batch()`
  (`HostInputBatch`: `.push(port, value)`, `.push_payload(port, payload)`, `.commit()`) and
  `push_batch([(port, payload), ...])` enqueue every value under one bridge lock and wake waiters
  once, and ticks now take all host inputs of a bridge under one lock, so `drive_blocking` never
  sees part of a batch. A value failing the type check rejects the whole batch before anything is
  queued (`HostBatchRejected { index, port, error }`, also `EngineError::HostBatch`); otherwise
  `HostBatchOutcomes` holds each value's `FeedOutcome`. Batches of up to four values do not
  allocate beyond their payloads, and commits count as host pushes in frame-overhead reports.
  See "Context Inputs: Held Ports And Batched Pushes" in `docs/node-authoring.md`.
- **Host input policies on built and loaded graphs.** `HostInputPolicy { Queued, Held }` with
  `Graph::set_host_input_policy(host, port, policy)` / `host_input_policy(host, port)` and the
  same on `GraphDocument`, so a loaded document can mark inputs held before it is compiled
  (needed for by-value consumers, which only get the planner's branch when held is declared
  before planning). `host` is an alias or a `NodeRef` (`HostNode`); unknown bridges and ports are
  typed `HostInputError`s (`UnknownHost`, `NotAHostBridge`, `UnknownPort` listing the bridge's
  inputs). `Graph::host_bridges()` and `host_input_ports(host)` list them. The policy is stored in
  `daedalus.host_held_inputs` through `set_host_input_policy(&mut metadata, port, policy)`, which
  `GraphBuilder::held_input` now uses too; the document schema is unchanged.
- **Pollable inbound fd (Linux, `std`).** `HostBridgeHandle::inbound_fd()` /
  `HostGraph::inbound_fd()` return an `InboundFd` (`AsFd + AsRawFd`, an `eventfd` created on
  first request) that becomes readable when non-held input is queued (a batch signals once), the
  bridge closes, or waiters are woken (`HostGraphStopHandle::stop`), so hosts can wait on
  Daedalus input in the same `poll(2)`/`epoll` as their own descriptors (a Styx `FrameClient`).
  `InboundFd::clear()` resets it before draining, `rearm()` signals it again while input is still
  pending, and `HostGraph::tick_ready()` does one clear/tick/rearm turn without losing wakeups.
  Without a requested fd a push pays one atomic load; with one, a burst costs one `write(2)`
  until cleared. `EngineError::Io` reports fd creation failures. Elsewhere `InboundWaiter`
  remains the hook: it is a `Future` any waker can poll.
- **Crate build info.** `CrateBuildInfo { name, version, features }`, captured in the owning
  crate with `crate_build_info!()` (plus a two-line `build.rs` exporting `CARGO_CFG_FEATURE`) and
  registered with `#[plugin(.., crate_build)]` or `PluginRegistry::register_crate_build`. Dynamic
  plugins export theirs; `PluginLibrary::crate_builds` / `crate_build_diff(&registry)`
  (`CrateBuildDiff`: version and missing/extra features) compare them with the host's, and
  boundary type conflict errors include the difference, e.g. ``crate `styx_core` 0.4.0: host
  features `framelease,v4l2`, plugin features `framelease` (missing in plugin: v4l2)``. The
  example plugin registers its build as the reference. See "Diagnosing Boundary Type Conflicts"
  in `docs/dynamic-plugins.md`.
- **Frame-path overhead.** `HostGraph::enable_frame_overhead(window)` (or
  `EngineConfig::with_frame_overhead`, `DAEDALUS_FRAME_OVERHEAD`) records every tick at any
  metrics level, allocation-free, into a rolling window; `HostGraph::frame_overhead()` returns a
  `FrameOverheadReport` (p50/p99/max/mean and a `Histogram` per row, a text table via `Display`,
  JSON) breaking each tick into host-bridge push and take time, inject, input collection,
  zero-copy vs copying adapters, handlers, node framing, output drain and dispatch, plus
  `graph_overhead = tick - handlers`, per-edge queue and adapter time, and per-tick counters
  (zero-copy/copying adapter runs, copied bytes, fan-out `Arc` clones, GPU uploads/downloads).
  `last_frame_tick()` returns one tick's `FrameTickSample`. The runtime side is
  `daedalus_runtime::FrameProbe` (`OwnedExecutor::set_frame_probe`) and `FrameOverheadWindow`;
  host bridges time feeds and takes with `HostBridgeHandle::set_io_timing`/`take_io_time`.
- **Allocation probe** (feature `alloc-probe`): `daedalus::alloc_probe::CountingAllocator`, a
  global allocator wrapper counting allocations per scope; the executor marks runtime work and
  node handler calls, host-bridge feeds and takes are host work, and the frame-overhead report
  shows runtime, node and host allocations per tick.
- **`explain_plan` copy flags.** `RuntimeEdgeExplanation::copies_frame` (a frame-like payload whose
  adapter path copies or changes residency) and `crosses_residency`, the plan-level
  `copying_edges`/`crossing_edges` lists, and a `Display` for `RuntimePlanExplanation` (one line
  per node and edge plus the flagged edges). `RuntimeEdgeTransport::copies_data`,
  `crosses_residency`, `device_transfers`, `carries_frame` and `AdaptKind::copies_data` /
  `is_device_transfer` expose the classification.
- **`daedalus-frame-bench`** (`crates/frame-bench`, unpublished): synthetic `daedalus:frame`
  sources in external memory (dma-heap or memfd), a chain of no-op `FrameView` stages,
  `run_frame_bench` for any host graph, the `frame_chain` criterion bench and example, and a test
  that the steady-state external frame chain makes no copies and no runtime, node or host
  allocations per tick.
- `daedalus-frame-bench` detector graph (`compile_detector`, `DetectorShape::{Flat, Group}`,
  the `detector` example): five typed stages with the parameter shapes of a staged marker
  detector, as a flat graph and as an embedded-graph group node, with a test asserting zero
  runtime, node and host allocations per frame and identical expanded edges.
  `FrameBenchRun::instructions_per_frame` (user-space instructions through `perf_event_open`,
  `InstructionCounter`).
- **`daedalus-gpu` async texture transfer.** `GpuAsyncBackend::upload_texture_async` and
  `read_texture_async` (wgpu backend) move texture upload and readback off the calling thread,
  alongside `upload_buffer`/`read_buffer`; `GpuState::read_async` reads a state value without
  blocking. `ShaderRunOutput::into_texture_rgba8` returns the RGBA8 image without cloning the bytes.

### Fixed
- **The no_std smoke crate builds with `metrics` on.** It built `daedalus-runtime` and
  `daedalus-engine` with `default-features = false`, so every metrics level above `Off` recorded
  nothing there and its zero assertions above `Off` held trivially. It enables `metrics` now, and
  the numbers in the docs are measured with it on.

- `daedalus-runtime`: `GraphBuilder` shares its capability registry, so connecting through the
  host bridge or a nested graph, and `GraphCtx`'s builder steps, no longer copy the whole node
  registry per edge. With 2000 registered nodes, a host-bridge connect went from about 24,600
  allocations to 15 (`tests/connect_allocations.rs`). Eidos measured the copies at about 95% of
  its graph-install time (8.4 G instructions per start).
- `daedalus-gpu`: the per-device caches (pipelines, staging pools, the installed pipeline cache)
  are keyed by the device's identity instead of the address of the `wgpu::Device` handle passed
  in. A clone of the device now finds the cache installed for it (`pipeline_cache_for`,
  `pipeline_cache_path_for` returned `None`), and a later handle at a reused address can no
  longer pick up another device's entries.
- **dmabuf storage imports are valid Vulkan.** `GpuStorage::import_dmabuf` (and the frame import
  cache built on it) allocated the whole dmabuf as a *dedicated* allocation, which must equal the
  buffer's memory requirement and bind at offset 0 (`VUID-VkMemoryDedicatedAllocateInfo-buffer-02965`,
  found by the validation layer on v3dv). The allocation is now dedicated only when the driver
  requires it, and then only for a buffer that covers the whole dmabuf.
- **wgpu adapter selection falls back.** When the best adapter's device cannot be created (out
  of memory on a GPU another process fills, a lost device), `select_backend` now tries the next
  adapter by rank instead of failing.

- **dmabuf import could hang the GPU on NVIDIA.** A fence that was not a `sync_file` (a pipe)
  was imported as a `SYNC_FD` semaphore. RADV rejects that import, but NVIDIA accepts it, and
  the GPU then waited forever. The acquire path now checks for a `sync_file`
  (`SYNC_IOC_FILE_INFO`) before the import, and other fences take the documented
  non-`sync_file` fallback.
- `SimpleBufferPool` could hand out a freed buffer smaller than the request when both fell in
  the same power-of-two bucket; it now only reuses buffers at least as large.
- **`Coalesce { strategy: KeepOldest }` edges kept the newest value instead of the oldest.**
  Every coalescing edge that alone fed its target port was routed through a direct slot, which
  always replaces, so `KeepOldest` behaved like `KeepNewest` and its window was ignored. Only
  `KeepNewest` (or a zero window) uses a direct slot now; `KeepOldest` with a window stays on a
  queue, which rejects arrivals inside the window.
- **`NodeRecovery::Retry` works for `#[node]` inputs taken by value.** The executor keeps a copy of
  each input for later attempts, so the input is shared and a plain move failed with "missing
  <port>". Generated handlers now take by-value inputs through `NodeIo::take_owned_with`, which
  moves an unshared input as before and clones a shared one of a `Clone` type.
- **`daedalus-gpu` readback and throttling waits.** Sync shader readbacks issue every map before a
  single GPU wait (N bindings cost one round trip, not N). Submission throttling waits on the oldest
  submission directly instead of adding a 5 ms timed poll to each throttle event. Sync and async
  texture readback copy rows straight from the mapped slice, and `read_texture` uses the staging
  pool.
- **`daedalus-gpu` buffer pools no longer over-reuse.** Temp and staging pools reuse a buffer only
  when its allocation is between 1x and 2x the request, and readbacks map only the bytes they need.
  The dmabuf import caches Vulkan format and modifier queries per device.
- **The first parallel frame allocated and spawned threads.** The worker pool was created lazily
  on the first parallel run, and only with `executor-pool` was it created when the graph was
  configured. Under `Adaptive`, which switches modes on measured timings, that first run could
  land mid-stream, so `Adaptive` frames spiked by about 2 allocations under load (the
  `graph_frame_allocations` budget flaked). The pool is now started and warmed when a graph is
  configured for `Parallel` or `Adaptive` in every threaded build: each helper runs its
  port-buffer warm-up and is parked before the configure call returns.
- **Adaptive mode no longer reads the calling thread's work as dispatch overhead.** A parallel
  frame's overhead was its wall time minus the segment time spread evenly over the workers, so
  when late workers left the calling thread most of the queue, its own segments were booked as
  dispatch, and the model fell back to serial. Overhead is now the wall time minus the larger of
  the frame's critical path and its busiest thread's segment time, which each worker sums
  without allocating. Late-worker regression test in `adaptive_mode`.
- **`adaptive_mode` tests were flaky under load.** Their mode decisions depended on real sleeps
  and wall-clock timings, and on which thread happened to drain the queue. They now run on an
  injected per-thread virtual clock, and the parallel frames gather their gang of handlers so
  every run has one segment per thread. The real-time speed check is `#[ignore]`d. Test-only:
  no runtime behaviour changed.
- **Several instances of one stateful node shared one state.** `#[node(state(..))]` state,
  `ExecutionContext` resources and custom metrics were keyed by node id, so two instances of a
  node in one graph (two tracked groups' `track_plan`, two frame-difference nodes) shared one
  tracker or previous frame, and their metrics were attributed to each other. They are now keyed
  per instance (`node_instance_keys`): by node id when it is unique in the graph (unchanged), else
  by `id@label`, or `id#index` when the labels don't tell the instances apart.
  `ExecutionContext::node_id` is that instance key; a node's type id is `RuntimeNode::id`.
- **Several instances of one node id ran in index order, not dependency order.** The planner's
  `topo_order`, `schedule_order` and `gpu_segments` metadata named nodes by id, and the runtime
  bound each id to the first unused instance with that id, so an instance fed by a later-index
  instance of the same node ran first on missing inputs (empty outputs, no error; e.g. two
  detector groups in one graph). The metadata now holds node indices (`node_index_list`,
  `metadata_node_indices`; `schedule_priority` entries gain a `node` index), and `build_runtime`
  keeps the dependency-resolved order of `RuntimePlan::from_execution`. Plans or tools that wrote
  these keys as id strings must write indices.
- A parallel worker running several segments recorded only the first at the configured metrics
  level (the rest at the default level, losing `Detailed` node transport metrics).

- Queue edges record their enqueue-to-dequeue wait in `EdgeMetrics` at `Detailed` (only direct
  slots did).
- Runtime and engine tests no longer assert on wall-clock time, so a loaded machine cannot fail
  them: `stream_graph` gates its slow handler instead of sleeping, the engine's concurrency probe
  waits for overlap instead of a deadline, and the stalled-camera test
  runs on a manual clock. Waits that remain carry a 30 s safety bound. `scripts/ci.sh -j` runs
  the `test` and `lean` lanes as soon as they are built.

- **`PressurePolicy::Coalesce` implements its window and `KeepOldest`.** Both were ignored, so
  every `Coalesce` behaved as `LatestOnly`. `KeepOldest` now keeps the value that opened the group
  and rejects arrivals within `window` (`DropReason::DropNewest`); `KeepNewest` is unchanged. See
  `CoalesceStrategy`.
- **Replacing a multi-item queue reports every dropped item.** `LatestOnly` and `Coalesce` over a
  FIFO backlog returned one evicted item and hid the rest. `PushOutcome::Replaced` is now
  `{ old, dropped }`, and `PushOutcome::dropped()` counts every item a push removed.
- **Async senders could be stranded by a receive.** Bounded senders and receivers shared one
  notification, so the receive that freed space could wake a parked receiver instead, and the
  blocked sender waited forever. They now use separate notifications.
- **`Newest` reports `stats().depth` 1 while its slot holds a value** (it was always 0).

### Maintenance

- **Smaller `daedalus-runtime` codegen for every dependent build.** The generic adapter
  registration (`register_transport_adapter_fn_with_options`, `register_branch_payload_adapter`,
  `register_boundary_type`, the built-in numeric widenings and serializers) now boxes the closure
  and runs one non-generic body, instead of compiling that body once per closure type (46 copies
  for the built-ins alone). `daedalus-runtime`'s LLVM IR drops 7.8% (883k to 814k lines) and a
  release rebuild of transport and runtime takes about 6% less CPU (64 to 60 s). Plugins calling
  these functions get the same saving per call site. `daedalus_transport::SharedAdapterFn` and
  `AdapterTable::insert_shared` are doc-hidden plumbing for it.

- **`examples/mcu_blink` no longer plans in its build script.** Its `build.rs` planned the graph
  with `daedalus-mcu-build` as a build dependency, which compiled the planner, registry, runtime
  and transport a second time for the host (a cold `cargo build --workspace --all-targets`
  dropped from 381 to 322 CPU s, and `scripts/ci.sh mcu` from 85 to 68). The planner output is now checked in under
  `examples/mcu_blink/generated/`, written by `cargo run -p daedalus-mcu-blink --example generate`
  and compared by `tests/generated.rs` (run by `scripts/ci.sh mcu`). The firmware sources are
  byte-identical; `docs/mcu.md` describes the pattern.
- **Removed the planner's orphaned plan goldens** (`crates/planner/tests/goldens`): no test has
  read them since the planner rebuild (509283e), so they had drifted from real plans. The runtime
  plan goldens and the planner's pass unit tests cover plan structure.
- **Removed the Docker example suite** (`ci-docker` workflow, `testing/docker`, the
  `docker_examples` test): it ran facade examples that no longer exist, and the
  `daedalus-examples` binaries already run natively in `scripts/ci.sh smoke` (CI workspace job)
  on the pinned toolchain.
- **Leaner builds and CI.** The `dev` profile keeps line tables only and no dependency debug
  info (a clean workspace test build: 22% less CPU, half the disk). The facade dropped its
  unused `wgpu`, `pollster`, `bytemuck` and `paste` dependencies (wgpu/naga/ash were compiled
  into every non-GPU facade test build and the trybuild project). Integration tests share one
  binary per crate (`tests/it`). `scripts/ci.sh` lost `check` (the same graph as clippy's) and
  `examples` (it tested a facade without examples); `clippy` lints `--all-features`; the wasm
  smoke modules build in the dev profile. CI groups jobs by crate graph (macro UI and dylib
  tests in one `facade` job, all check-mode work in `docs-and-lints`) and saves caches only on
  the default branch. Details and measurements in docs/testing.md and docs/development.md.
- **Faster local loop.** `scripts/ci.sh -j` runs the subcommands as parallel lanes (one target
  directory each, one shared jobserver, timing-sensitive tests run last), `quick` lints and tests
  only the packages changed since the upstream branch and their dependents (sharing the full
  `clippy` and `test` builds), `full` runs everything CI runs, and `doc` builds rustdoc with
  `-D warnings` (CI calls it). `smoke` builds the example binaries with the workspace test graph
  instead of rebuilding about 30 crates per `cargo run -p` feature set. Two tests that failed
  under load are fixed: two transport tests shared a drop counter (4 failures in 200 runs), and
  a runtime stream test checked the worker state after the worker could already have drained
  its inputs. docs/development.md recommends a target directory on a local SSD (an incremental
  rebuild took 172 s with it on a USB hard disk, 28 s on NVMe).
- **Toolchain 1.99.0, MSRV 1.99.** `rust-toolchain.toml`, the CI workflows and the examples
  Docker image pin Rust 1.99.0; `rust-version` is 1.99 and every crate now inherits it (it was
  declared for the workspace but not applied to the packages).
- **Dependencies on their newest releases** (`cargo upgrade --incompatible --recursive`,
  `cargo update`). New majors: `syn` 3 (the proc-macro crates), `spin` 0.12 (same `no_std`
  features, `portable_atomic` on targets without compare-and-swap), `libloading` 0.9,
  `sha2` 0.11, `base64` 0.23, `pollster` 1.0 and `pyo3` 0.29 (declared, unused). `wgpu`/`naga`
  30, `ash` 0.38, `criterion` 0.8, `proptest` 1.11, `image` 0.25 and `tracing` 0.1 were already
  current. Nothing is kept back (docs/development.md, "Dependency Policy").
- New Rust 1.99 / clippy lints fixed in code: `as_chunks` instead of constant-size
  `chunks_exact`, `Waker::noop()` in tests, `mem::take` for drained waiter lists, no deprecated
  `core::u32::MAX` in `#[adapt]` expansions.
- The MCU blink firmware's compiled size dropped with the toolchain: 3168 -> 3072 B
  (`thumbv7em`), 3716 -> 3392 B (`thumbv6m`); RAM unchanged (`MCU_COMPILED_SIZE`,
  docs/mcu.md).

- **MCU: plan blobs `FORMAT_VERSION` 2, with a CRC-32 trailer.** The last four bytes of a blob
  are the CRC-32 of the rest; `Interpreter::load` checks it before reading the plan and reports
  `LoadError::Checksum { expected, found }`, so truncated or corrupt transfers no longer surface as
  `Malformed`. `daedalus_mcu::loaded::crc32` computes it. Version 1 blobs are refused with
  `LoadError::Version`; regenerate them with `daedalus-mcu plan`. Loaded firmware grows 164 B
  (M4F) and 100 B (M0+).
- **MCU: queues store values without an `Option` tag per slot.** `daedalus_mcu::Queue` keeps
  `[MaybeUninit<T>; N]` and drops its live values. Compiled firmware drops 48 B of static RAM
  (160 to 112 B) and 76 to 84 B of flash; tunable firmware drops the same RAM and 0.8 to 0.9 KiB
  of flash.

- `#[node]` expansions emit the string metadata (`summary`, `description`, port descriptions and
  access modes) as one static table and one insertion loop per node, not one insertion per entry.
  A 50-node fixture expands to 1.135 MB -> 1.123 MB (-1.1%); the expanded line count and the build
  CPU (about 0.77 s) are unchanged within noise.
- `starter.print` takes a `value: i32` input and logs it (`tracing` with the `tracing` feature,
  else stdout). `starter.gpu_copy` is removed: it returned `Ok(())` without copying anything, and
  this crate has no GPU payload or context to copy with. `utils.add` returns
  `NodeError::InvalidInput` when the sum overflows `i32`, instead of panicking in debug builds and
  wrapping in release builds.
- `daedalus-frame-bench`: the detector and frame_chain examples run the runtime mode named by
  `FRAME_BENCH_MODE` (`serial`, `parallel`, `adaptive`), and the new `fanout` example runs one
  frame through `FRAME_BENCH_FANOUT` no-op stages (default 16). The pool comparison in
  `docs/development.md` used these.
- Internal: merged duplicate code, with no change in behaviour, public API or ABI:
  - The GPU chain union-find, copied in the planner's schedule and GPU buffer passes, is one
    planner helper.
  - The close rule of the bounded, unbounded, newest and broadcast channels is one
    `CloseBehavior::closes`.
  - The 32 configuration and lifecycle methods shared by `Executor` and `OwnedExecutor` expand
    from one macro (`executor_config_api`), so both keep them as inherent public methods.
  - The plugin and node macros share one `node_struct_ident`.
  - The frame-bench examples share one `env_usize`.
  - Test fixtures: one stable-ABI cdylib builder (`daedalus` tests), one set of executor test
    handlers and `test_node` (`runtime` tests), and one increment handler and `INT_KEY` (`engine`
    tests).

### Performance
- **The detector frame allocates 8 per frame (was 9): `exposure` borrows its `String` constant.** The
  node took `zone: String` by value, so each call cloned the decoded constant; it now takes
  `zone: &String` (the node macro does not accept `&str` for a constant). That allocation belongs
  to the node's own signature, not to the runtime, and `const_cache.rs` is unchanged. The budget
  rows are lowered to the measured 8. The remaining 8 are payloads: six node outputs (`Stats`,
  `Roi` twice, `Histogram`, `Track`, `Report`) and two adapter `FrameInfo`s, each one `Arc`. Reusing
  node outputs' allocations was tried with a per-output `PayloadSlot` in the node's state, and
  reverted: a slot keeps a reference to the allocation while the payload is downstream, so every
  consumer sees shared storage (`gpu_fallback` failed: `drain_owned` needs a unique payload). Reuse
  without that needs a design change: small plain-old-data outputs held inline in the payload, or
  a return buffer the consumer hands back. Not done here.
- **Adapter lifecycle records allocate nothing at `Profile` and `Trace`.** A record on an adapter
  edge formatted its step list, its detail and its port text, 75 allocations per frame on the
  detector graph (`graph_frame_allocations`), all on its metadata-only adapter edge. Now the path's
  step ids and detail are built once per edge (`AdapterText`, kept by the telemetry and rebuilt if
  the edge's path changes), a record clones handles, and pooled telemetries build theirs when the
  pool is prewarmed. The detector frame is 9 per frame at `Profile` and `Trace` in serial and
  parallel modes, as at every other level (the payloads and the owned `String` clone). The budget
  rows are added, not loosened. Public API change: `DataLifecycleEvent::adapter_steps` and
  `DataLifecycleRecord::adapter_steps` are now `AdapterSteps` (dereferences to `[AdapterId]`,
  serializes as the same array of step ids), and `detail` is `Option<IdStr>` (serializes as the
  same string), where both were `String`s. `AdapterPathReport` is unchanged. The JSON of a lifecycle
  event is pinned by `lifecycle_event_json_keeps_adapter_steps_and_detail_text`.
- **`Profile` and `Trace` allocate nothing per tick once telemetry is recycled.** Their lifecycle
  records shared a `String` port and a formatted `Payload(..)` text per event, so each event
  allocated (7 per tick on the no_std smoke graph, recycled). A record now clones the port's name,
  and the payload text is built once per type key, so a recycled tick allocates nothing at any
  level, on one thread and on four (`examples/nostd_smoke` asserts this at `Profile` and `Trace`
  too). Not recycled: `Profile` 10 per tick (12 before), `Trace` 11 (13 before).


- **A parallel fan-out allocates nothing per frame.** Each worker records into a telemetry the
  worker pool keeps, and the run absorbs them into the owned executor's recycled telemetry; a
  parallel or adaptive run takes that telemetry in place of a fresh one. The detector frame
  (`graph_frame_allocations`) went from 25 allocations per frame at `Basic` in parallel mode to 9,
  the frame's payloads and one owned string clone that serial frames allocate too, at every level
  up to `Hardware` and in both feature sets. Each row of the budget is 9, with no budget loosened:
  the `Parallel` and `Adaptive` rows at `Basic` are back.
- **Primitive payloads allocate nothing.** `Payload::owned` holds the integers, floats, `bool`,
  `char` and `()` inline instead of in an `Arc` (a `Payload` is 184 bytes, 8 more than before).
  A steady-state tick of the no_std smoke graph (`examples/nostd_smoke`, `i64` in and out) now
  allocates 0 times on one thread and on each of four (it was 2 per tick): the
  `parallel_executors` test asserts it. The detector graph frame (`graph_frame_allocations`) goes
  from 14 to 9 allocations per frame in serial, parallel and adaptive modes (basic metrics: 10 with the
`metrics` feature, whose per-tick metrics map is the extra one). Other types, and `Payload::shared`, keep the
  `Arc` path. For an inline value, `is_storage_unique`, `try_into_owned` and `get_mut` always
  succeed, `shares_storage` holds for equal values of one type (floats compare by bits), and
  `get_arc` hands out a new `Arc` of a copy. Public API and the plugin ABI are unchanged.
- **Per-tick runtime overhead on the serial executor.** Measured on the new `daedalus-frame-bench`
  example `apriltag_mirror`: the tracked AprilTag detector group (19 stages, planner-expanded from
  its embedded graph), the pose and multi-tag pose nodes and the host bridge edges, 21 planned
  nodes and about 12 runs per tick, with stub handlers so that what it times is the runtime. Median
  of 7 runs of 5000 ticks per mode, before (`f6299ad`) and after, on the development host (x86-64);
  the CM5 numbers are not in this entry.
  - metrics Off, no frame-overhead recording: 120,989 -> 109,263 instructions per tick (-9.7%);
    tick p50 14.0 -> 11.6 µs.
  - metrics Off, frame-overhead recording: 152,090 -> 140,300 instructions; overhead
    (`graph_overhead`) p50 13.8 -> 11.6 µs.
  - metrics Timing, no recording: 155,235 -> 123,358 instructions (-20.5%); tick p50 17.2 -> 13.2 µs.
  - metrics Timing, frame-overhead recording: 183,298 -> 151,368 instructions; overhead p50
    16.4 -> 12.4 µs (-24.5%).
  - Runtime allocations per tick at Timing: 43.6 -> 3.0 (the three left are the per-run telemetry
    maps, returned by value); at Off there were none before or after.

  The changes: direct-slot edges replace the held payload in place instead of moving it out and
  back; an edge that adapts nothing skips the adapter call; an empty edge's byte metrics skip the
  redundant atomic writes; each node's group id is read once when the executor is built, and group
  metrics merge without a new key string; custom-metric clear and drain skip an empty map. Telemetry
  contents, public API and plugin ABI are unchanged. The harness enables daedalus's `metrics` and
  `tracing` features, which the live engine runs with; without `metrics` the Timing level compiles
  out.
- The RGA and VGLite fixed-function rows (`crates/kernels/src/registry/fixed.rs`) use
  `lemnos-drivers-rga` and `lemnos-drivers-vglite`, pinned to Lemnos rev `cf3a4ac` in the
  workspace `Cargo.toml` (`lemnos-linux` was `branch = "dev"`). The `librga` variant is now
  `multirga` (`/dev/rga`, Lemnos's `MultiRga`), and `v4l2` is the mainline route. A Linux test
  ties each row to Lemnos's `caps::route_for`. `daedalus.image.line` on VGLite is deferred and
  runs on the CPU: Lemnos does not route lines there until a K230 test passes.

## [3.0.0] - 2026-10-05

### Breaking changes

3.0.0 is a major release; [`docs/migration-3.0.md`](docs/migration-3.0.md) has before/after code
for every item below, for upgrades from 2.0.0 and from pre-release `dev` commits.

- **Features:** `engine` no longer includes the Rayon pool or metrics (use `engine-full`);
  `threads` and `tracing` are default facade features that `default-features = false` builds must
  request; `gpu-wgpu` no longer builds GLES or the `image` bridges (`gpu-gles`, `gpu-image`).
- **Type keys:** every builtin number has its own key (`i32` is no longer `Int`) with lossless
  widening adapters; types from other crates need an owner-declared key, a port `type_key` or
  `foreign_types` (`UnkeyedForeignType` otherwise); key registration is strict.
- **No process-global registries:** the global typing registry, boundary contract registry,
  `type_key_of` and `platform::set_clock` are gone; generic pushes resolve through the registry's
  `TypeIndex`, so `bind_input`, `bind_lane`, `input_typed`/`output_typed` and `NodeIo::push*`
  return `Result`.
- **Node semantics:** `Option<T>` inputs are optional ports carrying `T`'s key, nodes are skipped
  (not failed) until their required inputs have values, and `NodeConfig` requires `Clone`.
- **Host bridge:** event recording is off by default, ports are `PortId`s, and
  `take_inbound` became `take_inbound_into`.
- **Dynamic plugins:** `PLUGIN_ABI_VERSION` 8 (rebuild every plugin), and `export_plugin!` belongs
  in a leaf `cdylib` crate.
- **GPU:** wgpu 30; `ExternalImportSupport::Supported` gained fence fields.
- **FFI SDKs:** width-exact scalars, `WireValue::UInt`, C++ `DAEDALUS_NODE(fn, ...)`, Java records
  for multi-output nodes; the untyped `Outputs` helpers are gone.
- **Removed aliases and dead APIs** (see Removed).

### Added

- **Graph documents.** `GraphDocument` (`format: "daedalus.graph"`, `schema_version`, `requires`,
  `metadata`, `graph`): strict parsing with JSON-path errors, plugin requirement checks
  (`PluginRegistry`, `Engine::{check,prepare,compile}_document*`) and a checked-in JSON Schema
  (`docs/schema/daedalus.graph.v1.schema.json`, `GraphDocument::json_schema()` with the planner's
  `schema` feature, regenerated by the `graph_document_schema` binary; enum lists come from
  `ComputeAffinity::ALL`, `SyncPolicy::ALL` and `BackpressureStrategy::ALL`).
  `daedalus_data::schema::value_json_schema` describes `Value`.
- **Typed host ports and host driving.** `GraphBuilder::{input_as, input_typed, output_as,
  output_typed}` declare host port types (`HostPortTypes`), so one host input feeds ports of
  different types with adapters per edge; nested graphs carry them
  (`NestedGraphHandle::host_types`).
  `RuntimePlan::host_ports*` and `HostGraph::{host_inputs, host_outputs}` describe ports;
  `HostGraph::{inspect_payload, inspect_outputs}` render payloads through value serializers
  (`PayloadSummary` otherwise) and `daedalus_data::json::to_plain_json` renders `Value`s.
  `HostBridgeHandle::inbound_waiter` (blocking and `Future`), `HostGraph::{wait_for_input,
  tick_on_input, drive_blocking, drive}` and `HostGraphStopHandle` drive a graph on input;
  `HostBridgeHandle::{input_port_stats, output_port_stats}` return `HostPortStats`.
- **Native Rust `cdylib` plugins** (`dylib-plugins` facade feature): `export_plugin!` exports a
  `#[repr(C)]` `PluginDescriptor` (`PLUGIN_ABI_VERSION` 8) with the schema, boundary type table,
  foreign interface table and `StableHandlers`; `PluginLibrary::load` reads it from any build,
  `discover_plugin_libraries` scans directories. `install_into` takes the Rust-ABI path
  (`InstallPath::RustAbi`: same Daedalus version, rustc and build fingerprint, identical boundary
  types) or the stable path (`InstallPath::Stable`, `STABLE_ABI_VERSION` 1: schema nodes run
  through a C-ABI `invoke` with `StableValue`s, about 1 µs per call; other rustc versions and
  Daedalus patch releases) and returns which; `install_mode`/`install_into_as` report or force it.
  Typed refusals: `PluginLibraryError::{Incompatible, BoundaryTypeConflict,
  ForeignInterfaceMismatch, MissingDependencies, StableAbiMismatch}`. `export_plugin!(P, deps
  [Dep], boundary_contracts [..])` links dependency plugins into the introspection registry;
  unkeyed foreign port types are listed as `external_types`. `plugin_descriptor!` builds the
  descriptor without exporting symbols. Feature classification lives in
  `[package.metadata.daedalus]` (generated `ENABLED_FEATURES`/`CARGO_MANIFEST`). See
  `docs/dynamic-plugins.md`; examples `stable_abi`, `foreign_consumer`, `dependent`,
  `example_project_dylib`.
- **Type keys for types owned by other crates.** Port `type_key = "..."`,
  `#[plugin(foreign_types(Type = "key"))]` and `PluginRegistry::register_foreign_type::<T>(key)`;
  boundary type records (`register_boundary_type`, `boundary_types`, `RustTypeIdentity`);
  `PluginRegistry::type_index()` (`daedalus_runtime::TypeIndex`, `TypeKeyError`) used by graph
  builders, executors, `NodeIo` and `HostGraph` for generic pushes; fed payloads are checked
  against it (`FeedOutcome::Rejected`). `TransportError::RustTypeMismatch` names both Rust types
  when a key matches but the type does not.
- **Foreign interfaces.** `foreign_interface!` (`#[repr(C)]` accessor vtable, key, version,
  layout hash), `ProvideForeign`, `ForeignHandle`, `ForeignRef<'_, I>`, and the standard
  `daedalus:frame` v1 interface (`FrameView`, `FrameSource`; `docs/foreign-frame-interface.md`).
  `register_foreign_provider{,_as}` / `#[plugin(foreign_providers(Owner => Interface))]` register
  a zero-copy `View` adapter; nodes take `FrameView<'_>`/`ForeignRef<'_, I>` inputs.
- **Optional inputs and joins.** `Option<T>` inputs (`PortDecl::optional`,
  `NODE_REQUIRED_INPUTS_META_KEY`), conditional outputs from `Result<Option<T>, _>`, and fire mode
  `all` (`#[node(fire = "all")]`, `GraphBuilder::{fire_all, fire}`, `NODE_FIRE_META_KEY`): wait,
  popping nothing, until every connected required input holds a value. See "Optional Inputs And
  Readiness" and "Cross-Tick Joins" in `docs/node-authoring.md`.
- **Builtin numbers.** `ValueType::{I8, I16, ISize, U8, U16, U64, USize}` next to `I32`, `U32`,
  `F32`, `Int` (`i64`) and `Float` (`f64`); the `daedalus.builtin.numeric_widening` provider adds
  lossless widening adapters (`daedalus.builtin.widen.<from>_to_<to>`); `ValueType::{rust_name,
  int_range, is_float, is_numeric, check_value}`.
- **Constants.** Node macros register a const coercer for every input and `NodeConfig` field type
  (`daedalus_runtime::const_coerce`, `NodeConfig::register_const_coercers`): unit enums deriving
  `DaedalusTypeExpr` take a variant name or index (`DaedalusTypeExpr::from_value`), other types
  deserialize. `NodeIo::coerce_input`, `daedalus_runtime::const_cache::{ConfigCache,
  DecodedInputs}`, `NodeConfig::port_names`.
- **Macros.** `id` (and `#[type_key]`, `#[adapt(from, to)]`, `foreign_types` keys) accept any
  `&'static str` constant expression; `#[plugin(values(...))]` for `DaedalusTypeExpr + ToValue`
  types; `DaedalusTypeExpr::visit_dependencies` registers nested types first.
- **Presets and features.** Facade `executor-pool`, `metrics`, `engine-full` (`engine` +
  `executor-pool` + `metrics`), `embedded` (`engine` + `plugins` + `threads`), `threads`,
  `tracing`, `gpu-gles`, `gpu-image`, `gpu-dmabuf`; `daedalus-ffi-core`/`-host` `integrity`.
- **Portability** (see "Portability" in `docs/development.md`). `no_std` + `alloc` builds of core,
  transport, data, registry and planner (tier 1) and of the runtime and engine with the serial
  executor (tier 2), including targets without compare-and-swap (`thumbv6m-none-eabi`,
  `riscv32imc`, via `portable-atomic`; `daedalus_core::platform::Arc`). `daedalus_runtime::sync`
  (`parking_lot` with `std`, `spin` without), `daedalus_core::platform::{Clock, Instant,
  OS_CLOCK}`, `EngineConfig::with_clock` and `with_clock` on executors and `StreamGraph`; lineage
  is stamped on the engine clock (`Payload::stamp`, `PayloadLineage::age`,
  `HostBridgeManager::set_clock`). The `embedded` preset runs on `wasm32-unknown-unknown` and
  `wasm32-wasip1`; `examples/wasm_smoke`, `examples/wasm_bindgen_host`, `examples/nostd_smoke`.
- **MCU profile** (`docs/mcu.md`): `daedalus-mcu-build` plans a graph on the host (in `build.rs`)
  and generates a heap-free Rust module (fixed-capacity typed queues per edge, a state slot per
  node, `push_*`/`pop_*` host ports, `tick`) for `daedalus-mcu` (`#![no_std]`, no `alloc`;
  `#[daedalus_mcu::node]`). Three modes: compiled, compiled + tunable (`daedalus.mcu.params`
  constants become a `Tunable` parameter table updated by postcard `ParamUpdate`s) and loaded
  (`loaded::Interpreter` validates and swaps postcard plan blobs for a generated node library at
  a tick boundary). `examples/mcu_blink` (Cortex-M4F/M0+, 3.1 KiB flash and 160 B RAM compiled)
  and the `daedalus-mcu` host tool (`plan`, `param`); `scripts/ci.sh mcu` enforces size budgets.
- **dmabuf import** in `daedalus-gpu` (`gpu-dmabuf`; Vulkan via wgpu-hal):
  `ExternalFrameDescriptor`/`ExternalPlane`, `DrmFourcc`, `GpuBackend::{dmabuf_import_support,
  import_dmabuf}`, `GpuFormat::{Rg8Unorm, Bgra8Unorm, Nv12}`, explicit sync
  (`with_acquire_fence`, `with_acquire_timeout`, `with_implicit_fence`, `export_dmabuf_fence`).
  The fence wait is an `AcquireFenceMode` (`Auto` = `SyncFd` -> `Timeline` -> `Cpu`) set per
  backend (`GpuOptions::acquire_fence_mode`) or per import, resolved against the device's
  `AcquireFenceWaits`; `Timeline` adds a GPU-side timeout (`GpuImageHandle::acquire_status()`).
  Imports do a queue-family-foreign acquire and release; tiled and compressed (DCC) modifiers
  with aux planes import (`MAX_MEMORY_PLANES`); NV12 imports as one texture or per plane.
  `gpu_probe` example; `scripts/ci.sh pi` and `scripts/ci.sh vvl` (validation layers).
- **FFI.** `FfiHost` (`FfiHostBuilder`, `install_package`/`add_package`, `invoke`) runs packages on
  one runner pool with rollback; `WireValue::UInt` (`"uint"`) carries `u64` above `i64::MAX`, and
  every SDK's wire encoder writes `u64` ports as `uint`.
- **Other APIs.** `CapabilityRegistry::remove_plugin`; `StateStore::{take_node_state,
  set_node_state}`; `ExecutionContext::detached`, `RuntimeNode::new`; `NodeInstance::new` and
  `Edge::new` builders; `Value` accessors (`field`, `as_str`, `as_bool`, `as_u64`, `as_list`,
  ...); `daedalus_transport::{PolicyQueue, PushOutcome}`; `daedalus_planner::{edge_explanations,
  host_bridge_metadata}`; `TypeRegistry::{empty, registered_types}`;
  `daedalus_registry::transport_key_typeexpr`; adaptive tuning
  (`with_adaptive_dispatch_overhead`, `NODE_COST_META_KEY`).
- **Docs.** `docs/node-authoring.md`, `docs/dynamic-plugins.md`, `docs/foreign-frame-interface.md`,
  `docs/mcu.md`, `docs/migration-3.0.md`, a minimal CPU-only profile and the
  `external_frame_source` example.

### Changed

- **Features.** The facade `engine` feature no longer enables the Rayon executor pool or metrics
  (`engine-full` does); without `executor-pool`, parallel frames run on persistent threads parked
  between frames. The facade's default features are `threads` and `tracing`; without `threads`
  `Parallel`/`Adaptive` run serially and the worker pool, stream workers and blocking waits do not
  exist. `tracing` is optional in runtime, engine, planner and nodes (spans compile to nothing
  without it). `gpu-wgpu` builds Vulkan (plus Metal/DX12) only: `gpu-gles` adds GLES and
  `gpu-image` (formerly `daedalus-gpu`'s `image`) the `image` crate bridges. `dylib-plugins` uses
  the FFI crates without `integrity`. Workspace dependencies of the `no_std`-capable crates are
  declared without default features.
- **Builtin numbers have distinct keys.** `typeexpr:{"Scalar":"I32"}` names `i32` only; lossless
  conversions are inserted as widening adapters, narrowing fails at plan time
  (`ConverterMissing`), constants are range-checked against the port's exact width, and
  `i128`/`u128` are no longer builtins.
- **Port keys are deterministic.** Macros resolve a type's own key (`#[type_key]`,
  `DaedalusTypeExpr`) at compile time, outputs push under the declared key, `Arc<T>` ports use
  `T`'s key, and `foreign_types` mappings resolve through the installing registry
  (`node_decl_in`, `boundary_contracts_in`, `handler_registry_in`; generic `*_for` functions and
  `NodeConfig::ports` take a `&TypeRegistry`). A foreign type without a key fails install with
  `PluginError::UnkeyedForeignType`.
- **Key registration is strict.** Re-registering the same type under the same key is a no-op;
  another Rust type (`BoundaryTypeConflict`), another key (`TypeKeyedTwice`) or another
  declaration (`TypeDeclarationConflict`) for a key fails. `TypeRegistry::register_type`/
  `register_enum` return `Result<(), TypeConflict>`.
- **Generic pushes are fallible.** `bind_input` and `bind_lane` return `Result<_, EngineError>`,
  `GraphBuilder::input_typed`/`output_typed` return `Result<Self, GraphBuildError>`,
  `NodeIo::push_to`/`push`/`push_default` return `Result<(), NodeError>`, and `run_once` returns
  rejected feeds as errors. `HostGraphRunInput::into_parts` takes the `TypeIndex`.
- **Readiness.** A node runs only when each connected required input has a value and is skipped
  otherwise; optional inputs are `None` without one. Per-edge policies are no longer overwritten
  by `SchedulerConfig::default_policy`.
- **Configs.** `NodeConfig` requires `Clone + Send + Sync + 'static` and `port_names()` (the
  derive generates it); generated handlers decode configs and `&T` constants once per change.
  `NodeIo::take_owned` coerces `Value` constants. `#[node]` rejects the ignored `compute(...)`
  and `bundle` arguments.
- **Host bridge.** Event recording is off by default (`DEFAULT_HOST_BRIDGE_EVENT_RECORDING`);
  ports keep one state per direction with in-place single-slot queues; write paths take
  `impl Into<PortId>`, lookups `impl AsRef<str>`; `HostBridgeManager::take_inbound` became
  `take_inbound_into(alias, &mut Vec)`. Host and edge queues are `PolicyQueue`s, so `Coalesce`
  clears a multi-item FIFO like `LatestOnly`.
- **Ids and payloads.** `TypeKey`, `PortId` and the other text ids wrap `IdStr`; `NodeIo` ports
  are `PortId`s and `NodeConstInputs` is keyed by them. `Payload::owned` always builds typed
  storage; boundary contracts are registry-scoped. `CorrelatedPayload::correlation_id` is the
  lineage id and `enqueued_at` an `Option<Instant>`.
- **Locks.** The runtime uses `parking_lot` (no poisoning; `spin` without `std`), and state,
  context and resource APIs whose only error was poisoning are infallible.
- **Runtime modes.** `RuntimeMode::Adaptive` decides per frame from measured costs (parallel only
  when the predicted gain exceeds 25%, with hysteresis); `ExecutionTelemetry::node_metrics` is a
  `NodeMetricsMap`.
- **GPU.** wgpu 30 (from 29); `get_mapped_range{,_mut}` failures are `GpuError::Internal`.
  `ExternalImportSupport::Supported` reports `acquire_fence`, `acquire_fence_mode` and
  `fence_waits`.
- **FFI.** Installs are atomic and roll back on failure; `RunnerPool::shutdown_all` returns a
  `RunnerShutdownError`. Java and C++ SDKs declare width-exact scalars (Java `@Scalar("u32")` for
  unsigned widths) and the host range-checks worker outputs. C++ `DAEDALUS_NODE(fn, inputs(...),
  outputs(...))` goes after the function and types every port from `decltype(&fn)`
  (`static_assert`s for unmapped types; `std::tuple` for several outputs; `DAEDALUS_TYPE_KEY`).
  Java multi-output nodes return a record whose components name and type the outputs.
- **Misc.** `RuntimeDebugConfig::from_env`/`EngineConfig::from_env` need `std`; the
  `DaedalusTypeExpr`/`DaedalusToValue` derives resolve through the facade; value serializer
  registration no longer requires `T: Clone`; `StrView`, `PluginInfo`, `PluginDescriptor` and
  `PluginLibrary` are `Send + Sync`; `smallvec` always enables `union`.

### Removed

- Process-global state: the typing registry (`daedalus_data::typing::{register_type,
  register_enum, lookup_type, type_expr, snapshot_global_registry, restore_global_registry,
  reset_global_registry, ...}`, `NamedTypeRegistry::global`, `named_types::*` globals), the
  boundary contract registry (`daedalus_transport::{BoundaryContractRegistry,
  register_boundary_contract, boundary_contract_for_type, ...}`),
  `daedalus_core::platform::{set_clock, THREADS}` and
  `daedalus_runtime::config::runtime_debug_config`.
- `daedalus_registry::type_key_of` (and its runtime re-export), `NodeIo::{push_any, push_output,
  push_output_default, take_outputs_small}`.
- Aliases and dead code: `HostBridgeHandle::{push_payload, push_any, feed_payload_ref,
  next_correlation_id}`, `TypeKey::opaque`, `value_serializer_map`,
  `HostGraph::set_value_serializers`, `InboundWait::is_ready`, `RuntimePlan::host_bridge_aliases`,
  `StrSink::discard`, `daedalus_planner::helpers`, the no-op `Outputs` derive, the
  `register_<type>_type` functions generated by `#[type_key]`, `PayloadStorage::{into_any,
  into_any_arc}`, `StateError::LockPoisoned`, `RunnerPoolError::LockPoisoned`,
  `StateStore::get_result`, `EngineError::Io`, `GPU_FEATURE_ENABLED` (use `ENABLED_FEATURES`).
- FFI SDK helpers: Java `Outputs`, C++ `daedalus::Outputs`/`daedalus::outputs` and the
  `daedalus::signature<F>()` registration option.

### Fixed

- **Parallel runs count each group once.** A parallel run added each segment's node metrics to
  their groups, then added them again over the merged run, so a group's `calls` and
  `total_duration` came out twice the serial run's (4 calls against 2 for a two-node group). A
  parallel run now aggregates its groups once, as a serial run does. Test:
  `crates/runtime/tests/it/parallel_telemetry.rs`, which runs grouped ticks in parallel and
  serially and compares them tick by tick.

- **`Payload::to_foreign_handle` no longer has undefined behaviour for a shared owner value.** A
  `provide_foreign` payload over an `Arc<T>` built the handle's `Arc` from `&self`, whose provenance
  covers the value but not the reference counts beside it (Miri reported it under Stacked Borrows).
  The handle now comes from the `Arc` holding the storage, passed through a hidden provided method,
  `PayloadStorage::to_foreign_handle_in`. It costs the same reference count and no allocation.
- Plugins with `i64` and `i32` ports no longer conflict, and an `i64` host input fanned out to two
  `i64` inputs no longer fails with `payload type mismatch`.
- Enum `NodeConfig` fields and enum handler inputs no longer fail with `missing <port>` (the
  engine now passes the registry's const coercers to its executors).
- Single-node direct host routes (`run_direct_once`, lanes, `tick_direct_*`) deliver const
  inputs.
- `fn(&A, &B, &mut State)` nodes compile (the low-level form is recognized by parameter types).
- Declared host port types survive embedding and `nest`.
- Per-edge pressure and freshness policies are no longer replaced by the scheduler default.
- Dynamic plugin nodes no longer crash in hosts built with other dependencies (`smallvec`
  `union`, `NodeIo` in the fingerprint, `Any` fallback for payloads from another
  `daedalus-transport` copy).
- `daedalus-gpu` no longer segfaults in the Vulkan loader when threads create backends
  concurrently (driver setup is serialized; wgpu's `DEBUG` flag only with `WGPU_DEBUG=1`).
- Schema export/import encoding round trip, unknown wgpu formats silently treated as RGBA8,
  `Coalesce` never shrinking host FIFOs, shared input/output freshness watermarks, and the
  `engine,plugins,gpu-mock` build.

### Performance

- Host graph push/tick/take with metrics off: 31 heap allocations per round trip down to 4 (the
  two payloads), pinned by `crates/engine/tests/hot_path_allocations.rs`.
- A 16-node detector-like frame: 290 allocations down to its 31 payloads (generated handlers keep
  state in `StateStore` slots, decode configs and constants once, resolve output keys once per
  registry); parallel frames allocate nothing beyond serial and went from 157 to 74 µs (659 to
  69 µs without the pool). Numbers in `docs/development.md`.
- Owned non-builtin constants decode once per change and are cloned per call.
- Benchmarks: `host_graph_drive`, `graph_frame` and the `bench` workflow flagging >15% median
  regressions.

### Maintenance

- **Removed the Docker example suite** (`ci-docker` workflow, `testing/docker`, the
  `docker_examples` test): it ran facade examples that no longer exist, and the
  `daedalus-examples` binaries already run natively in `scripts/ci.sh smoke` (CI workspace job)
  on the pinned toolchain.
- `scripts/ci.sh` subcommands (`lints`, `test`, `macro-ui`, `aarch64`, `lean`, `features`,
  `nostd`, `wasm`, `mcu`, `smoke`, `bench`, `pi`, `vvl`) and matching CI jobs; CI builds the
  workspace with `--all-features` (the `example_project` export moved to the leaf
  `example_project_dylib` crate).
- `scripts/check-file-sizes.sh` fails on any oversized file (empty baseline); large files split.
- Duplicate code consolidated across macros, FFI language crates and builtins;
  `docs/host-bridge-lock-granularity.md` documents the lock order.
- The optional Styx examples keyed `FrameLease` as `styx:framelease` (those examples are removed
  under Unreleased).

## [2.0.0] - 2026-04-30

### Added

- Added the `daedalus-transport` crate with stable transport identities, payload storage,
  adapter declarations, boundary contracts, device transfer metadata, stream policies,
  residency/layout tracking, and payload lifecycle records.
- Added typed transport identifiers and request metadata around `TypeKey`, `AdapterId`,
  `SourceId`, layouts, residency, access mode, adapter kind, and payload release state so
  runtime and FFI boundaries no longer depend on ad hoc string matching for core transport
  behavior.
- Added capability-oriented registry support for plugin manifests, type/node/adapter/device
  declarations, serializer declarations, capability snapshots, and deterministic capability
  resolution.
- Added capability source tracking, freeze-time dependency validation, filtered snapshots,
  typed duplicate diagnostics, and built-in provider metadata for release-facing plugin
  registries.
- Added a modular planner pass pipeline covering setup, hydration, embedded graph lowering,
  type validation, overload handling, adapter insertion, schedule generation, linting,
  explanations, suggestions, and plan metadata.
- Added planner lowering registries, typed planner diagnostics, graph patch reports, embedded
  graph host-port mapping, node execution-kind metadata, overload resolution metadata, and
  structured schedule/GPU segment metadata.
- Added runtime-owned executor infrastructure with compiled schedules, direct host routes,
  queue accounting, patching, reusable owned executors, stream graph support, and expanded
  serial/parallel execution tests.
- Added adaptive runtime execution mode that keeps linear plans on the serial path and selects
  parallel execution when the compiled segment graph has useful fan-out or multiple ready
  segments.
- Added detailed runtime telemetry modules for node timing, transport/adapters, queue pressure,
  payload lifecycle, ownership/resource reporting, and compact telemetry summaries.
- Added FFI telemetry sections for packages, backends, workers, payload handles, adapters,
  byte counts, worker stderr, malformed responses, typed errors, and payload ownership mode
  counters.
- Added host bridge manager, event, policy, serializer, and type modules for structured
  direct host I/O and streaming workflows.
- Added runtime-configurable host bridge queue bounds, event retention, event recording,
  stream idle sleep, worker diagnostics, stop timeouts, shutdown-pending state, host stats,
  and retained host events.
- Added graph builder modules for scoped graph construction, typed handles/ports, edge policy
  metadata, nested graphs, and stricter validation.
- Added compact graph-builder helpers for common `host input -> node -> host output` graphs,
  including typed node-port wiring helpers used by the quickstart examples.
- Added engine execution layers for prepared plans, host graph support, compiled runs, transport
  execution tests, and richer engine configuration validation.
- Added engine cache metrics, cache clearing, runtime pool-size environment parsing, demand
  sinks, runtime debug configuration, metrics levels, stream host configuration, and host graph
  direct-lane bindings.
- Added macro support for `#[adapt]`, device declarations, plugin declarations, branch payloads,
  type keys, richer node metadata, generic registration, config-backed ports, and UI compile
  tests for invalid macro use.
- Added public facade exports and a `daedalus::prelude` for common application code, covering
  engine, runtime, transport, registry, macros, plugins, host bridge helpers, and optional GPU
  types behind their existing features.
- Added GPU adapter selection, device capability reporting, staging/copy limiters, device caches,
  async polling helpers, WGPU backend resource modules, and dispatch/readback benchmarks.
- Added configurable GPU async readback timeout and poll interval APIs, named async poll worker
  limits, panic-safe GPU poll workers, bounded overflow poll slots, and regression tests for
  poll worker recovery.
- Added Rust dynamic plugin boundary coverage and shared FFI contract models.
- Added FFI schema package modules, wire protocol modules, conformance fixtures, generated
  descriptor snapshots, package integrity stamping, artifact hashing, lockfile generation,
  worker protocol negotiation, payload handle validation, and shared package validation across
  Rust, C/C++, Java, Node, and Python.
- Added language SDK surfaces for C/C++, Java, Node, and Python FFI integrations, including
  transport options for pointer/length views, direct byte buffers, memoryviews, mmap-backed
  payload handles, and shared-memory buffer access.
- Added persistent worker lifecycle handling for startup, request timeouts, stderr drainage,
  malformed responses, worker restarts, repeated invocation, state import/export, and payload
  ownership modes.
- Added standalone example plugin crates under `examples/plugins` for a copyable Rust plugin
  project, math capabilities, and optional Styx `FrameLease` plugins.
- Added top-level runnable examples for quickstarts, typed ports, runtime configuration,
  transport behavior, async graphs, metrics/debugging, GPU fallback, and mixed CPU/GPU flows.
- Added FFI showcase examples and smoke coverage for multi-language package loading, transcript
  nodes, payload/GPU feature coverage, and all-plugin graph validation.
- Added CI/release helper scripts for workspace dependency checks and GPU async blocking audits.
- Added runtime diagnostics documentation with recommended `RUST_LOG` targets for executor,
  queue pressure, stream/host bridge behavior, engine cache behavior, GPU backend/dispatch/
  poll/readback paths, planner passes, and demo nodes.

### Changed

- Reworked the workspace around separated core, data, transport, registry, planner, runtime,
  engine, GPU, FFI, macro, daemon, and facade responsibilities.
- Bumped all workspace crates and standalone example plugin crates to `2.0.0`.
- Updated the facade crate exports for the new plugin, transport, host bridge, graph builder,
  macro, engine, and GPU surfaces.
- Updated quickstart examples to use the facade prelude and compact single-node roundtrip
  graph builder helper instead of manual host bridge handle wiring.
- Reworked data conversion and typing, including named type registration, const coercion,
  value serialization, descriptor handling, JSON conversion, and conversion test coverage.
- Added full process-global type registry snapshot, restore, and reset helpers for test
  isolation and embedders that temporarily rely on global type convenience APIs.
- Refactored planner internals from a monolithic pass module into focused pass modules with
  refreshed golden outputs.
- Reworked runtime plugin installation around plugin manifests, built-in capability providers,
  registry freezing, transport adapter registration, boundary contracts, and capability
  source tracking.
- Reworked runtime state management into context/resource modules with stronger lifecycle and
  type mismatch errors.
- Reworked executor queues, direct-slot handling, backpressure behavior, edge policies,
  runtime plan snapshots, and scheduling paths.
- Reworked borrowed and owned executor configuration through shared configuration-target
  helpers so pool size, fail-fast mode, metrics level, debug configuration, host bridges,
  state, GPU handles, selected output ports, runtime transport, and mask validation stay
  consistent.
- Reworked selected-host-output and demand-sink execution to use fallible mask setters and
  typed demand errors instead of panicking on user-provided mask mismatches.
- Reworked engine cache/config/diagnostics/error handling and moved execution behavior into
  dedicated execution modules.
- Reworked engine runtime dispatch so `Serial`, `Parallel`, and `Adaptive` modes are distinct
  in direct execution and compiled runs.
- Reworked daemon startup/service integration for the new engine/runtime APIs.
- Refreshed Rust, C/C++, Java, Node, and Python FFI packaging APIs, generated manifest builders,
  subprocess pack/bundle tests, SDK type surfaces, and plugin library loading.
- Refactored Java FFI bridge code into a checked-in bridge source file.
- Updated node bundles to install through the new plugin/capability registry instead of the
  removed planner/registry adapter shims.
- Reorganized README guidance and dependency snippets for `2.0.0`.
- Reworked runtime diagnostics, development, testing, and crate README documentation around
  the new layered architecture, release validation flow, feature gates, dependency policy,
  and operational debugging path.
- Reworked runtime benchmark coverage for executor snapshots, direct host routes, stream
  round trips, worker idle behavior, backpressure, state resources, and telemetry clone/report
  costs.
- Reworked GPU README and benchmark guidance around async WGPU dispatch/readback behavior,
  fallback paths, and blocking compatibility APIs.

### Fixed

- Fixed the release clippy blocker in `crates/ffi/host/benches/ffi_overhead.rs`.
- Fixed GPU async poll workers so a panicking poll job no longer permanently kills a shared
  worker thread.
- Fixed GPU overflow poll slot accounting so slots are released by a drop guard even if the
  poll job panics.
- Fixed async readback configuration so zero-duration knobs normalize to a safe minimum rather
  than creating a tight polling loop.
- Fixed stream worker benchmark flakiness by waiting boundedly for background-worker output
  instead of assuming one short receive timeout always catches a scheduled tick.
- Fixed direct and compiled engine execution so adaptive mode no longer aliases unconditional
  parallel execution.
- Fixed host graph selected execution to prefer fallible executor mask APIs.
- Fixed global type registry isolation gaps by adding snapshot/restore/reset APIs and coverage.
- Fixed FFI persistent-worker stderr handling so retained stderr is drained continuously and
  included in startup/message errors without blocking stdout progress.
- Fixed FFI runner limit handling by rejecting unsupported persistent-worker queue depth,
  request timeout, and restart policy combinations until cancellable worker I/O and automatic
  restart semantics are implemented.

### Performance

- Added and smoke-ran runtime benchmarks covering retained serial ticks, scoped parallel
  ticks, worker-pool parallel ticks, direct host route cache hits, state-resource access,
  edge pressure policies, stream round trips, worker backpressure, and telemetry clone/report
  overhead.
- Reduced common quickstart graph construction boilerplate by routing single-node host
  roundtrips through typed graph-builder helpers.
- Kept async GPU readback polling on a bounded worker pool so executor threads do not park on
  WGPU map completion.
- Kept stream and host bridge queues bounded by default and documented runtime knobs for
  pressure/freshness policies.

### Removed

- Removed the old root `plugins/` example folder; standalone plugin examples now live under
  `examples/plugins`.
- Removed crate-local runnable examples from `crates/daedalus/examples` and
  `crates/ffi/examples` in favor of the top-level `examples` crate and language-specific FFI
  example projects.
- Removed the old registry bundle/store/convert modules in favor of capability registries and
  transport identities.
- Removed the old runtime conversion module and crash diagnostics path after moving conversion
  and execution concerns into transport, data, executor, and telemetry modules.
- Removed legacy planner/runtime/engine tests and benchmarks that covered APIs replaced by the
  new transport, planner, executor, and engine flows.

## [1.0.0] - 2026-04-19

- Standardized the workspace layout, docs, CI, linting, and helper scripts.
- Removed the `extensions/` tree and aligned the repo around crates, plugins, docs, testing, and Docker-backed facade validation.
- Centralized more workspace dependencies and documented the intentional `default-features = false` manifest exception in `crates/engine`.
- Added `scripts/check-file-sizes.sh`, `scripts/ci.sh`, and `scripts/repo-clean.sh`.
