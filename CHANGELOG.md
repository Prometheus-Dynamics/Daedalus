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

### Removed

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

### Changed

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
