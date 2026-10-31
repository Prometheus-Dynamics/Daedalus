# Migrating To Daedalus 4.0

This guide covers upgrading from **3.0.0** (tag `v3.0.0`) to 4.0.0, which `dev` carries as
`3.0.1-dev` until the coordinated release. The full list of changes is in
[`CHANGELOG.md`](../CHANGELOG.md) (`[Unreleased]` until the release); every breaking item that
`./scripts/ci.sh semver` reports against `v3.0.0` is named there or in
[`api-review.md`](api-review.md).

```toml
# from git until 4.0.0 is tagged: pin the commit, without a `version` requirement
daedalus = { package = "daedalus-rs", git = "https://github.com/Prometheus-Dynamics/Daedalus.git", rev = "<commit>" }
```

Everything that links Daedalus (the host, its dynamic plugins, libraries with a `daedalus`
feature) must resolve to the same Daedalus. A git dependency with `version = "3.0.0"` no longer
resolves against `3.0.1-dev`, so drop the requirement.

## Checklist

1. Rebuild every Rust-ABI dynamic plugin with the host. `PLUGIN_ABI_VERSION` is 11 and
   `PluginInfo::daedalus_version` is compared exactly. Stable-ABI plugins built against
   `STABLE_ABI_VERSION` 2 keep loading. See [Dynamic plugins](#dynamic-plugins).
2. Remove the `executor-pool` and `rayon` features, and `styx-camera-example` /
   `gpu-styx`, from your dependency lines. See [Features](#features).
3. Struct literals of the types under [Structs that gained fields](#structs-that-gained-fields)
   need the new fields; prefer `Default` plus setters.
4. Exhaustive `match`es on `GpuError` and `EdgePressureReason` need the new variants, and
   code relying on the numeric discriminants of `PluginLibraryError` or `daedalus-mcu`'s
   `LoadError` must stop. See [Enums](#enums).
5. aarch64 Linux hosts that relied on the Vulkan-only default must select Vulkan. See [GPU](#gpu).
6. Calls to removed functions: see [Removed APIs](#removed-apis).
7. Read [Behavior changes](#behavior-changes) for what changed meaning without changing a type.

## Dynamic plugins

- `PLUGIN_ABI_VERSION` went 9 → 10 → 11 over this release. The descriptor gained
  `PluginDescriptor::crate_builds` and its boundary contracts. Layout hashes now come from
  `TypeExpr::canonical_label()`, a pinned rendering, instead of `Debug`, so every boundary layout
  hash changed once. Rebuild each Rust-ABI plugin with the host's exact Daedalus version.
- `STABLE_ABI_VERSION` is 2. `StableHandlers` gained `create` and `unload`, and `StableInput`
  gained `port` and `fanin`. The stable SDK fills these in; only hand-written descriptors need
  them.
- `daedalus:frame` is interface v3, and `FrameVTable` gained `colorimetry`, `format_kind`,
  `metadata_bytes`, `metadata_u64`, `plane_end_cpu_access` and `plane_mapping`.
  `FramePlane` gained `mapping`. Plugins built against frame v2 keep working through the stable
  path (`frame_handle_to_v2` / `frame_handle_from_v2`). Code comparing `FrameInterface::VERSION`
  against 2, or building a `FrameVTable` by hand, must update; `FRAME_INTERFACE_V2` names the
  old version.
- `ForeignView::from_handle` is now `from_payload`, and `PayloadStorage` no longer has
  `type_key`. Only custom view and storage types implement these traits.
- Plugin libraries close when unused; `PluginLibrary` holds its library by reference count.

## Features

- `executor-pool` (facade, engine, runtime, daemon) and the runtime's `rayon` feature are gone.
  `RuntimeMode::Parallel` / `Adaptive` use the parked worker threads that `threads` enables.
  `engine-full` is `engine` + `metrics`.
- `styx-camera-example` and `gpu-styx` are gone along with the Styx examples. Styx owns its
  Daedalus integration (`styx_core::daedalus`, Styx's `daedalus` feature).

## GPU

- Backend selection no longer defaults to Vulkan-only on aarch64 Linux. Set
  `WGPU_BACKEND=vulkan`, or `GpuOptions::backends = Some(GpuBackends::VULKAN)`.
- `GpuCapabilities` gained `compute` (`GpuComputeCaps`: limits and enabled shader features), and
  the wgpu backend now requests optional features (f16, int64, subgroups, timestamps, pipeline
  cache) when the adapter has them.
- `GpuOptions` gained `backends`, and `ExternalFrameDescriptor` gained `colorimetry`.
- `GpuError::InvalidInput(String)` is new, for caller sizes and ranges.
- Tiled dmabuf imports take 1 to 4 memory planes, whatever the format's plane count (v3dv's
  UIF NV12 is one plane). `LINEAR` still takes one plane per format plane.
- The dmabuf acquire path no longer hands a fence that is not a `sync_file` to the GPU; such
  fences take the documented fallback. This fixes an NVIDIA hang, and only fd types that were
  never valid behave differently.
- New, all additive: `GpuContextHandle::wgpu()`, `GpuStorage` (including `import_dmabuf`),
  `GpuStoragePool`, `GpuTexture`, `GpuTicket` / `GpuInFlight`, `GpuReadback`, `FragmentPass`
  (with `FragmentBinding`, `FragmentRects`), `GpuTimer`, `GpuPipelineCache` (and the engine-owned cache,
  `EngineConfig::with_gpu_pipeline_cache`), and `#[device]`
  transfers taking `&GpuContextHandle`. See [`gpu-nodes.md`](gpu-nodes.md).

## Structs that gained fields

A struct literal of one of these must add the field (or use `..Default::default()` where the type
implements `Default`):

| Crate | Struct | New fields |
|---|---|---|
| engine | `CacheSection` | `gpu_pipeline_dir` |
| engine | `RuntimeSection` | `blocking_backpressure`, `frame_overhead_window`, `node_fusion`, `recovery`, `stream_error_policy` |
| gpu | `GpuCapabilities` | `compute` |
| gpu | `GpuOptions` | `backends` |
| gpu | `ExternalFrameDescriptor` | `colorimetry` (build it with the constructors) |
| runtime | `StreamWorkerConfig` | `on_error` |
| runtime | `TelemetryReport` | `backpressure_deferrals`, `recovery` |
| runtime | `EdgeMetrics` / `EdgePressureMetrics` | `fused_handoffs` / `coalesce_reject` |
| runtime | `StreamWorkerDiagnostics` | `tick_errors` |
| runtime | `RuntimePlanExplanation` | `copying_edges`, `crossing_edges`, `fused_units` |
| runtime | `RuntimeEdgeExplanation` | `copies_frame`, `crosses_residency`, `fused`, `fusion_block` |
| runtime | `RuntimeNodeExplanation` | `device`, `device_reason` |
| runtime | `ApplyPolicyOwnedArgs` | `stamp_enqueue` |
| transport | `FrameVTable`, `FramePlane` | see [Dynamic plugins](#dynamic-plugins) |
| facade | `PluginDescriptor`, `StableHandlers`, `StableInput` | see [Dynamic plugins](#dynamic-plugins) |

`RuntimeNode::metadata` is an `Arc<BTreeMap<..>>`; code building a `RuntimeNode` wraps its map in
`Arc::new`. `NodePortRefresh` (new in this release) has `dropped_consts`.

## Enums

- New variants: `GpuError::InvalidInput`, `EdgePressureReason::CoalesceReject`.
- Discriminants moved because variants were inserted: `PluginLibraryError` (facade) and
  `daedalus-mcu`'s `LoadError`. Match on the variants, not on numbers.
- `daedalus-daemon`'s `ServiceResponse::Pong` is no longer a unit variant: it is
  `Pong { protocol_version }` (absent in an old daemon's answer, which reads as 0).
- `daedalus-core`'s `channels::policy` module, with its `EdgePolicy` and `ChannelAsync`, is gone.
  Use `bounded_with_behavior`, `unbounded_with_behavior` and `broadcast_with_behavior`.
- `EngineError` is no longer `UnwindSafe` / `RefUnwindSafe`: it gained `Io(std::io::Error)`
  (whose custom errors are trait objects) and `HostBatch`. Wrap it with `AssertUnwindSafe` if
  you pass it across `catch_unwind`.

## Removed APIs

The public API review removed 55 functions that had no caller in the workspace, HeliOS, Styx,
Eidos or the docs; others are now `#[doc(hidden)]`. The full list, with replacements, is under
"Removed" in the CHANGELOG and in [`api-review.md`](api-review.md). The ones with a
replacement:

| Removed | Use |
|---|---|
| `channels::policy::{build_bounded, build_unbounded, build_broadcast}` | `bounded_with_behavior`, `unbounded_with_behavior`, `broadcast_with_behavior` |
| `Engine::build_runtime_plan` | `daedalus_runtime::build_runtime` |
| `RunnerPool::{ffi_telemetry, set_ffi_telemetry}` | `RunnerPool::with_ffi_telemetry` |
| `NodeError::retryable`, `ExecuteError::retryable` | `NodeRecovery` policies in node metadata |
| `FfiBackendTelemetry::runner_restarts` | `ExecutionTelemetry::recovery().restarts` |

## Behavior changes

- **CPU fallback.** Without a GPU backend, a `#[node(fallback = "...")]` node runs its CPU
  node in its place; before, the GPU handler ran on CPU data. `explain_plan()` reports each
  node's resolved `device` and `device_reason`, and its `Display` node line reads
  `(<compute>, on <device>)`.
- **Adapter search.** Equal-cost paths now tie-break deterministically: fewer steps first, then
  ids in order. Only the choice between equal-cost routes changes.
- **Counting drops.** A push that evicts a backlog counts every evicted item, in edge drops,
  pressure, host-bridge stats and `FeedOutcome::Replaced { dropped }`. A `KeepOldest` coalesce
  refusal is `CoalesceReject`.
- **FFI.** Bytes travel as base64 (`{"data": "...", "encoding": "base64"}`), and every decoder
  still reads the old number arrays. C++ is unchanged.
- **Stored graphs.** A node whose plugin gained ports no longer fails to plan after
  `refresh_graph_ports`, and with `RefreshPolicy { drop_stale_consts: true }` a const on a
  dropped port is dropped and reported.
