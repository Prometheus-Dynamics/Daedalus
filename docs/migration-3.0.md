# Migrating To Daedalus 3.0

This guide covers upgrading to 3.0.0 from:

- **2.0.0**, the April release (tag `v2.0.0`): read every section.
- **`dev` at `75dc4c1`**, the pre-release commit Styx `dev` and Eidos `main` pinned: start with
  the [checklist](#quick-checklist-from-75dc4c1) and read the items marked **since 75dc4c1**.
  Items marked "since 2.0.0" changed after 2.0.0 but were already in that commit.

The full list of changes is in [`CHANGELOG.md`](../CHANGELOG.md#300---2026-10-05).

```toml
daedalus = { package = "daedalus-rs", version = "3.0.0", features = ["engine-full", "plugins"] }
# or, from git until 3.0.0 is on crates.io:
daedalus = { package = "daedalus-rs", git = "https://github.com/Prometheus-Dynamics/Daedalus.git", tag = "v3.0.0" }
```

Everything that links Daedalus (the host, its dynamic plugins, libraries with a `daedalus`
integration feature such as Styx) must resolve to the same 3.0.0 `daedalus-rs`; a crate still
asking for `version = "2"` pulls in a second, incompatible copy.

## Quick Checklist From 75dc4c1

1. Default features now include `threads` and `tracing`; `default-features = false` builds
   (`embedded`, wasm) add `tracing` if they want spans. See [Features](#features).
2. `gpu-wgpu` no longer brings GLES or the `image` bridges: add `gpu-gles` / `gpu-image`.
3. Rebuild every dynamic plugin: `PLUGIN_ABI_VERSION` is 8. `install_into` returns the
   `InstallPath` and installs plugins from other toolchains through the stable path instead of
   failing with `Incompatible`. See [Dynamic Plugins](#dynamic-plugins).
4. `daedalus_core::platform::{set_clock, THREADS}` are gone: use `EngineConfig::with_clock` and
   the `threads` feature. See [Removed APIs](#removed-apis).
5. wgpu 30, and `ExternalImportSupport::Supported` gained fields. See [GPU](#gpu).
6. FFI SDKs: C++ `DAEDALUS_NODE(fn, ...)` after the function, Java records for multi-output
   nodes, `WireValue::UInt`. See [FFI SDKs](#ffi-sdks).
7. Behavior: per-edge policies now win over `SchedulerConfig::default_policy`, and `fire = "all"`
   joins are available. See [Scheduling](#scheduling-and-node-semantics).

## Features

| Before | 3.0.0 | Since |
| --- | --- | --- |
| `engine` (engine + Rayon pool + plugins) | `engine-full` = `engine` + `executor-pool` + `metrics`; `engine` alone has no pool and no metrics | 2.0.0 |
| no preset for small hosts | `embedded` = `engine` + `plugins` + `threads`, no pool, metrics or `tracing` | 2.0.0 |
| `ffi` (native plugins) | `dylib-plugins`, in both host and plugin | 2.0.0 |
| `default = []` | `default = ["threads", "tracing"]` | **75dc4c1** |
| `embedded` ran on wasm | wasm and other targets without threads: `default-features = false, features = ["engine", "plugins"]` | **75dc4c1** |
| spans always compiled | `tracing` feature (default); `default-features = false` builds add it explicitly | **75dc4c1** |
| `gpu-wgpu` built Vulkan + GLES + `image` | `gpu-wgpu` = Vulkan (Metal/DX12 on their platforms); add `gpu-gles` for OpenGL/GLES devices and `gpu-image` for `Compute<DynamicImage>`, image `DeviceBridge`s and `ShaderRunOutput` image readbacks | **75dc4c1** |
| `daedalus-gpu` feature `image` | `gpu-image` | **75dc4c1** |
| `daedalus-ffi-core`/`-host` always hashed packages | `integrity` feature (default on those crates) | **75dc4c1** |

```toml
# Before (2.0.0)
daedalus = { package = "daedalus-rs", version = "2.0.0", features = ["engine", "plugins", "gpu-wgpu"] }
# After: same behavior (pool, metrics, GLES, image bridges)
daedalus = { package = "daedalus-rs", version = "3.0.0", features = ["engine-full", "plugins", "gpu-wgpu", "gpu-gles", "gpu-image"] }

# Before (75dc4c1): lean host
daedalus = { package = "daedalus-rs", git = "...", rev = "75dc4c1", default-features = false, features = ["embedded"] }
# After: keep spans if you relied on them
daedalus = { package = "daedalus-rs", version = "3.0.0", default-features = false, features = ["embedded", "tracing"] }
```

Without `threads`, `RuntimeMode::Parallel`/`Adaptive` run serially and the worker pool,
`StreamGraph::spawn_continuous*`, `InboundWaiter::wait`, `HostBridgeHandle::{wait_inbound,
recv_payload_timeout}`, `prewarm_worker_pool` and `HostGraph::{wait_for_input, tick_on_input,
drive_blocking}` do not exist. Poll (`try_pop*`, `drain*`) or await `InboundWaiter` instead.
`RuntimeDebugConfig::from_env` and `EngineConfig::from_env` need `std`.

## Type Keys

### Builtin numbers have their own keys (since 2.0.0)

`i8`..`u64`, `isize`/`usize` and `f32` no longer share `Int`/`Float`; `ValueType` gained `I8`,
`I16`, `ISize`, `U8`, `U16`, `U64`, `USize` (`Int` is `i64`, `Float` is `f64`). The planner
inserts lossless widening adapters (`i32 -> i64`, `f32 -> f64`, ...) and rejects narrowing at
plan time with `ConverterMissing`. Graph constants are range-checked against the exact width.

```rust
// Before: an i32 port accepted Int-typed host inputs and i64 producers.
graph.input_as("gain", TypeExpr::Scalar(ValueType::Int));          // -> fn scale(gain: i32)
// After: declare the width the port has (or feed i64 into an i64 port).
graph.input_as("gain", TypeExpr::Scalar(ValueType::I32));
// or let Rust pick it:
let graph = registry.graph_builder()?.input_typed::<i32>("gain")?;
```

`i128`/`u128` are no longer builtins (they get `rust:` keys and convert through serde). Wire
types in FFI descriptors and stored graphs that relied on `Int` for narrower widths need the
exact width. See "Builtin Numbers" in [`node-authoring.md`](node-authoring.md#builtin-numbers).

### Types owned by other crates need a key (since 2.0.0)

A port type from another crate without a `#[type_key]`/`DaedalusTypeExpr` key used to get an
order-dependent `rust:` key; now install fails with `PluginError::UnkeyedForeignType`. In order
of preference:

```rust
// 1. The owner's integration feature declares the key (e.g. Styx's `daedalus` feature):
//    styx = { ..., features = ["daedalus"] }, then install `StyxFramesPlugin` in the host.
// 2. Map it in your plugin, using the key the owner would use:
#[plugin(id = "app", nodes(mark), foreign_types(FrameLease = "styx:framelease"))]
struct AppPlugin;
// 3. Per port:
#[node(id = "mark", inputs(port(name = "frame", type_key = "styx:framelease")), outputs("frame"))]
// 4. In code: registry.register_foreign_type::<FrameLease>("styx:framelease")?;
```

### Strict key registration (since 2.0.0)

Registering the same Rust type under the same key again is a no-op; another Rust type for a key
(`BoundaryTypeConflict`), another key for a type (`TypeKeyedTwice`) or a different declaration
(`TypeDeclarationConflict`) fails instead of silently replacing the earlier one.
`TypeRegistry::register_type`/`register_enum` return `Result<(), TypeConflict>`.

### Registry type index; fallible generic pushes (since 2.0.0)

Generic `T -> TypeKey` lookups go through the registry the graph was built from
(`PluginRegistry::type_index()`), never through process globals. Build graphs with
`registry.graph_builder()?` so they carry the index. These now return `Result`:

```rust
// Before
let input = host.bind_input::<Frame>("frame");
let lane = host.bind_lane::<Frame>("frame", "out").expect("direct route");
let graph = GraphBuilder::new(caps).input_typed::<Frame>("frame").build();
io.push_to("out", value);
// After
let input = host.bind_input::<Frame>("frame")?;            // Result<HostGraphInput<_>, EngineError>
let lane = host.bind_lane::<Frame>("frame", "out")?;       // Err also when there is no direct route
let graph = registry.graph_builder()?.input_typed::<Frame>("frame")?.build();
io.push_to("out", value)?;                                 // Result<(), NodeError>
```

`HostGraph::push`/`HostBridgeHandle::push` still return `FeedOutcome`; a type the index cannot
key, or a payload whose key the registry records for another Rust type, is
`FeedOutcome::Rejected` with a `TypeKeyError`. `run_once` returns rejected feeds as errors.
`HostGraphRunInput::into_parts` takes the `TypeIndex`.

## Scheduling And Node Semantics

### Optional inputs and readiness (since 2.0.0)

`Option<T>` (also `Option<&T>`, `Option<Arc<T>>`) parameters are ports with `T`'s key marked
optional, so `T` producers connect directly (before, `typeexpr:Optional(..)` needed a converter
and the edge failed to plan). A node runs when every *connected required* input has a value and
is **skipped** otherwise; before, it failed with `missing <port>`. Code that relied on that error
to detect missing data should make the input optional and check for `None`.
`Result<Option<T>, _>` returns are conditional outputs.

### Configs and constants (since 2.0.0)

`NodeConfig` requires `Clone + Send + Sync + 'static` and `port_names()` (the derive generates
it): add `#[derive(Clone)]`. Configs and `&T` constants are decoded once per change, so
`sanitize` warnings log once per change. `#[node]` rejects the ignored `compute(...)` and
`bundle = "..."` arguments; set compute affinity per graph node. Enum config fields and enum
inputs work without `register_enum` (unit enums deriving `DaedalusTypeExpr` take a variant name
or index).

### Per-edge policies and joins (since 75dc4c1)

- `edge_latest_only`, `edge_bounded` and edge policy metadata in graph documents were overwritten
  by `SchedulerConfig::default_policy`; they now win. Graphs that set both get the per-edge one.
- New: `fire = "all"` (`#[node(fire = "all")]`, `GraphBuilder::fire_all`, node metadata
  `daedalus.node.fire`) waits until each connected required input holds a value. Hand-written
  "latch" nodes that kept the last value of each input in state can switch to it. See
  "Cross-Tick Joins" in [`node-authoring.md`](node-authoring.md#cross-tick-joins-fire--all).
- Typed host ports inside embedded graphs and `nest` now keep their declared type.

### Graph documents (since 2.0.0)

Persist graphs as `GraphDocument` (`format: "daedalus.graph"`, `schema_version`, `requires`,
`metadata`, `graph`) and load them with `GraphDocument::from_json`. Unknown fields are rejected
with their JSON path, including inside `graph` (`PortRef`, `Edge`, `NodeInstance` deny unknown
fields), so hand-edited JSON with extra keys must move them into `metadata`. Wrap a bare graph
with `GraphDocument::new(graph)`; `Engine::{check,prepare,compile}_document*` also check
`requires`.

## Host Bridge (since 2.0.0)

```rust
// Events are off by default; turn them on while debugging dropped payloads:
let config = EngineConfig::default().with_host_event_recording(true); // or DAEDALUS_HOST_EVENT_RECORDING=1
// take_inbound -> take_inbound_into (reuses your buffer)
let items = manager.take_inbound("host");              // before
let mut items = Vec::new();
manager.take_inbound_into("host", &mut items);         // after
```

Ports are `PortId`s: write paths (`push*`, `feed_payload`, `set_*_policy`, `close_input`) take
`impl Into<PortId>`, lookups (`take*`, `try_pop*`, `drain*`, `latest`, `bind_lane`, ...) take
`impl AsRef<str>`. `NodeIo` ports are `PortId`s and `NodeConstInputs` is keyed by them.
`CorrelatedPayload::correlation_id` is the payload's lineage id and `enqueued_at` an
`Option<Instant>` (set with basic metrics). `Coalesce` now clears a multi-item host FIFO like
`LatestOnly`. `ExecutionTelemetry::node_metrics` is a `NodeMetricsMap` (`get`/`entry` take the
index by value).

## Removed APIs

| Removed | Use | Since |
| --- | --- | --- |
| `daedalus_registry::type_key_of` | `PluginRegistry::type_index()`, typed pushes | 2.0.0 |
| global typing registry (`daedalus_data::typing::{register_type, register_enum, lookup_type, type_expr, snapshot_global_registry, reset_global_registry, ...}`) | `PluginRegistry::{type_registry, named_type_registry}`, `#[plugin(types(...))]` | 2.0.0 |
| global boundary contracts (`daedalus_transport::{BoundaryContractRegistry, register_boundary_contract, boundary_contract_for_type, ...}`) | `PluginRegistry::{register_boundary_contract, boundary_contract}` | 2.0.0 |
| `NodeIo::{push_any, push_output, push_output_default, take_outputs_small}` | `push`, `push_to`, `push_default`, `take_outputs` | 2.0.0 |
| `HostBridgeHandle::{push_payload, push_any, feed_payload_ref, next_correlation_id}` | `feed_payload`, `push` | 2.0.0 |
| `TypeKey::opaque` | `TypeKey::new` | 2.0.0 |
| `register_<type>_type` generated by `#[type_key]` | `#[plugin(types(...))]`, `register_daedalus_type` | 2.0.0 |
| `Outputs` derive | nothing (it was a no-op) | 2.0.0 |
| `daedalus_planner::helpers` | `NodeInstance::new`, `Edge::new` | 2.0.0 |
| `StateError::LockPoisoned`, `RunnerPoolError::LockPoisoned`, `StateStore::get_result` | infallible `parking_lot` APIs | 2.0.0 |
| `GPU_FEATURE_ENABLED` | `ENABLED_FEATURES` | 2.0.0 |
| `daedalus_core::platform::set_clock(fn)` | `EngineConfig::default().with_clock(Clock::new(f))`, `with_clock` on executors and `StreamGraph` | **75dc4c1** |
| `daedalus_core::platform::THREADS` | `cfg(feature = "threads")` | **75dc4c1** |
| `PayloadStorage::{into_any, into_any_arc}` | `Payload::try_into_owned` | **75dc4c1** |
| `daedalus_runtime::config::runtime_debug_config`, `EngineError::Io` | `RuntimeDebugConfig::from_env` | **75dc4c1** |

## Dynamic Plugins

Native plugins were the `ffi` feature in 2.0.0; see "Coming From The Pre-Release FFI" in
[`dynamic-plugins.md`](dynamic-plugins.md#coming-from-the-pre-release-ffi) for that move.

- **ABI 8 (since 75dc4c1).** `PLUGIN_ABI_VERSION` went 7 -> 8 (the descriptor gained
  `StableHandlers`). A plugin built against 75dc4c1 fails to load with `AbiMismatch`: rebuild it
  against 3.0.0.
- **Install paths (since 75dc4c1).** `install_into` now returns `InstallPath` and picks
  `RustAbi` (same Daedalus version, rustc and fingerprint; full Rust types, no per-call cost) or
  `Stable` (other rustc or Daedalus patch release; schema nodes only, values cross as
  `StableValue`s, about 1 µs per call). A plugin that used to fail with
  `PluginLibraryError::Incompatible` now installs through the stable path; `Incompatible` is only
  returned by `install_into_as(.., InstallPath::RustAbi)`, and `StableAbiMismatch` means neither
  path is available.

  ```rust
  // Before (75dc4c1)
  library.install_into(&mut registry)?;
  // After
  let path = library.install_into(&mut registry)?;      // InstallPath::RustAbi or ::Stable
  library.install_into_as(&mut registry, InstallPath::RustAbi)?; // to require the Rust ABI
  ```

  Plugins that share third-party Rust types with the host (adapters, serializers, typed
  `&FrameLease` ports) still need the Rust-ABI path, so build them in the same `cargo build` as
  the host; use `FrameView`/foreign interfaces to cross builds.
- **Leaf `cdylib` rule (since 2.0.0).** `export_plugin!` emits unmangled symbols; put it in a
  small leaf crate (`crate-type = ["cdylib"]`) instead of a plugin library that other crates
  link, or `--all-features` builds fail to link with duplicate `daedalus_plugin_abi_version`.
  See `examples/plugins/example_project_dylib`.
- **Checks before install (since 2.0.0).** `PluginLibraryError::{BoundaryTypeConflict,
  ForeignInterfaceMismatch, MissingDependencies}`: install the plugins that own shared types
  (e.g. Styx's `StyxFramesPlugin`) in the host before loading dynamic plugins.
  `BoundaryTypeMismatch` became `BoundaryTypeConflict`.

## GPU

- **wgpu 30 (since 75dc4c1).** Hosts that create wgpu objects themselves move to wgpu 30;
  `get_mapped_range{,_mut}` failures are `GpuError::Internal`.
- **dmabuf (since 75dc4c1).** `ExternalImportSupport::Supported` gained `acquire_fence_mode` and
  `fence_waits`; match with `..`. Acquire fences are waited for by `AcquireFenceMode::Auto`
  (`SyncFd`, then `Timeline`, then `Cpu`); set `GpuOptions::acquire_fence_mode` or
  `ExternalFrameDescriptor::with_acquire_fence_mode(AcquireFenceMode::Cpu)` for the old CPU
  wait.
- Driver setup is serialized process-wide and wgpu's `DEBUG` flag is only set with
  `WGPU_DEBUG=1`.

## FFI SDKs

- **Width-exact scalars (since 2.0.0).** Node ports have the exact width (`i32` is `I32`). Java
  maps `int` to `I32` and `long` to `Int`; `@Scalar("u32")` (on a parameter, or on the method for
  its output) declares unsigned widths. Workers' outputs are range-checked on the host.
- **`u64` on the wire (since 75dc4c1).** `WireValue::UInt` (`"uint"`) carries values above
  `i64::MAX`; each SDK writes `u64` ports as `uint`. Hosts matching on `WireValue` add the arm.
- **C++ (since 75dc4c1).** `DAEDALUS_NODE` names the function, goes after it, and types ports
  from `decltype(&fn)`; several outputs return a `std::tuple`; opaque types use
  `DAEDALUS_TYPE_KEY(T, key)`. `daedalus::Outputs`/`daedalus::outputs` and
  `daedalus::signature<F>()` are gone.

  ```cpp
  // Before
  DAEDALUS_NODE(split_sign, inputs(value), outputs(positive, negative))
  daedalus::Outputs split_sign_i64(int64_t value) {
    return (daedalus::outputs)("positive", value, "negative", -value);
  }
  // After
  std::tuple<int64_t, int64_t> split_sign(int64_t value) { return {value, -value}; }
  DAEDALUS_NODE(split_sign, inputs(value), outputs(positive, negative))
  ```
- **Java (since 75dc4c1).** Multi-output nodes return a record whose components are named after
  the outputs; `dev.daedalus.plugin.Outputs` is gone.

  ```java
  // Before
  public static Outputs splitSign(long value) { return Outputs.of("positive", value, "negative", -value); }
  // After
  public record SignSplit(long positive, long negative) {}
  @Node(id = "split_sign", inputs = {"value"}, outputs = {"positive", "negative"})
  public static SignSplit splitSign(long value) { return new SignSplit(value, -value); }
  ```
- `FfiHost` installs roll back on failure, and `RunnerPool::shutdown_all` returns a
  `RunnerShutdownError` (since 2.0.0).

## Other Changes

- Locks are `parking_lot` (no poisoning): state, context and resource methods whose only error
  was poisoning are infallible; drop the `?`/`unwrap`. (since 2.0.0)
- `TypeKey`, `PortId` and other text ids wrap `IdStr`; `From<&'static str>` does not allocate.
  (since 2.0.0)
- `Payload::owned` always builds typed storage; register boundary contracts on the
  `PluginRegistry`. (since 2.0.0)
- `RuntimeMode::Adaptive` decides per frame from measured costs; graphs of cheap nodes run
  serially. Mark expensive nodes with node metadata `daedalus.node.cost = "heavy"`. (since 2.0.0)
- Workspace manifests: `daedalus-runtime` and the facade are workspace dependencies without
  default features; enable `threads`/`std` where you inherit them. (since 75dc4c1)

## New, Nothing To Migrate

- **`no_std` and wasm** (since 75dc4c1 for runtime/engine): see "Portability" in
  [`development.md`](development.md#portability).
- **MCU profile** (since 75dc4c1): `daedalus-mcu` and `daedalus-mcu-build` compile host-planned
  graphs for microcontrollers; see [`mcu.md`](mcu.md).
- **Foreign interfaces and `FrameView`** (since 2.0.0): see "Foreign Interfaces" in
  [`node-authoring.md`](node-authoring.md#foreign-interfaces).
