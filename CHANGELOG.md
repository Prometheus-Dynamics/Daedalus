# Changelog

All notable changes to this workspace should be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [Unreleased]

### Changed

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

### Added

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

### Fixed

- Queue edges record their enqueue-to-dequeue wait in `EdgeMetrics` at `Detailed` (only direct
  slots did).

### Maintenance

- **Toolchain 1.99.0, MSRV 1.99.** `rust-toolchain.toml`, the CI workflows and the examples
  Docker image pin Rust 1.99.0; `rust-version` is 1.99 and every crate now inherits it (it was
  declared for the workspace but not applied to the packages).
- **Dependencies on their newest releases** (`cargo upgrade --incompatible --recursive`,
  `cargo update`). New majors: `syn` 3 (the proc-macro crates), `spin` 0.12 (same `no_std`
  features, `portable_atomic` on targets without compare-and-swap), `libloading` 0.9,
  `sha2` 0.11, `base64` 0.23, `pollster` 1.0 and `pyo3` 0.29 (declared, unused). `wgpu`/`naga`
  30, `ash` 0.38, `criterion` 0.8, `proptest` 1.11, `image` 0.25 and `tracing` 0.1 were already
  current; Styx follows `dev`. Nothing is kept back (docs/development.md, "Dependency Policy").
- New Rust 1.99 / clippy lints fixed in code: `as_chunks` instead of constant-size
  `chunks_exact`, `Waker::noop()` in tests, `mem::take` for drained waiter lists, no deprecated
  `core::u32::MAX` in `#[adapt]` expansions.
- The MCU blink firmware's compiled size dropped with the toolchain: 3168 -> 3072 B
  (`thumbv7em`), 3716 -> 3392 B (`thumbv6m`); RAM unchanged (`MCU_COMPILED_SIZE`,
  docs/mcu.md).

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

- `scripts/ci.sh` subcommands (`lints`, `test`, `macro-ui`, `aarch64`, `lean`, `features`,
  `nostd`, `wasm`, `mcu`, `smoke`, `bench`, `pi`, `vvl`) and matching CI jobs; CI builds the
  workspace with `--all-features` (the `example_project` export moved to the leaf
  `example_project_dylib` crate).
- `scripts/check-file-sizes.sh` fails on any oversized file (empty baseline); large files split.
- Duplicate code consolidated across macros, FFI language crates and builtins;
  `docs/host-bridge-lock-granularity.md` documents the lock order.
- The optional Styx examples track Styx `dev` and key `FrameLease` as `styx:framelease`.

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
