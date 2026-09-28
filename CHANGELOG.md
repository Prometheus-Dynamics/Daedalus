# Changelog

All notable changes to this workspace should be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [Unreleased]

### Added

- Added the `dylib-plugins` facade feature: `PluginLibrary` loads native Rust `cdylib` plugins
  exported with `export_plugin!`, rejecting mismatched plugin ABI, Daedalus version, rustc
  version, or build fingerprint with a typed `PluginLibraryError` before any Rust type crosses
  the boundary. `discover_plugin_libraries` finds plugin libraries across directories.
- Added `GraphDocument`, a versioned persisted graph format (`format: "daedalus.graph"`,
  `schema_version`, `requires`, `metadata`, `graph`), with plugin requirement checks on
  `PluginRegistry` and `Engine::{check,prepare,compile}_document*` wrappers.
- Added host port introspection: `RuntimePlan::host_ports*` and
  `HostGraph::{host_inputs, host_outputs}` return typed `HostPortDescriptor`s.
- Added payload inspection: `HostGraph::inspect_payload` and `inspect_payload` render payloads
  through registered value serializers, falling back to a `PayloadSummary`;
  `daedalus_data::json::to_plain_json` renders `Value`s as plain JSON.
- Added input-driven host graph execution: `HostBridgeHandle::inbound_waiter` (blocking and
  `Future`), `HostGraph::{wait_for_input, tick_on_input, drive_blocking, drive}`, and
  `HostGraphStopHandle`.
- Added `FfiHost` to `daedalus-ffi-host`: `FfiHost::install_package(&mut registry, &package,
  &factory)` installs a package and starts its persistent-worker runners, `invoke(node_id,
  request)` invokes by node id with typed `FfiHostError`s, and `add_package` installs further
  packages into the same runner pool and telemetry. Installs roll the registry back on failure.
  `FfiHostBuilder` shares telemetry, sets pool options, and defers runner startup;
  `PersistentWorkerRunnerFactory` is the built-in factory for persistent-worker backends.
- Added `docs/node-authoring.md`, `docs/dynamic-plugins.md`, and a measured minimal CPU-only
  profile in `docs/development.md`.
- Added zero-copy dmabuf import in `daedalus-gpu`: `ExternalFrameDescriptor`/`ExternalPlane`
  (Linux, owned fds plus an `ExternalKeepalive` released when the GPU is done), `DrmFourcc`,
  `GpuBackend::{dmabuf_import_support, import_dmabuf}` and `GpuContextHandle` wrappers with typed
  `ExternalImportSupport`/`ExternalImportError`. The mock backend records imports; the new
  `gpu-dmabuf` feature (daedalus-gpu and facade; implies `gpu-wgpu`) implements it on Vulkan via
  wgpu-hal, enabling `VK_EXT_image_drm_format_modifier` at device creation when available.
- Added `GpuFormat::{Rg8Unorm, Bgra8Unorm}` (used by imported frames).

- Added facade features `executor-pool`, `metrics`, `engine-full` (`engine` + `executor-pool`
  + `metrics`, the recommended application preset) and `embedded` (`engine` + `plugins`, no
  pool, no metrics).
- Added `ENABLED_FEATURES` constants to `daedalus-rs`, `daedalus-core`, `daedalus-data`,
  `daedalus-registry`, `daedalus-planner`, and `daedalus-runtime`, plus
  `daedalus::dylib::{boundary_features, describe_fingerprint_mismatch, HOST_ONLY_FEATURES}`.

### Changed

- The facade `engine` feature no longer enables the Rayon executor pool or metrics; use
  `engine-full` for the previous behavior. Without `executor-pool`, parallel/adaptive runtime
  modes run on scoped threads. The workspace `daedalus-engine` dependency now sets
  `default-features = false`.
- The facade `gpu-runtime` feature now also enables `daedalus-engine/gpu` when `engine` is on,
  fixing the `engine,plugins,gpu-mock` build (planner and engine GPU config disagreed).
- The dynamic plugin build fingerprint now covers the enabled boundary-relevant features of
  every Daedalus crate whose types cross the plugin boundary (readable lists plus a stable
  hash) and the size/alignment of `PluginRegistry`, `HandlerRegistry`, `Payload`, `TypeKey`,
  `BoundaryTypeContract`, `TypeExpr`, `NodeDecl`, `AdapterDecl`, `PluginManifest`, and
  `StrView`. `PluginLibraryError::BuildFingerprintMismatch` gained a `differences` field naming
  the differing segments.
- Plugin manifests declared with `#[plugin]` or `declare_plugin!` now record the plugin crate
  version.
- `BoundaryVTable` gained a `value_any` field used by `Payload::value_any_sync`.
- Host bridge event recording is now off by default (`DEFAULT_HOST_BRIDGE_EVENT_RECORDING`,
  `HostBridgeConfig::default()`, `RuntimeSection::host_event_recording`, new manager handles).
  Enable it explicitly for diagnostics; stats and pressure tracing are unaffected.
- Host port arguments are consistent across `HostBridgeHandle` and `HostGraph`: write paths
  (`push*`, `feed_payload`, `set_*_policy`, `set_latest_*`, `close_input`,
  `profiled_feed_tick_drain_owned`) take `impl Into<PortId>`; lookups (`take*`, `try_pop*`,
  `drain*`, `latest`, `is_input_closed`, `direct_host_route`, `bind_lane`,
  `tick_direct_payload`, `run_direct_once`) take `impl AsRef<str>` and do not allocate.
  `HostBridgeHandle::feed_payload_ref` was removed (use `feed_payload`); `PortId` and the other
  text ids implement `From<&Id>`. `HostBridgeManager::ensure_handle` takes `impl AsRef<str>`.
- Host bridge buffers keep one per-port state (queue, policy overrides, freshness watermarks,
  close flag) per direction, so a push does a single lookup; freshness watermarks are no longer
  shared between an input and an output port with the same name. Replace-style capacity-one
  policies (including the default) store the value in a single slot replaced in place, and
  recording an event no longer clones the payload.
- `HostGraph::drive` docs describe running `drive_blocking` under
  `tokio::task::spawn_blocking` with a `HostGraphStopHandle`; added the `host_graph_drive`
  criterion bench to `daedalus-engine`.

### Removed

- Removed `daedalus_runtime::GPU_FEATURE_ENABLED` and `daedalus_registry::GPU_FEATURE_ENABLED`;
  use `ENABLED_FEATURES`.

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
