# Changelog

All notable changes to this workspace should be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [2.0.0] - 2026-04-30

### Added

- Added the `daedalus-transport` crate with stable transport identities, payload storage,
  adapter declarations, boundary contracts, device transfer metadata, stream policies,
  residency/layout tracking, and payload lifecycle records.
- Added capability-oriented registry support for plugin manifests, type/node/adapter/device
  declarations, serializer declarations, capability snapshots, and deterministic capability
  resolution.
- Added a modular planner pass pipeline covering setup, hydration, embedded graph lowering,
  type validation, overload handling, adapter insertion, schedule generation, linting,
  explanations, suggestions, and plan metadata.
- Added runtime-owned executor infrastructure with compiled schedules, direct host routes,
  queue accounting, patching, reusable owned executors, stream graph support, and expanded
  serial/parallel execution tests.
- Added detailed runtime telemetry modules for node timing, transport/adapters, queue pressure,
  payload lifecycle, ownership/resource reporting, and compact telemetry summaries.
- Added host bridge manager, event, policy, serializer, and type modules for structured
  direct host I/O and streaming workflows.
- Added graph builder modules for scoped graph construction, typed handles/ports, edge policy
  metadata, nested graphs, and stricter validation.
- Added engine execution layers for prepared plans, host graph support, compiled runs, transport
  execution tests, and richer engine configuration validation.
- Added macro support for `#[adapt]`, device declarations, plugin declarations, branch payloads,
  type keys, richer node metadata, generic registration, config-backed ports, and UI compile
  tests for invalid macro use.
- Added GPU adapter selection, device capability reporting, staging/copy limiters, device caches,
  async polling helpers, WGPU backend resource modules, and dispatch/readback benchmarks.
- Added Rust dynamic plugin boundary coverage and shared FFI contract models.
- Added standalone example plugin crates under `examples/plugins` for a copyable Rust plugin
  project, math capabilities, and optional Styx `FrameLease` plugins.
- Added top-level runnable examples for quickstarts, typed ports, runtime configuration,
  transport behavior, async graphs, metrics/debugging, GPU fallback, and mixed CPU/GPU flows.
- Added CI/release helper scripts for workspace dependency checks and GPU async blocking audits.

### Changed

- Reworked the workspace around separated core, data, transport, registry, planner, runtime,
  engine, GPU, FFI, macro, daemon, and facade responsibilities.
- Bumped all workspace crates and standalone example plugin crates to `2.0.0`.
- Updated the facade crate exports for the new plugin, transport, host bridge, graph builder,
  macro, engine, and GPU surfaces.
- Reworked data conversion and typing, including named type registration, const coercion,
  value serialization, descriptor handling, JSON conversion, and conversion test coverage.
- Refactored planner internals from a monolithic pass module into focused pass modules with
  refreshed golden outputs.
- Reworked runtime plugin installation around plugin manifests, built-in capability providers,
  registry freezing, transport adapter registration, boundary contracts, and capability
  source tracking.
- Reworked runtime state management into context/resource modules with stronger lifecycle and
  type mismatch errors.
- Reworked executor queues, direct-slot handling, backpressure behavior, edge policies,
  runtime plan snapshots, and scheduling paths.
- Reworked engine cache/config/diagnostics/error handling and moved execution behavior into
  dedicated execution modules.
- Reworked daemon startup/service integration for the new engine/runtime APIs.
- Refreshed Rust, C/C++, Java, Node, and Python FFI packaging APIs, generated manifest builders,
  subprocess pack/bundle tests, SDK type surfaces, and plugin library loading.
- Refactored Java FFI bridge code into a checked-in bridge source file.
- Updated node bundles to install through the new plugin/capability registry instead of the
  removed planner/registry adapter shims.
- Reorganized README guidance and dependency snippets for `2.0.0`.

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
