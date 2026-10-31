# TODO

## Next release (3.1.0)

Feature freeze of 2026-10-27, plus its last addition, hardware support and GPU (2026-10-30).
Nothing else joins this release. Each item says **what** it is, **why** it is in the release,
**done when** (a checkable test, gate, measurement or board run) and **blocked by**.
Board-gated items are marked **[board]**.

### 1. Freeze-list items still open

- [ ] **What:** confirm the stale-graph refresh on the Raze (`refresh_graph_ports_with` +
      `drop_stale_consts`, bb07e3b).
      **Why:** HeliOS's stored rev-215 graph failed to plan (`PortExtra` on folded Eidos knobs).
      **Done when:** HeliOS dev on the Raze plans and runs the default pipeline from the stored
      graph and logs the dropped consts.
      **Blocked by:** a HeliOS deploy carrying bb07e3b (the HeliOS coordinator).
- [ ] **What:** frame interface v3 follow-ups.
      **Why:** v3 shipped (SharedFrame, colorimetry, dylib reload); further changes only on
      consumer request.
      **Done when:** no consumer request is open at release time, or each request has a merged
      commit and a test.
      **Blocked by:** a consumer asking (Styx, HeliOS, Eidos); none open.

### 2. Hardware support and GPU

GPU is an explicit backend chosen by the consumer (per node or per graph), like a SIMD
feature. Daedalus provides residency, `#[device]` adapters and transfers, capabilities, timing,
and a clean explicit choice. It does no cost-based CPU/GPU selection.

- [ ] **What:** the `gpu_bench` report on the Raze CM5 (v3dv): limits, dispatch latency, kernel
      throughput against the CPU, and dmabuf frame import as a storage buffer vs image + copy.
      **Why:** it fixes V3D's real budget for Eidos dense depth, and shows whether LINEAR camera
      frames can reach compute zero-copy.
      **Done when:** the report is pasted into `docs/testing.md` ("Validating on a Raspberry
      Pi 5"), with every `_verify` line `ok`.
      **Blocked by:** a Raze window (HeliOS coordinator).
- [ ] **What:** the `gpu_probe` segfault at exit under the cross-built Khronos validation layer
      on v3dv.
      **Why:** VVL is how new Eidos kernels get validated; a crashing run hides real errors.
      **Done when:** `./scripts/ci.sh vvl` on the Raze exits 0, with no layer errors from our
      code.
      **Blocked by:** a Raze window.
- [ ] **What:** compute limits and features in `GpuCapabilities`: workgroup size and
      invocations, workgroup storage, storage buffer size, f16, int64, int64 atomics,
      timestamps, subgroups.
      **Why:** a library picks a kernel variant, or refuses the GPU backend, from facts rather
      than from trial dispatches.
      **Done when:** unit tests against the mock backend pass, and `gpu_probe` on the Raze
      prints the same values as `gpu_bench`'s `[limits]`.
      **Blocked by:** nothing (the Raze check needs a window).
- [ ] **What:** explicit device assignment. The per-node `compute` and a graph-level default
      choose CPU or GPU. The planner honours it, `GpuRequired` fails planning without an
      adapter, and `explain_plan()` reports the resolved device per node (today it reports only
      the requested affinity). Any fallback is visible, never silent.
      **Why:** user direction (2026-10-30): the consumer chooses the backend; the planner does
      not.
      **Done when:** planner and runtime tests cover CPU, GPU and missing-adapter cases, and
      each case's resolved device appears in `explain_plan()`; `docs/node-authoring.md`
      documents it.
      **Blocked by:** nothing.
- [ ] **What:** `GpuContextHandle::wgpu() -> Option<(&wgpu::Device, &wgpu::Queue)>`, so a node
      reaches the engine's own device.
      **Why:** Eidos nodes keep pipelines and persistent buffers in `state(T)`; without it they
      open a second device (Eidos GPU worker, 2026-10-30).
      **Done when:** a node test builds a pipeline on `ctx.gpu`'s device and dispatches on its
      queue.
      **Blocked by:** nothing.
- [ ] **What:** `#[device]` upload/download functions can take `&GpuContextHandle`, and the
      runtime passes it (as `DeviceBridge::upload(self, ctx)` already allows).
      **Why:** a real upload adapter has to create buffers on the engine's device; otherwise
      transfers stay inside handlers and never become plan edges, so `explain_plan()` cannot
      show them.
      **Done when:** a `#[device]` type with context-taking upload/download round-trips through
      a graph, and its edges show DeviceUpload/DeviceDownload in `explain_plan()`; the existing
      `fn(&Cpu)` form still compiles.
      **Blocked by:** nothing.
- [ ] **What:** `GpuStorage`, a sized persistent storage buffer kept in a node's `state(T)`
      across ticks (TSDF volume, cost-volume scratch).
      **Why:** Eidos M1 (plane sweep) keeps GPU state for the node's lifetime.
      **Done when:** a test keeps a buffer across 100 ticks with no reallocation, and the buffer
      is dropped on `Restart` and on reload.
      **Blocked by:** nothing.
- [ ] **What:** `GpuSubmission`, a non-blocking completion API. Submit on tick N;
      `try_complete()` polls (never waits) on later ticks; there is an in-flight limit.
      **Why:** the vision core must never wait on a GPU fence inside a tick.
      **Done when:** a graph test shows the tick never blocks on the fence (tick time stays
      under the kernel time) and the result arrives on a later tick.
      **Blocked by:** nothing.
- [ ] **What:** `read_buffer_into(&GpuStorage, range, &mut [u8])`, plus a non-blocking form
      through `GpuSubmission`, with a persistent staging ring.
      **Why:** Eidos mesh and depth readback, with no allocation per download.
      **Done when:** a test reads back 1000 times with zero allocations after warm-up (counting
      allocator) and correct contents.
      **Blocked by:** nothing.
- [ ] **What:** `RuntimePlanExplanation::device_round_trips()`, which lists CPU→GPU→CPU chains
      across nodes.
      **Why:** a consumer test asserts mechanically that a GPU pipeline has none.
      **Done when:** tests cover a graph with a round trip (reported with nodes and ports) and
      one without (empty).
      **Blocked by:** nothing.
- [ ] **What:** GPU time per dispatch: timestamp queries where the device has them, otherwise
      submit→fence time, fed into the frame-overhead stages.
      **Why:** GPU ms per keyframe for Eidos; v3dv timestamp support is unconfirmed.
      **Done when:** a test sees nonzero GPU time per GPU node, and the Raze report states
      which source v3dv uses.
      **Blocked by:** the bench run (timestamp support on v3dv).
- [ ] **What:** import a LINEAR dmabuf as a storage buffer (public API).
      **Why:** zero-copy camera frames for packed-u32 compute on v3dv, where LINEAR images
      cannot be sampled.
      **Done when:** a hardware test imports, runs a kernel and verifies the output, on RADV and
      on the Raze.
      **Blocked by:** the bench result (whether v3dv imports dmabuf buffers).
- [ ] **What:** an on-disk pipeline cache across runs (wgpu `PipelineCache` on Vulkan), keyed by
      adapter, driver and shader hash.
      **Why:** startup time on the CM5.
      **Done when:** a second process start creates its pipelines measurably faster (reported by
      `gpu_bench`), and a driver change invalidates the cache.
      **Blocked by:** nothing.
- [ ] **What:** a buffer pool with size classes, per-frame reuse, a memory cap and stats,
      replacing `SimpleBufferPool`'s exact-size matching.
      **Why:** steady-state GPU nodes should not allocate per frame.
      **Done when:** a 1000-frame test with varying sizes stays under the cap, and its reuse
      rate is reported.
      **Blocked by:** nothing.
- [ ] **What:** a guide to writing GPU nodes: a `#[device]` WGSL node with its CPU pair,
      persistent state, non-blocking completion and readback, plus a tested example.
      **Why:** Eidos and other libraries ship GPU implementations behind features.
      **Done when:** the example runs in CI (CPU path) and on the Raze (GPU path).
      **Blocked by:** the items above.
- [ ] **[board] What:** K230D, CPU-only: a `riscv64gc-unknown-linux-gnu` build with wgpu
      feature-gated off, and `#[device]` nodes running their CPU implementation.
      **Why:** the K230D has no compute-capable GPU.
      **Done when:** the cross-build check is in `ci.sh`, and a CPU-only graph test passes on
      the K230D.
      **Blocked by:** nothing for the build; a K230D board for the run.
- [ ] **[board] What:** RK3588S (Mali-G610): `gpu_probe`, the `ci.sh pi` hardware tests and
      `gpu_bench` with the shipped driver (libmali or PanVK).
      **Why:** proper hardware support on every board.
      **Done when:** the reports are in `docs/testing.md`, and the hardware tests pass or each
      gap is documented, with the copy fallback in place.
      **Blocked by:** a board (the PCB dev board) and Gaia's driver choice.
- [ ] **[board] What:** RK3576, RK3566 and RK3562 (Mali-G52): the same reports, or a confirmed
      clean CPU fallback where the driver has no usable Vulkan compute.
      **Why:** as above.
      **Done when:** as above, per chip.
      **Blocked by:** boards and Gaia's driver choice.

A CUDA backend is not in this release. If one comes later, it needs:

- a second `GpuBackend` implementation;
- a residency key per backend, so CUDA and wgpu device memory are distinct port types;
- upload/download adapters, and CUDA↔Vulkan interop (`cudaImportExternalMemory`) if both run
  in one graph;
- `#[device]` implementations keyed by backend instead of a single GPU slot.

The explicit assignment above already carries a backend choice.

### 3. Release mechanics

- [x] **What:** the internal API dedup pass. **Why:** the freeze list. **Done when:** merged
      with the gate green (9561d9b..f6299ad). **Blocked by:** nothing.
- [ ] **What:** run the semver check against `v3.0.0`, and set `SEMVER_BASELINE` to the new tag
      afterwards.
      **Why:** decides between 3.1.0 and 4.0.0 (`NodePortRefresh` and other public structs
      changed).
      **Done when:** `./scripts/ci.sh semver` reports no unexplained break, and the CI `semver`
      job is blocking.
      **Blocked by:** nothing.
- [ ] **What:** the version number. **Why:** dev is `3.0.1-dev`; this release adds API.
      **Done when:** every crate is bumped to the agreed version, and the plugin-ABI and
      stable-ABI constants are unchanged or bumped with a reason. **Blocked by:** a user
      decision (3.1.0 proposed; 4.0.0 if semver reports breaks).
- [ ] **What:** CHANGELOG `[Unreleased]` becomes `[3.1.0]`, with a migration note if needed.
      **Why:** downstream pins by commit and reads the CHANGELOG.
      **Done when:** every merged change since `v3.0.0` has an entry.
      **Blocked by:** the items above.
- [ ] **What:** release gates. **Why:** release quality.
      **Done when:** `./scripts/ci.sh full` passes, `ci.yml` and `bench.yml` are green on `dev`,
      and `ci.sh pi` and `ci.sh vvl` pass on the Raze.
      **Blocked by:** a Raze window.
- [ ] **What:** merge `dev` into `main`, and tag `v3.1.0`.
      **Why:** the release. **Done when:** the tag exists and downstream bumps its pin.
      **Blocked by:** every item above, and user approval.

## Backlog (not for this release)

The maturity pass after 2.0.0 (a feature branch, merged into `dev`) was released as 3.0.0. It was
driven by host-integration reviews. See `CHANGELOG.md` `[3.0.0]` for the full list of changes
and `docs/migration-3.0.md` for upgrading.

### Done

#### Host integration
- [x] Native Rust `cdylib` plugin loading (`dylib-plugins`): stable `#[repr(C)]` descriptor, schema
      introspection, and typed rejection on rustc/version/fingerprint mismatch.
- [x] Declarative boundary/host-only feature classification (`[package.metadata.daedalus]`) with
      generated `ENABLED_FEATURES`.
- [x] `GraphDocument` (`daedalus.graph` v1): strict parsing with JSON-path errors, plugin
      requirements, checked-in JSON Schema (`docs/schema/daedalus.graph.v1.schema.json`).
- [x] Host graph APIs: typed host ports (`input_typed` / `input_as`), port introspection,
      `inspect_payload` / `inspect_outputs`, per-port stats, and input-driven `drive_blocking` /
      `drive`.
- [x] Lean presets: `embedded`, `engine`, `engine-full`; the executor pool and metrics are opt-in.
- [x] `FfiHost` one-call package host with rollback and aggregated shutdown.
- [x] dmabuf zero-copy GPU import (`gpu-dmabuf`): acquire fences and single-image NV12.
- [x] Docs: `docs/node-authoring.md`, `docs/dynamic-plugins.md`, minimal CPU-only profile, and the
      `external_frame_source` example (camera glue pattern with no camera-library dependency).
- [x] Decision: Daedalus never depends on a camera library. The crate that defines a type owns its key and
      Daedalus registration behind an optional `daedalus` feature (documented in
      `docs/node-authoring.md`, "Library-Owned Integration Features"); without one, the frame
      glue lives in the application.
- [x] **Deterministic port keys for foreign types.** Macros resolve a type's own key at compile
      time; ports take `type_key = "..."`, plugins `foreign_types(Type = "key")`; an unkeyed
      foreign type fails install (`UnkeyedForeignType`) instead of getting an order-dependent
      `rust:` key.
- [x] **Boundary type check for dylib plugins.** A host found that a plugin built in a separate
      cargo invocation passed the fingerprint check but failed on every frame with
      `payload type mismatch: expected example:frame, found example:frame`. Now the
      descriptor (ABI 6) exports `(TypeKey, TypeId hash, size, align, type_name)` per boundary
      type, `install_into` fails with `BoundaryTypeConflict` naming every differing key before
      installing anything, adapter errors say "same TypeKey, different Rust type", and the
      same-cargo-build rule is documented in `docs/dynamic-plugins.md`.
- [x] **Foreign (host-owned) types for separately built plugins.** Foreign interfaces
      (`foreign_interface!`: `#[repr(C)]` accessor vtable, key, version, layout hash), owner
      providers registered as zero-copy `View` adapters, `ForeignHandle` payloads with an
      `Arc`-backed keepalive, and node inputs `FrameView<'_>` / `ForeignRef<'_, I>`. The standard
      `daedalus:frame` v1 interface (`docs/foreign-frame-interface.md`) lets owners such as
      camera-library crates implement `FrameSource` in their `daedalus` feature. The descriptor (ABI 7)
      exports the interfaces a plugin uses and `install_into` refuses version/layout mismatches;
      `examples/plugins/foreign_consumer`, built separately with a different copy of the type's
      crate, reads host values in place.
- [x] **Generic runtime key lookups.** `type_key_of` is gone. `PluginRegistry::type_index()`
      freezes `TypeId → TypeKey` (builtins and `Option`/`Vec` of them, the key a type owns in
      that registry, else the one key its ports use) and `TypeKey → RustTypeIdentity`; graph
      builders and compiled graphs capture it, so `push::<T>`, `bind_input`, `bind_lane`,
      `run_once`, `HostBridgeHandle::push`, `NodeIo::push_to` and `input_typed` no longer depend
      on install order or on other registries, and an unknown type is a `TypeKeyError` naming
      the fixes. The host bridge rejects payloads whose key is registered for another Rust type
      (`FeedOutcome::Rejected`), and key registration is strict (identical is a no-op, another
      type, key or declaration fails; one `BoundaryTypeConflict` for static and dylib installs).

- [x] **Optional inputs.** `Option<T>` inputs carry `T`'s key (producers of `T` connect
      directly; before, the edge had no converter), are marked `PortDecl::optional`, and never
      block: a node runs when each connected required input has a value and is skipped
      otherwise; optional inputs are `None` without one. `Result<Option<T>, _>` returns are
      conditional outputs. See "Optional Inputs And Readiness" in `docs/node-authoring.md`.
- [x] **Cross-tick joins.** `fire = "all"` nodes (`#[node(fire = "all")]`,
      `GraphBuilder::fire_all`, `daedalus.node.fire` metadata) peek their required edges and wait,
      popping nothing, until each holds a value, then take one value per edge; optional inputs
      never block, edge policies decide what is held, and the planner lints joins on producers
      that may not produce. Per-edge policies now also survive the scheduler default.
- [x] **Typed host ports in embedded graphs.** Embedded-graph expansion and `nest` carry an
      inner host port's declared type to an undeclared outer host port wired to it.
- [x] **Stable plugin handler path.** Plugins built with another rustc or Daedalus patch release
      (same `PLUGIN_ABI_VERSION`) install and run: the descriptor's `StableHandlers`
      (`STABLE_ABI_VERSION` 1) run schema nodes through a C-ABI `invoke` with `StableValue`s
      (scalars inline, strings/bytes/nested values borrowed, frames as foreign handles), node port
      codecs registered by the node macros convert to and from the handlers' Rust types, and
      `install_into` picks `InstallPath::RustAbi` or `InstallPath::Stable`
      (`install_into_as` forces one). `examples/plugins/stable_abi` and the facade's
      `dylib` test (`stable` module) cover it.
- [x] **`export_plugin!` boundary contracts in the schema.** `PluginSchema::boundary_contracts`
      lists them next to the plugin's own.
- [x] **Graph JSON Schema drift.** Enum lists come from each core type's `ALL` (guarded by an
      exhaustive match), and a test validates every variant against the generated schema.
- [x] **Enum config ports.** `NodeConfig` enum fields and enum handler inputs failed
      with `missing <port>`: the engine never handed the registry's const coercers to its
      executors and owned/borrowed inputs never coerced `Value` constants. The node macros now
      register a coercer per input and config field type at install (`DaedalusTypeExpr` unit
      enums by name or index, else serde); see "Constants, Defaults And Config Enums" in
      `docs/node-authoring.md`.
- [x] **Ids from other macros.** `#[node]`/`#[adapt]`/`#[plugin]` accept any `&'static
      str` constant expression as `id` (`concat!`, a user macro, a `const`).
- [x] **Macro leaf keys without an owned key.** Port types without a `#[type_key]` (a
      `foreign_types` mapping) resolve through the installing registry's `TypeRegistry`
      (`node_decl_in`, `handler_registry_in`, ...), handlers compute output keys once per
      registry, and the process-global typing registry is gone. Dynamic plugins resolve a
      dependency's mapping through the registry they install into: the introspection registry
      (where `export_plugin!(.., deps [..])` installs linked dependencies first) or the host's.
- [x] **Dynamic plugin dependencies.** Schemas list a plugin's dependencies and
      `install_into` refuses a plugin whose dependencies the host has not installed
      (`MissingDependencies`). `export_plugin!(P, deps [Dep])` links dependency plugins into
      the library's introspection registry, so keys they map resolve; without the link,
      unkeyed foreign port types are listed as `external_types` instead of failing the schema.
- [x] **One key per builtin number.** `i32`/`u32`/`f32` (and every other width) shared
      `Int`/`Float` with `i64`/`f64`: plugins with `i64` and `i32` ports conflicted and a fanned-out
      `i64` host input was branched by the `i32` adapter. Each builtin number now has its own
      `ValueType` and key, the planner inserts lossless widening adapters (`i32 -> i64`,
      `f32 -> f64`, ...) and rejects narrowing at plan time, and constants are range-checked
      against the port's exact width. See "Builtin Numbers" in `docs/node-authoring.md`.
- [x] **Direct routes deliver const inputs.** The single-node direct route (`run_direct_once`,
      lanes, `tick_direct_*`) skipped const inputs, so constants, port defaults and config fields
      were `missing`; it now delivers them like a scheduled tick (`tests/it/direct_const_inputs.rs`).
- [x] **Typed nodes with three reference parameters.** `fn(&A, &B, &mut State)` was taken for
      the low-level `(node, ctx, io)` form; the form is now recognized by parameter types.

#### Performance
- [x] Process-global boundary contract registry removed; `Payload::owned` always uses typed storage.
- [x] Host bridge: per-port state, single-slot latest-only queues, events off by default.
- [x] Executor: direct bridge handles, allocation-free ticks (31 → 4 allocations per round trip,
      enforced by `crates/engine/tests/hot_path_allocations.rs`), `IdStr` for static ids, cheaper
      `Payload`.
- [x] Shared `PolicyQueue<T>` for host ports and executor edges.
- [x] Generated handlers allocate nothing per frame: stateful nodes keep state in per-node
      `StateStore` slots instead of formatting a key per call, and configs and `&T` constants are
      decoded once per change (`daedalus_runtime::const_cache`), so the detector graph frame is
      its 31 payload allocations.
- [x] **Java and C++ SDK integer widths.** Both SDKs declare width-exact scalars (Java
      `@Scalar` for unsigned widths); the host range-checks worker outputs. C++ registrations
      always type ports from `decltype(&fn)` (unmapped types fail to compile), and Java
      multi-output nodes type each output from a returned record's components.
- [x] **`u64` on the FFI wire.** `WireValue::UInt` (`"uint"`) carries values above `i64::MAX`;
      every SDK's wire encoder writes `u64` port values as `uint`.
- [x] **Owned constants decode once.** Owned `T`/`Option<T>` parameters fed a non-builtin
      constant clone the value decoded into the per-node cache (`T: Clone`, probed by the macro)
      instead of converting it every call.
- [x] Benchmarks: `crates/engine/benches/host_graph_drive.rs`, plus `.github/workflows/bench.yml`
      with regression flagging.

#### CI, examples and daemon
- [x] CI runs on pushes to `dev` as well, and `dev` saves the Rust cache, so PRs into `dev`
      restore it.
- [x] `ci.sh smoke` runs the quickstart and basics bins and the new `09_hosts` bins.
- [x] Examples for the host-side features that only had tests: execution domains
      (`09_hosts/execution_domain`), node fusion (`03_transport/node_fusion`), held inputs and
      batch pushes (`09_hosts/held_inputs`), multi-camera feeders (`09_hosts/multicam_feeders`),
      graph documents (`09_hosts/graph_document`), hot reload (`09_hosts/graph_hot_reload`) and
      node recovery (`09_hosts/node_recovery`).
- [x] Daemon tests: the stdio protocol through the binary, and the TCP protocol with `serve` and
      the CLI behind the `tcp` feature (run by `ci.sh test`).
- [x] Docs match the tree: UI test path, `repo-clean.sh` runs the default CI loop, the facade's
      feature list, and the duplicate dependency roots from the lock.

#### Code health
- [x] `parking_lot` everywhere outside `core`/`transport`; poison handling removed.
- [x] Oversized files split; the strict file-size check now fails on new oversized files.
- [x] Duplicate code consolidated: macros, FFI language crates, edge explanations, `Value`
      accessors, text ids, GPU format tables, build scripts, builtins, host bridge config, test
      graph builders (`NodeInstance::new`, `Edge::new`).
- [x] Dead code removed (`Outputs` derive, aliases, unused globals); `CapabilityRegistry::remove_plugin`
      added.
- [x] **`--all-features` workspace builds link.** Example plugins depending on
      `example_project` duplicated its `export_plugin!` symbols once features unified; the export
      lives in the leaf `examples/plugins/example_project_dylib`, and CI builds (links) the
      workspace with `--all-features`.
- [x] CI: aarch64 check, lean-preset tests, macro UI and dylib jobs, example smoke runs, and
      `scripts/ci.sh` subcommands.
- [x] Portability tier 1: `no_std` + `alloc` core/transport/data/registry/planner (default `std`
      feature), the `embedded` preset running on `wasm32-unknown-unknown` (`platform::THREADS`,
      `platform::Instant`), and the `portability` CI job (`scripts/ci.sh nostd wasm`).
- [x] Bugs fixed: schema export/import encoding round trip, unknown wgpu formats silently treated
      as RGBA8, `Coalesce` never shrinking host FIFOs, input and output freshness watermarks
      shared by name, and broken CI feature checks.

### Remaining

#### High priority
- [x] **Validate on a Pi 5 / CM5 with Mesa v3dv** (1512568, abe7ad4, d02276e). Run `./scripts/ci.sh pi` on the device and
      paste the `gpu_probe` report (fence path, per-format modifiers, LINEAR NV12, `DISJOINT`,
      `TEXTURE_FORMAT_NV12`, the fence export ioctl, dma-heaps) and the `--nocapture`
      measurements of the fence tests; see "Validating on a Raspberry Pi 5" in
      `docs/testing.md`.
- [x] **Frame-path overhead on the CM5.** Measured `detector`, `frame_chain` and
      `shared_detectors` at `bcc9f33` (4939e42, 3367fb2; `docs/development.md`): detector
      `graph_overhead` p50 7.2 µs unfused / 6.8 µs fused, a real detector graph 16-17 µs.
- [x] **First GitHub Actions run** of `ci.yml`: every job (workspace, facade with the dylib and
      macro-UI tests, docs-and-lints, package-surface, aarch64, lean-preset, portability with
      nostd/wasm/MCU) runs and passes on pushes to `dev` (e.g. run 40175816440).
- [x] **`bench.yml` on `dev`.** Its push trigger now covers `dev` as well as `main`, with the same
      path filters, and the baseline is the newest earlier run on the same branch (it was the
      default branch, which lacked the file). Its schedule and `workflow_dispatch` still fire from
      `main` only. The first dev runs (40532524948 onward) worked, but comparing against an earlier
      job's artifact flagged runner speed differences (+15-45% on every bench, including a
      docs-only commit), so each run now benches the previous run's head commit on the same
      runner.
- [x] **Tags.** `v2.0.0` (8946223) and `v3.0.0` (e7e88fc) exist. Dev work is versioned
      `<major>.<minor>.<patch>-dev` (now `3.0.1-dev`): only the patch moves during dev, major and
      minor change only for an agreed release, and changes go under CHANGELOG `[Unreleased]`.
      Downstream pins by commit and depends on Daedalus by git without a `version =`
      requirement (`3.0.0` does not match `3.0.1-dev`).
- [x] **Faster local builds and `ci.sh`** (f7edb7d): `ci.sh -j` parallel lanes, `ci.sh quick`,
      smoke reusing the test build, `ci.sh doc`/`full`; target dirs belong on a local SSD (the
      external drive made the edit loop ~6x slower).

#### Medium priority
- [x] **Public API review.** 266 public items have no use outside their own file (the TODO's
      estimate was about 130). Each is kept (downstream, documented, or a builder family),
      hidden with `#[doc(hidden)]`, made `pub(crate)` (test-only where so), or removed; 55 were
      removed. Classified in `docs/api-review.md`, with the replacements in CHANGELOG
      `[Unreleased]`.
- [x] **dmabuf: GPU-side fence wait with a timeout.** `AcquireFenceWait::Timeline`: a timeline
      semaphore host-signaled by one watcher thread per device on fence or `acquire_timeout`,
      `GpuImageHandle::acquire_status()` reports `TimedOut`; `SyncFd` (no timeout) and `Cpu`
      fallbacks. Every import does a queue-family-foreign acquire into a known wgpu state and a
      release back on drop.
- [x] **dmabuf: configurable fence wait, `SyncFd` by default.** `AcquireFenceMode`
      (`Auto` = `SyncFd` -> `Timeline` -> `Cpu`, or an explicit mode falling back to `Cpu`) as a
      backend default (`GpuOptions::acquire_fence_mode`) with a per-import override;
      `ExternalImportSupport` and `gpu_probe` report the mode, its wait and the available waits.
- [x] **dmabuf: hardware tests under the Vulkan validation layers.** `scripts/ci.sh vvl`
      (`docs/testing.md`); on RADV the NV12 view-format list was the one error in our code, and the
      only remaining message is the `Timeline` mode's `VUID-vkQueueSubmit-pWaitSemaphores-03238`
      (wgpu-hal's binary semaphore chain behind a pending timeline wait).
- [x] **Parallel test runs segfaulted** in the Vulkan loader (concurrent instance creation with
      the NVIDIA ICD installed; debug-utils terminators racing with instance/device creation).
      `daedalus-gpu` serializes driver setup and creates instances without wgpu's `DEBUG` flag
      unless `WGPU_DEBUG=1`.
- [x] **dmabuf: compressed modifiers.** Multi-memory-plane modifiers (aux planes) import; on RADV
      every renderable `R8`/`XRGB8888` modifier including DCC (2 and 3 planes) round-trips between
      devices (`dmabuf/tests/modifiers.rs`), tiled NV12 imports and samples.
- [ ] **dmabuf: `Timeline` mode blocks the next submission on Mesa** and trips
      `VUID-vkQueueSubmit-pWaitSemaphores-03238`: wgpu-hal chains submissions with binary
      semaphores, which Mesa only accepts once the previous (wait-before-signal) submission
      reached the kernel. It is opt-in now (`SyncFd` is the default); revisit if wgpu-hal relays
      with timeline semaphores (then `Timeline` would neither block nor violate the VUID).
- [x] **dmabuf: validation layers on the Pi** (d02276e; the exit segfault is a release item). Run `./scripts/ci.sh vvl` on v3dv alongside the
      `pi` report (only RADV has been validated).
- [x] **`NodeRecovery::Retry` with by-value inputs** (89836d1): `#[node]` by-value inputs go
      through `NodeIo::take_owned_with`, cloning a shared `Clone` input, so retries get it again.
- [x] **GPU fallback with `GpuBackend::Cpu`** (e9a1f05): the engine substitutes the `fallback`
      node when the backend is `Cpu`; it was stored as metadata and never read.
- [x] **Host-created Vulkan instances.** `daedalus_gpu::driver_lock()` and
      `daedalus_gpu::with_driver_lock(f)` expose the lock that serializes Daedalus's own driver
      setup, so a host creating wgpu/Vulkan instances can serialize with it. The lock is not
      reentrant; see "Host-created Vulkan instances" in `crates/gpu/README.md`.

#### Portability (tier 2)
Tier 1 (`no_std` + `alloc` core/transport/data/registry/planner, and the `embedded` preset on
`wasm32-unknown-unknown`) and tier 2 (`no_std` + `alloc` runtime and engine with the serial
executor, checked on `thumbv7em` and `thumbv6m`) are done; see "Portability" in `docs/development.md`.
- [x] **Lock backend for runtime/engine.** `daedalus_runtime::sync`: `parking_lot` with `std`,
      `spin` (`lock_api`) without; the engine has no direct `parking_lot` dependency.
- [x] **`threads` feature.** Worker pool, stream workers and blocking waits (`InboundWaiter::wait`,
      `drive_blocking`, `recv_payload_timeout`, ...) exist only with `threads`; `platform::THREADS`
      is gone. The bridge's `Condvar` stays (`std`) for those waits.
- [x] **Clock injection.** `daedalus_core::platform::Clock` on the executor
      (`with_clock`), `StreamGraph` and `EngineConfig::with_clock`.
- [x] **Non-blocking host bridge without `std`.** Push, poll (`try_pop*`, `drain*`) and await the
      `core::task` `InboundWaiter`; the `Condvar` exists only with `std`.
- [x] **Lineage clock.** Node pushes, bridge pushes and direct lanes stamp lineage with the
      engine/executor clock (`Payload::stamp`); bridges (`HostBridgeManager::set_clock`) timestamp
      events and age `MaxAge` payloads on it. `platform::set_clock` is gone.
- [x] **`alloc`-only runtime.** The runtime's and engine's `std` feature removes `std`
      (`hashbrown` maps, spin-locked port buffer pool, perf counters/env config `std`-only);
      `ci.sh nostd` checks both and `examples/nostd_smoke` for `thumbv7em` and runs the smoke
      graph natively without `std`.
- [x] **Targets without compare-and-swap** (`thumbv6m-none-eabi`, `riscv32imc`): tier-1 crates
      build there; atomics, `spin` and `Arc` via `portable-atomic(-util)` (`critical-section`),
      target-specific; `ci.sh nostd` checks `thumbv6m`.
- [x] **Runtime and engine without compare-and-swap.** `tracing` is an optional feature
      (implied by `std`) behind the crate-private `trace` macros; `Arc<dyn _>` coercions go
      through `arc_dyn!`; `lockfree-queues` falls back to locked queues; `ci.sh nostd` checks
      runtime, engine and `nostd_smoke` for `thumbv6m` (`riscv32imc` checked locally).
- [x] **wasm host glue.** `examples/wasm_bindgen_host` (`Clock::new` on
      `performance.now()`, `push`/`tick`/`take` from JS, Node-driven), a `wasm32-wasip1` check and
      WASI smoke run (`ci.sh wasm`).

#### MCU profile
`daedalus-mcu` + `daedalus-mcu-build` (host-planned graphs, no heap) in three modes per firmware:
compiled (3.1-3.6 KiB flash, 160 B RAM for `examples/mcu_blink`), compiled + tunable (6.0-6.9
KiB, 176 B) and loaded (11.3-12.4 KiB, 584 B); see `docs/mcu.md`.
- [x] Host plan compiler, device crate, `#[daedalus_mcu::node]`, blink example with firmware for
      `thumbv7em`/`thumbv6m`, `scripts/ci.sh mcu` size budgets in the `portability` job.
- [x] **Live parameters**: constants marked in `daedalus.mcu.params` become a `Tunable` parameter
      table (typed setters, range and type checks with the planner's constant rules, postcard
      `ParamUpdate` messages, a JSON manifest and `daedalus-mcu param`); `freeze_params` and
      unmarked graphs compile to the same code as before (byte-exact size check in `ci.sh mcu`).
- [x] **Runtime-loaded plans**: `daedalus_mcu_build::library` generates a node library,
      `compile_loaded`/`to_blob` compile plans for it (same lowering as compiled mode),
      `loaded::Interpreter` validates and swaps them in a fixed arena (feature `loaded`), with
      parameters; plan A/B swap, validation failures and per-mode budgets tested.
- [ ] **Flash the firmwares** on a real board (with a HAL: ADC input, GPIO output) and measure
      stack use and tick time in each mode.
- [ ] **Chunked blob receive helper** (feature-gated framing + CRC32 into a buffer or flash
      partition) if applications keep writing the same transport code.
- [ ] **Smaller tunable/loaded code**: the generic `Scalar` conversion pulls in `f64` soft-float
      even for `f32`-only graphs; a per-kind conversion could drop it.
- [ ] **Fan-in and user adapters** on the device (generate adapter calls for `#[adapt]` functions
      that are plain `no_std` code).
- [ ] **Shared nodes with the full runtime:** generate both a `#[node]` handler and the device
      glue from one function, so a node crate serves both.

#### Low priority
- [ ] Windows checkouts need `core.symlinks` for the shared `crates/build_features.rs` symlinks.
      Decide whether Windows matters.
- [ ] Bench noise on shared CI runners may produce occasional false 15% flags. Consider a median of
      several runs.
