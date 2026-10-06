# TODO

Status of the maturity pass after 2.0.0 (branch `helios-integration`, merged into `dev`), released
as 3.0.0. It was driven by the Daedalus ↔ HeliOS integration review. See `CHANGELOG.md` `[3.0.0]`
for the full list of changes and `docs/migration-3.0.md` for upgrading.

## Done

### Integration with HeliOS (and other hosts)
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
      `external_frame_source` example (camera glue pattern with no Styx dependency).
- [x] Decision: Daedalus never depends on Styx. The crate that defines a type owns its key and
      Daedalus registration behind an optional `daedalus` feature (documented in
      `docs/node-authoring.md`, "Library-Owned Integration Features"); without one, the frame
      glue lives in the application.
- [x] **Deterministic port keys for foreign types.** Macros resolve a type's own key at compile
      time; ports take `type_key = "..."`, plugins `foreign_types(Type = "key")`; an unkeyed
      foreign type fails install (`UnkeyedForeignType`) instead of getting an order-dependent
      `rust:` key.
- [x] **Boundary type check for dylib plugins.** HeliOS found that a plugin built in a separate
      cargo invocation passed the fingerprint check but failed on every frame with
      `payload type mismatch: expected styx:framelease, found styx:framelease`. Now the
      descriptor (ABI 6) exports `(TypeKey, TypeId hash, size, align, type_name)` per boundary
      type, `install_into` fails with `BoundaryTypeConflict` naming every differing key before
      installing anything, adapter errors say "same TypeKey, different Rust type", and the
      same-cargo-build rule is documented in `docs/dynamic-plugins.md`.
- [x] **Foreign (host-owned) types for separately built plugins.** Foreign interfaces
      (`foreign_interface!`: `#[repr(C)]` accessor vtable, key, version, layout hash), owner
      providers registered as zero-copy `View` adapters, `ForeignHandle` payloads with an
      `Arc`-backed keepalive, and node inputs `FrameView<'_>` / `ForeignRef<'_, I>`. The standard
      `daedalus:frame` v1 interface (`docs/foreign-frame-interface.md`) lets owners such as
      styx-core implement `FrameSource` in their `daedalus` feature. The descriptor (ABI 7)
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

- [x] **Optional inputs (Eidos).** `Option<T>` inputs carry `T`'s key (producers of `T` connect
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
      `dylib_stable` test cover it.
- [x] **`export_plugin!` boundary contracts in the schema.** `PluginSchema::boundary_contracts`
      lists them next to the plugin's own.
- [x] **Graph JSON Schema drift.** Enum lists come from each core type's `ALL` (guarded by an
      exhaustive match), and a test validates every variant against the generated schema.
- [x] **Enum config ports (Eidos).** `NodeConfig` enum fields and enum handler inputs failed
      with `missing <port>`: the engine never handed the registry's const coercers to its
      executors and owned/borrowed inputs never coerced `Value` constants. The node macros now
      register a coercer per input and config field type at install (`DaedalusTypeExpr` unit
      enums by name or index, else serde); see "Constants, Defaults And Config Enums" in
      `docs/node-authoring.md`.
- [x] **Ids from other macros (Eidos).** `#[node]`/`#[adapt]`/`#[plugin]` accept any `&'static
      str` constant expression as `id` (`concat!`, a user macro, a `const`).
- [x] **Macro leaf keys without an owned key.** Port types without a `#[type_key]` (a
      `foreign_types` mapping) resolve through the installing registry's `TypeRegistry`
      (`node_decl_in`, `handler_registry_in`, ...), handlers compute output keys once per
      registry, and the process-global typing registry is gone. Dynamic plugins resolve a
      dependency's mapping through the registry they install into: the introspection registry
      (where `export_plugin!(.., deps [..])` installs linked dependencies first) or the host's.
- [x] **Dynamic plugin dependencies (Eidos/Styx).** Schemas list a plugin's dependencies and
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
      were `missing`; it now delivers them like a scheduled tick (`tests/direct_const_inputs.rs`).
- [x] **Typed nodes with three reference parameters.** `fn(&A, &B, &mut State)` was taken for
      the low-level `(node, ctx, io)` form; the form is now recognized by parameter types.

### Performance
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

### Code health
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

## Remaining

### High priority
- [ ] **Validate on Raspberry Pi 5 / CM5 (v3dv).** Run `./scripts/ci.sh pi` on the device and
      paste the `gpu_probe` report (fence path, per-format modifiers, LINEAR NV12, `DISJOINT`,
      `TEXTURE_FORMAT_NV12`, the fence export ioctl, dma-heaps) and the `--nocapture`
      measurements of the fence tests; see "Validating on a Raspberry Pi 5" in
      `docs/testing.md`.
- [ ] **Frame-path overhead on the CM5 (requires the board).** Run
      `cargo run --release -p daedalus-frame-bench --example frame_chain` (and
      `cargo bench -p daedalus-frame-bench --bench frame_chain`) on the CM5, record the fixed and
      per-stage cost, the overhead table and whether frames came from `/dev/dma_heap`, and add
      them next to the host x86_64 numbers in `docs/development.md` ("Frame-path overhead");
      then run Eidos's `eidos:detectors.aruco` through `run_frame_bench` on the same board.
- [ ] **First GitHub Actions run** of the new jobs (aarch64, lean-preset, macro-ui, dylib-plugins)
      and of `bench.yml`, including the `gh run download` baseline lookup and YAML anchors.
- [ ] **Tags** (coordinator): `v2.0.0` at `8946223` (the April release) and `v3.0.0` at the 3.0.0
      release commit on `dev`. Downstream (HeliOS, Styx, Eidos) then pins `tag = "v3.0.0"`; Styx
      `dev` and Eidos `main` still lock `75dc4c1`, and Styx's `daedalus` feature asks for
      `daedalus-rs` 2.0.0, so it must move to 3.0.0 before Daedalus's examples can use it.
- [ ] **HeliOS migration** (HeliOS-owned, in the HeliOS repo; see `docs/migration-3.0.md`): pin
      `v3.0.0`, drop the `ffi`/`gpu` features, switch the loader to `PluginLibrary`, use Styx's
      `daedalus` feature (`StyxFramesPlugin`, once it requires Daedalus 3.0.0) for `FrameLease`
      (or write the frame glue following `examples/04_async/external_frame_source.rs`), install
      the Styx plugin in the host before loading plugins, build host and plugins in one
      `cargo build`, replace the 250 ms tick with `drive_blocking`, and rewrite `AGENTS.md`
      against `docs/node-authoring.md`.

### Medium priority
- [ ] **Public API review.** About 130 public functions have no in-repo callers (e.g.
      `stream::feed_typed`, several `gpu` helpers). Keep, document, or remove them.
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
- [ ] **dmabuf: validation layers on the Pi.** Run `./scripts/ci.sh vvl` on v3dv alongside the
      `pi` report (only RADV has been validated).
- [ ] **Host-created Vulkan instances.** The driver lock only covers instances `daedalus-gpu`
      creates; a host that creates wgpu/Vulkan instances on other threads at the same time can
      still hit the loader race. Expose the lock (or document a creation order) if a host needs
      it.
- [ ] **Generic image nodes** (color convert, resize, blur, threshold, HSV range, morphology, CLAHE),
      frame-native, rebuilt from the old HeliOS `lib-cv` shaders. On hold by decision.

### Portability (tier 2)
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

### MCU profile
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

### Low priority
- [ ] Windows checkouts need `core.symlinks` for the shared `crates/build_features.rs` symlinks.
      Decide whether Windows matters.
- [ ] Bench noise on shared CI runners may produce occasional false 15% flags. Consider a median of
      several runs.
- [ ] `daedalus-rs` has a dev-dependency on the unpublished `daedalus-plugins-example-project`;
      strip it before publishing the facade.
