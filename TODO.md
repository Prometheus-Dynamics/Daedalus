# TODO

Status of the 2.x maturity pass (branch `helios-integration`, merged into `dev`). It was driven by
the Daedalus ↔ HeliOS integration review. See `CHANGELOG.md` `[Unreleased]` for the full list of
changes.

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
- [ ] **Stable plugin handler path.** Mismatched-toolchain (or Daedalus-version) plugins can be
      inspected but not run; separately built plugins still need the host's Daedalus build.
      Needs a `TypeKey`-keyed codec table registered by the node macros, an `invoke` entry point
      using ffi-core `InvokeRequest`/`InvokeResponse`, and schema-built host handlers (design in
      `docs/dynamic-plugins.md`).
- [ ] **Validate on Raspberry Pi 5 / CM5 (v3dv).** Run `./scripts/ci.sh pi` on the device and
      paste the `gpu_probe` report (LINEAR NV12 modifiers, `DISJOINT`, `TEXTURE_FORMAT_NV12`,
      the fence export ioctl, dma-heaps); see "Validating on a Raspberry Pi 5" in
      `docs/testing.md`.
- [ ] **First GitHub Actions run** of the new jobs (aarch64, lean-preset, macro-ui, dylib-plugins)
      and of `bench.yml`, including the `gh run download` baseline lookup and YAML anchors.
- [ ] **Tag `v2.0.0`.** There are no tags yet; downstream projects pin a commit hash.
- [ ] **HeliOS migration** (in the HeliOS repo): bump the pin, drop the `ffi`/`gpu` features, switch
      the loader to `PluginLibrary`, use styx-core's `daedalus` feature for `FrameLease` (or write
      the frame glue following `examples/04_async/external_frame_source.rs` until it exists),
      install the styx plugin in the host before loading plugins, build host and plugins in one
      `cargo build`, replace the 250 ms tick with `drive_blocking`, and rewrite `AGENTS.md`
      against `docs/node-authoring.md`.

### Medium priority
- [ ] **Public API review.** About 130 public functions have no in-repo callers (e.g.
      `stream::feed_typed`, several `gpu` helpers). Keep, document, or remove them.
- [ ] **dmabuf: GPU-side fence wait.** The acquire fence is waited on the CPU because wgpu-hal 29
      cannot add external wait semaphores. Revisit when wgpu exposes it; also queue-family-foreign
      acquire for compressed modifiers.
- [ ] **`export_plugin!` boundary contracts** are registered at install time but are not in the
      exported schema.
- [ ] **Generic image nodes** (color convert, resize, blur, threshold, HSV range, morphology, CLAHE),
      frame-native, rebuilt from the old HeliOS `lib-cv` shaders. On hold by decision.

### Portability (tier 2)
Tier 1 (`no_std` + `alloc` core/transport/data/registry/planner, and the `embedded` preset on
`wasm32-unknown-unknown`) is done; see "Portability" in `docs/development.md`. Next: a `no_std`
serial executor.
- [ ] **Lock backend for runtime/engine.** Replace direct `parking_lot` use with a
      `lock_api`-based alias (`parking_lot` with `std`, `spin`/`critical-section` without).
- [ ] **Clock injection.** Carry a `Clock` in `EngineConfig`/the executor instead of the global
      `daedalus_core::platform::set_clock`.
- [ ] **Non-blocking host bridge.** Make the bridge's `Condvar` waits `std`-only; `no_std` hosts
      push, poll and await `InboundWaiter`.
- [ ] **`threads` feature.** Gate the worker pool, stream workers and blocking waits at compile
      time instead of the runtime `platform::THREADS` checks.
- [ ] **`alloc`-only runtime.** `serde_json` const decoding, `tracing` and telemetry without
      `std`; `libc` only on Linux; then a `daedalus-runtime` `std` feature and a `thumbv7em`
      check of the serial executor.
- [ ] **Targets without compare-and-swap** (`thumbv6m-none-eabi`): `spin` with
      `portable-atomic`, or a `critical-section` lock.
- [ ] **wasm host glue.** A `wasm-bindgen` example wiring `set_clock` to `performance.now()` and
      driving a graph from JS; a `wasm32-wasip1` check.

### Low priority
- [ ] Windows checkouts need `core.symlinks` for the shared `crates/build_features.rs` symlinks.
      Decide whether Windows matters.
- [ ] Bench noise on shared CI runners may produce occasional false 15% flags. Consider a median of
      several runs.
- [ ] `daedalus-rs` has a dev-dependency on the unpublished `daedalus-plugins-example-project`;
      strip it before publishing the facade.
