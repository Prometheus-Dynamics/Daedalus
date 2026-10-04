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

### Performance
- [x] Host bridge: per-port state, single-slot latest-only queues, events off by default.
- [x] Executor: direct bridge handles, allocation-free ticks (31 → 4 allocations per round trip,
      enforced by `crates/engine/tests/hot_path_allocations.rs`), `IdStr` for static ids, cheaper
      `Payload`.
- [x] Shared `PolicyQueue<T>` for host ports and executor edges.
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
- [x] CI: aarch64 check, lean-preset tests, macro UI and dylib jobs, example smoke runs, and
      `scripts/ci.sh` subcommands.
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
- [ ] **Validate on Raspberry Pi 5 / CM5 (v3dv).** Run
      `cargo test -p daedalus-gpu --features gpu-dmabuf -- --ignored dmabuf`. Check LINEAR NV12
      modifiers, `DISJOINT`, `TEXTURE_FORMAT_NV12`, and the fence export ioctl (kernel 6.0+).
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
- [ ] **Cross-tick joins.** A node skipped for a missing required input drops what arrived on
      its other ports that tick. Graphs that need "wait until every input arrived" joins across
      ticks would need readiness checked before popping edges.
- [ ] **Fanning a builtin host input out to several nodes.** A host input of `i64` feeding two
      `i64` node inputs failed in one of them with `payload type mismatch: same TypeKey
      typeexpr:{"Scalar":"Int"}, different Rust type (expected i32, found i64)`: the fan-out
      branch adapter for the shared `Int` key appears to be the one registered for `i32`. Seen
      while testing enum config ports; `enum_config_ports` avoids it.
- [ ] **Global boundary contract registry.** Macro installs register boundary contracts for
      their port types (builtins included) in `daedalus_transport`'s process-global registry,
      so `Payload::owned` of e.g. `i64` takes the boundary storage path (contract clone,
      `LayoutHash::for_type` strings): about 8 allocations per payload instead of 2. Scope the
      contracts to the registry, or skip them for builtins.
- [ ] **Public API review.** About 130 public functions have no in-repo callers (e.g.
      `stream::feed_typed`, several `gpu` helpers). Keep, document, or remove them.
- [ ] **dmabuf: GPU-side fence wait.** The acquire fence is waited on the CPU because wgpu-hal 29
      cannot add external wait semaphores. Revisit when wgpu exposes it; also queue-family-foreign
      acquire for compressed modifiers.
- [ ] **Typed host ports in embedded graphs.** Declared host port types are not carried through
      embedded-graph expansion.
- [ ] **`export_plugin!` boundary contracts** are registered at install time but are not in the
      exported schema.
- [ ] **Graph JSON Schema** hand-copies `SyncGroup` variants from `daedalus-core`; derive them or add
      a variant-drift test.
- [ ] **Generic image nodes** (color convert, resize, blur, threshold, HSV range, morphology, CLAHE),
      frame-native, rebuilt from the old HeliOS `lib-cv` shaders. On hold by decision.

### Low priority
- [ ] Windows checkouts need `core.symlinks` for the shared `crates/build_features.rs` symlinks.
      Decide whether Windows matters.
- [ ] `daedalus-transport` keeps its own FNV-1a copy (it has no dependency on core).
- [ ] Bench noise on shared CI runners may produce occasional false 15% flags. Consider a median of
      several runs.
- [ ] `daedalus-rs` has a dev-dependency on the unpublished `daedalus-plugins-example-project`;
      strip it before publishing the facade.
