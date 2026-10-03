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
- [x] Decision: Daedalus and Styx stay independent. The frame glue lives in the application.

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
- [ ] **Stable plugin handler path.** Mismatched-toolchain plugins can be inspected but not run.
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
      the loader to `PluginLibrary`, write the frame glue module following
      `examples/04_async/external_frame_source.rs`, replace the 250 ms tick with `drive_blocking`,
      and rewrite `AGENTS.md` against `docs/node-authoring.md`.

### Medium priority
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
