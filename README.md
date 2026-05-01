# Daedalus

Daedalus is a Rust workspace for typed dataflow graphs. The current codebase is organized around a layered pipeline:

1. describe node and payload types,
2. register capabilities and plugin-provided nodes,
3. plan a graph into a deterministic runtime plan,
4. execute that plan on CPU and, when enabled, GPU backends,
5. expose host and FFI boundaries for applications and language SDKs.

The facade crate is published as `daedalus-rs` and imported as `daedalus`.

## Workspace

- `crates/daedalus`: facade crate and public re-exports.
- `crates/core`: ids, clocks, channel traits, sync policy, backpressure names, errors, and metrics hooks.
- `crates/transport`: stable payload identity, access, residency, adapter, and lifecycle primitives.
- `crates/data`: portable value/type model, descriptors, conversion, schema/proto/json helpers, and type metadata.
- `crates/registry`: capability, type, adapter, serializer, device, and node declaration registry.
- `crates/planner`: graph model, validation, adapter resolution, scheduling, and runtime-plan inputs.
- `crates/runtime`: executor, host bridge, stream workers, runtime plans, telemetry, state, and resources.
- `crates/engine`: high-level registry to planner to runtime facade for application hosts.
- `crates/gpu`: GPU handle types, backend selection, mock/noop/wgpu backends, and shader dispatch helpers.
- `crates/macros`: node, config, transport, type-key, value, and GPU derive macros.
- `crates/ffi`: shared FFI contract, host runner, and Python/Node/Java/C++ SDK targets.
- `examples`: runnable graph, runtime, GPU, metrics, and FFI examples.
- `testing`: local and CI validation notes.

## Features

The facade starts with no default feature set. Enable only the layers your application needs.

- `engine`: high-level engine facade.
- `plugins`: plugin registry and macro-generated plugin installation.
- `gpu-types`: GPU handles and type surface only.
- `gpu-runtime`: GPU-aware registry, planner, and runtime wiring.
- `gpu-engine`: GPU-aware engine wiring.
- `gpu-wgpu`: real `wgpu` backend.
- `gpu-async`: async shader dispatch/readback helpers for `gpu-wgpu`.
- `gpu-mock`: deterministic mock GPU backend for tests.
- `schema` and `proto`: optional data/planner export surfaces.

## Getting Started

```toml
[dependencies]
daedalus = { package = "daedalus-rs", version = "2.0.0", features = ["engine", "plugins"] }
```

Useful examples:

```bash
cargo run -p daedalus-examples --bin quickstart_typed_cpu_graph
cargo run -p daedalus-examples --bin quickstart_bounded_streaming_io
cargo run -p daedalus-examples --bin typed_handle_graph
cargo run -p daedalus-examples --bin adapter_path
cargo run -p daedalus-examples --bin observability
cargo run -p daedalus-examples --features gpu-wgpu --bin gpu_node
```

## Validation

```bash
./scripts/repo-clean.sh
cargo fmt --all -- --check
./scripts/check-file-sizes.sh
./scripts/check-workspace-deps.sh
./scripts/check-gpu-async-blocking.sh
cargo test --workspace --all-targets --features "engine,plugins"
cargo clippy --workspace --all-targets --features "engine,plugins" -- -D warnings
cargo doc --workspace --no-deps
```

Docker-backed example validation:

```bash
cargo test -p daedalus-rs --test docker_examples -- --ignored --nocapture
```

## Documentation

- [docs/README.md](docs/README.md): documentation map.
- [docs/development.md](docs/development.md): development rules, features, observability, and production API guidance.
- [docs/testing.md](docs/testing.md): supported validation surface.
- [crates/ffi/README.md](crates/ffi/README.md): FFI contract, package, worker, and SDK direction.
- [testing/README.md](testing/README.md): quick local testing reference.

## License

Licensed under either `LICENSE-APACHE` or `LICENSE-MIT`.
