# Daedalus

Daedalus is a Rust workspace for building typed dataflow graphs that can run on CPU or GPU.

The repository is split into focused crates so applications, plugins, GPU backends, FFI layers, and test helpers can evolve independently.

## Workspace Layout

- `crates/daedalus`: consumer-facing facade and public API re-exports
- `crates/core`: shared identifiers, metrics, and low-level utility types
- `crates/data`: type/value model and serialization helpers
- `crates/registry`: node descriptor store and plugin loading support
- `crates/planner`: graph validation, scheduling, and runtime-plan generation
- `crates/runtime`: executor, host bridge, backpressure, and telemetry
- `crates/gpu`: optional GPU backends and WGSL dispatch helpers
- `crates/macros`: proc macros for nodes and GPU binding helpers
- `crates/engine`: higher-level facade wiring registry, planner, and runtime
- `crates/ffi`: language bindings and plugin interoperability
- `crates/nodes`: demo nodes and fixtures used by examples and tests
- `plugins/*`: optional plugin crates

Additional repository notes live under [docs/README.md](docs/README.md).

## Getting Started

Add the facade crate:

```toml
[dependencies]
daedalus = { package = "daedalus-rs", version = "1.0.0" }
```

Core feature sets:

- `engine`: enable the high-level engine facade and end-to-end examples
- `plugins`: enable plugin registry and plugin-oriented examples
- `gpu-wgpu`: enable the real GPU backend
- `gpu-mock`: enable the deterministic mock GPU backend for tests

Example:

```toml
[dependencies]
daedalus = { package = "daedalus-rs", version = "1.0.0", features = ["engine", "plugins"] }
```

## Examples

The facade crate includes CPU and GPU examples:

- `cargo run -p daedalus-rs --features "engine,plugins" --example cpu_image`
- `cargo run -p daedalus-rs --features "engine,plugins" --example cpu_text`
- `cargo run -p daedalus-rs --features "engine,plugins" --example cpu_branch`
- `cargo run -p daedalus-rs --features "engine,plugins" --example typed_any`
- `cargo run -p daedalus-rs --features "engine,plugins,gpu-wgpu" --example gpu_image`
- `cargo run -p daedalus-rs --features "engine,plugins,gpu-wgpu" --example gpu_shader_nodes`

## Development

Common workspace commands:

```bash
./scripts/repo-clean.sh
cargo fmt --all -- --check
./scripts/check-file-sizes.sh
cargo test --workspace --all-targets --features "engine,plugins"
cargo clippy --workspace --all-targets --features "engine,plugins" -- -D warnings
cargo doc --workspace --no-deps
```

Optional Docker-backed example validation:

- `cargo test -p daedalus-rs --test docker_examples -- --ignored --nocapture`

Targeted helper scripts and CI assets live under `scripts/` and `testing/`.

## Documentation Index

- [docs/README.md](docs/README.md): repository documentation index
- [docs/development.md](docs/development.md): repo layout, validation commands, and shared conventions
- [docs/testing.md](docs/testing.md): test surfaces, example expectations, and CI notes
- [CHANGELOG.md](CHANGELOG.md): release history and notable workspace changes
- [testing/README.md](testing/README.md): local and CI validation entry points
- [plugins/README.md](plugins/README.md): plugin crate conventions and usage
- [scripts/ci.sh](scripts/ci.sh): shared local CI entry point
- [scripts/repo-clean.sh](scripts/repo-clean.sh): pre-commit cleanup and verification entry point
- [crates/daedalus/README.md](crates/daedalus/README.md): facade API notes
- [crates/runtime/README.md](crates/runtime/README.md): executor and runtime behavior
- [crates/planner/README.md](crates/planner/README.md): planner model and graph validation
- [crates/gpu/README.md](crates/gpu/README.md): GPU backend and WGSL notes

## License

Licensed under either of:

- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT license (`LICENSE-MIT`)
