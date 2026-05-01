# Testing

Daedalus validation is split into a default workspace loop, focused feature checks, FFI fixture checks, GPU checks, and Docker-backed example validation.

## Default Surface

```bash
cargo fmt --all -- --check
./scripts/check-file-sizes.sh
./scripts/check-workspace-deps.sh
./scripts/check-gpu-async-blocking.sh
cargo check --workspace --all-targets
cargo test --workspace --all-targets --features "engine,plugins"
cargo clippy --workspace --all-targets --features "engine,plugins" -- -D warnings
cargo doc --workspace --no-deps
```

## Release Feature Surface

```bash
cargo check -p daedalus-rs --no-default-features
cargo check -p daedalus-rs --all-targets --features "engine,plugins"
cargo check -p daedalus-rs --all-targets --features "gpu-mock,plugins,engine"
cargo check -p daedalus-runtime --features "metrics,executor-pool,lockfree-queues"
cargo check -p daedalus-ffi-core --no-default-features
cargo check -p daedalus-ffi-core --features "image-payload"
cargo check -p daedalus-ffi-host --no-default-features
cargo check -p daedalus-ffi-host --features "image-payload"
cargo check -p daedalus-gpu --no-default-features --features gpu-wgpu
cargo check -p daedalus-gpu --no-default-features --features gpu-wgpu,gpu-async
```

Use `gpu-wgpu` only where hardware and drivers are available. Use `gpu-mock` for CI-stable GPU-path coverage.

## Examples

```bash
cargo check -p daedalus-examples --features "engine,plugins"
cargo test -p daedalus-rs --features "engine,plugins" --examples
cargo run -p daedalus-examples --quiet --bin runtime_metrics
cargo run -p daedalus-examples --quiet --bin transport_metrics
cargo run -p daedalus-examples --quiet --bin ownership_metrics
cargo run -p daedalus-examples --quiet --bin lifecycle_trace
cargo run -p daedalus-examples --quiet --bin plan_debug
cargo run -p daedalus-examples --quiet --bin overhead_floor
```

## FFI

FFI tests should cover both contract-level validation and host execution behavior:

- generated canonical fixture snapshots,
- package descriptor validation and integrity stamping,
- persistent worker handshake and repeated invocation,
- state synchronization,
- payload lease ownership and release accounting,
- normalized response decoding across Python, Node, Java, C/C++, and Rust fixture paths.

Run package-specific tests while the FFI rewrite is active:

```bash
cargo test -p daedalus-ffi-core
cargo test -p daedalus-ffi-host
cargo test -p daedalus-ffi-python
cargo test -p daedalus-ffi-node
cargo test -p daedalus-ffi-java
cargo test -p daedalus-ffi-cpp
```

## Docker

```bash
cargo test -p daedalus-rs --test docker_examples -- --ignored --nocapture
```

The Docker suite uses [`testing/docker/daedalus-examples.Dockerfile`](../testing/docker/daedalus-examples.Dockerfile) and validates real facade examples in a controlled image.

## Other Coverage

- UI and macro diagnostics live under `crates/nodes/tests/ui` and `crates/daedalus/tests/ui`.
- Planner/runtime integration tests live under `crates/planner/tests` and `crates/runtime/tests`.
- Runtime plan goldens live under `crates/runtime/tests/goldens`.
- File-size linting is warning-only and uses `testing/ci/file-size-baseline.txt`.
