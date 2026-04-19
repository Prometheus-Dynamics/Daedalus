# Testing

Daedalus splits validation into default workspace checks, facade-example coverage, and Docker-backed example validation.

## Default Surface

- `cargo fmt --all -- --check`
- `./scripts/check-file-sizes.sh`
- `cargo test --workspace --all-targets --features "engine,plugins"`
- `cargo clippy --workspace --all-targets --features "engine,plugins" -- -D warnings`
- `cargo doc --workspace --no-deps`

## Facade Example Surface

- `cargo check -p daedalus-rs --features "engine,plugins" --examples`
- `cargo test -p daedalus-rs --features "engine,plugins" --examples`

This keeps the consumer-facing facade examples healthy without relying on the FFI/plugin helper scripts that were removed.

## Docker Surface

- `cargo test -p daedalus-rs --test docker_examples -- --ignored --nocapture`

The Docker suite uses [`testing/docker/daedalus-examples.Dockerfile`](../testing/docker/daedalus-examples.Dockerfile) and exercises real facade examples inside the container image.

## Additional Coverage

- UI and macro diagnostics live under `crates/nodes/tests/ui`
- Planner and runtime integration tests live under `crates/planner/tests` and `crates/runtime/tests`
- GPU validation should be added explicitly with `--features "engine,plugins,gpu-wgpu"` on compatible systems
- File-size linting is warning-only, supports `FILE_SIZE_EXCLUDE_DIRS=path1:path2`, and tracks current exceptions through `testing/ci/file-size-baseline.txt`
