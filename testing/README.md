# Testing

Short reference for local validation. The fuller guide is [`../docs/testing.md`](../docs/testing.md).

## Default

```bash
cargo fmt --all -- --check
./scripts/check-file-sizes.sh
./scripts/check-workspace-deps.sh
./scripts/check-gpu-async-blocking.sh
cargo test --workspace --all-targets --features "engine,plugins"
cargo clippy --workspace --all-targets --features "engine,plugins" -- -D warnings
```

## Focused

```bash
cargo test -p daedalus-runtime --features "plugins"
cargo test -p daedalus-ffi-core
cargo test -p daedalus-ffi-host
cargo test -p daedalus-rs --features "engine,plugins" --examples
```

## Docker

```bash
cargo test -p daedalus-rs --test docker_examples -- --ignored --nocapture
```

The Docker suite uses [`docker/daedalus-examples.Dockerfile`](docker/daedalus-examples.Dockerfile).

## Notes

- Use `gpu-mock` for deterministic GPU-path coverage.
- Use `gpu-wgpu` only on hardware-backed hosts.
- File-size linting is warning-only and reads `ci/file-size-baseline.txt`.
