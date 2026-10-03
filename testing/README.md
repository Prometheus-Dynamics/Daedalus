# Testing

Short reference for local validation. The fuller guide is [`../docs/testing.md`](../docs/testing.md).

## Default

See [the default surface](../docs/testing.md#default-surface); `./scripts/ci.sh` runs it.

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
- File-size linting fails on new oversized Rust files; existing ones are listed in `ci/file-size-baseline.txt`.
