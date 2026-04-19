# Development

Daedalus follows the shared Prometheus Dynamics workspace layout:

- `crates/`: core library crates and facades
- `plugins/`: plugin-style node bundles and fixtures
- `docs/`: repository-level guidance
- `scripts/`: local validation helpers
- `testing/`: validation notes and CI-facing test surfaces
- `.github/workflows/`: GitHub Actions pipelines

## Validation Surface

Use these commands for the default local validation loop:

```bash
./scripts/repo-clean.sh
cargo fmt --all -- --check
./scripts/check-file-sizes.sh
cargo test --workspace --all-targets --features "engine,plugins"
cargo clippy --workspace --all-targets --features "engine,plugins" -- -D warnings
cargo doc --workspace --no-deps
```

Add `gpu-wgpu` when validating real GPU paths on compatible hardware.
Use `FILE_SIZE_EXCLUDE_DIRS=path1:path2` when you need to mute file-size warnings for specific subtrees.
See [`testing.md`](testing.md) for the facade example and Docker validation surfaces.

## Tooling

- Rust toolchain is pinned in [`rust-toolchain.toml`](../rust-toolchain.toml)
- Root dependency versions are aligned in [`Cargo.toml`](../Cargo.toml)
- Local validation entrypoint lives in [`scripts/ci.sh`](../scripts/ci.sh)
- Local cleanup entrypoint lives in [`scripts/repo-clean.sh`](../scripts/repo-clean.sh)
- CI entrypoints live in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)

## Dependency Policy

- Library-facing error types use `thiserror`.
- Shared runtime instrumentation uses `tracing`.
- Backend variants stay behind stable feature names such as `gpu-mock` and `gpu-wgpu`.
- `crates/engine` keeps a small set of explicit path dependencies because Cargo workspace inheritance cannot currently express those dependencies together with `default-features = false`.
