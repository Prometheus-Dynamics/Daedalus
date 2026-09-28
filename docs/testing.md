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

Slower suites run as separate CI jobs so they overlap with the workspace tests:

```bash
# trybuild macro UI tests (ignored in the default test run)
cargo test -p daedalus-rs --features plugins --test transport_macro_ui -- --ignored
# native Rust cdylib plugin loading
cargo test -p daedalus-rs --features "engine,plugins,dylib-plugins"
```

## Local CI Runner

`scripts/ci.sh` runs the same commands as CI. With no arguments it runs the full default loop
(`all`); otherwise pass one or more subcommands, e.g. `scripts/ci.sh lints test`. Run
`scripts/ci.sh help` for the list. CI jobs call these subcommands, so edit commands there.

| Subcommand | What it runs |
|---|---|
| `lints` | file-size, workspace-deps, GPU async lints, `cargo fmt --check` |
| `check`, `features` | workspace check; release feature-surface checks (incl. `--all-features`, needs `libcamera-dev`) |
| `clippy`, `test`, `macro-ui`, `examples` | clippy `-D warnings`; workspace + dylib tests; trybuild; facade examples |
| `smoke` | the CPU-only example binaries (`runtime_metrics` ... `external_frame_source`) |
| `aarch64` | `cargo check --target aarch64-unknown-linux-gnu` (see below) |
| `lean` | lean-preset tests (see below) |
| `bench` | host bridge and executor criterion benches (see below) |

### aarch64

```bash
scripts/ci.sh aarch64
```

Type-checks (no linking) the workspace with the default CI features (`engine,plugins`), the
facade `embedded` preset, and `daedalus-gpu` with `gpu-dmabuf` for `aarch64-unknown-linux-gnu`.
The script adds the rustup target if missing. Build scripts that compile C (criterion's
`alloca`, pulled in by `--all-targets`) need an aarch64 C compiler with libc headers: on
Debian/Ubuntu install `gcc-aarch64-linux-gnu` (CI does). On hosts whose cross gcc has no aarch64
glibc sysroot (e.g. Fedora's `gcc-aarch64-linux-gnu`), set
`CFLAGS_aarch64_unknown_linux_gnu=-ffreestanding`. The `styx-camera-example` feature is not
checked: it needs target `libcamera` via pkg-config.

### Lean preset

```bash
scripts/ci.sh lean
# = cargo test -p daedalus-rs -p daedalus-engine -p daedalus-runtime --all-targets --no-default-features \
#     --features "daedalus-rs/embedded,daedalus-engine/config-env,daedalus-engine/plugins,daedalus-runtime/plugins"
```

`cargo test --workspace` unifies features across members, and `daedalus-daemon` enables the
executor pool and metrics, so workspace tests never exercise the serial/scoped-thread executor
or no-op telemetry. The lean run selects only the facade, engine, and runtime crates (never the
daemon) with default features off. Verify with `cargo tree -i rayon` using the same flags: rayon
should only appear under criterion.

### Benchmarks

Benches do not run on pull requests. The `bench` workflow (`.github/workflows/bench.yml`) runs
on `workflow_dispatch`, weekly, and on pushes to `main` that touch runtime/engine/transport. It
runs `scripts/ci.sh bench`, uploads `target/criterion` as the `criterion` artifact (90 days),
downloads the same artifact from the newest earlier completed run on the default branch, and
runs:

```bash
python3 scripts/bench-compare.py BASELINE_DIR CURRENT_DIR [--threshold 15] [--stat median|mean]
```

The script reads every `new/estimates.json` under both criterion roots, prints a table (and
appends it to the job summary), emits a warning annotation per benchmark whose median grew by
more than the threshold, and exits 1 if any did. With no baseline it just reports. Locally:

```bash
cp -r target/criterion /tmp/criterion-before   # after a run on the base commit
scripts/ci.sh bench
python3 scripts/bench-compare.py /tmp/criterion-before target/criterion
```

Shared CI runners are noisy; treat a single flagged run as a prompt to re-run, and a flag that
repeats as a real regression. Since the baseline is always the previous run, an accepted
regression only flags once.

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
cargo run -p daedalus-examples --quiet --bin external_frame_source
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
- File-size linting warns about Rust files over 800 lines that are not listed in
  `testing/ci/file-size-baseline.txt`, and fails when a baseline entry is stale (the file is
  missing or back within the limit), so the baseline only ever shrinks.
