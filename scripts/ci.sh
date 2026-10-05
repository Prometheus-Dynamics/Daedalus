#!/usr/bin/env bash
# Local equivalent of the CI workflows. Run with no arguments for the full default loop, or
# pass one or more subcommands (see `usage`). CI jobs call the same subcommands, so the
# commands only live here.
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

# Default CI feature set for workspace-wide builds.
readonly CI_FEATURES="engine,plugins"
readonly AARCH64_TARGET="aarch64-unknown-linux-gnu"
readonly AARCH64_MUSL_TARGET="aarch64-unknown-linux-musl"
# Bare metal without `std`: Cortex-M4F (32-bit atomics, no 64-bit ones) and Cortex-M0 (no
# compare-and-swap: atomics, locks and `Arc` go through `portable-atomic`).
readonly NOSTD_TARGETS=("thumbv7em-none-eabihf" "thumbv6m-none-eabi")
readonly NOSTD_CRATES=(-p daedalus-core -p daedalus-transport -p daedalus-data -p daedalus-registry
  -p daedalus-planner)
readonly WASM_TARGET="wasm32-unknown-unknown"
readonly WASI_TARGET="wasm32-wasip1"

step() { echo "==> $*"; }

ensure_target() {
  rustup target list --installed | grep -qx "$1" || rustup target add "$1"
}

usage() {
  cat <<'EOF'
usage: scripts/ci.sh [subcommand...]

  all         lints check features clippy test macro-ui examples smoke (default)
  lints       file-size, workspace-deps, GPU async lints and rustfmt
  check       cargo check of the workspace
  features    release feature-surface checks
  clippy      clippy with -D warnings
  test        workspace tests (default CI features) and dylib plugin tests
  macro-ui    trybuild macro UI tests
  examples    build facade examples and run their tests
  smoke       run the CPU-only example binaries
  aarch64     cargo check for aarch64 gnu (default, embedded, gpu-dmabuf) and musl (libraries)
  lean        tests for the lean preset (no executor pool, no metrics) and without threads
  nostd       no_std + alloc checks for thumbv7em and thumbv6m (no CAS): tier-1 crates, the
              serial runtime and engine, and the no_std smoke graph
  wasm        engine,plugins (embedded without threads) checks and Node runs for wasm32 and WASI
  bench       host bridge, runtime executor and graph frame criterion benches
  pi          on-device dmabuf hardware tests and the gpu_probe report (Raspberry Pi 5 / CM5)
EOF
}

cmd_lints() {
  step "Checking file sizes"
  ./scripts/check-file-sizes.sh
  step "Checking workspace dependency centralization"
  ./scripts/check-workspace-deps.sh
  step "Checking async GPU paths"
  ./scripts/check-gpu-async-blocking.sh
  step "Checking formatting"
  cargo fmt --all -- --check
}

cmd_check() {
  step "Checking workspace"
  cargo check --workspace --all-targets
}

cmd_features() {
  step "Checking feature surfaces"
  cargo check -p daedalus-rs --no-default-features
  cargo check -p daedalus-rs --features "engine,plugins,gpu-mock"
  cargo check -p daedalus-rs --no-default-features --features "embedded"
  cargo check -p daedalus-rs --no-default-features --features "engine,plugins"
  cargo check -p daedalus-rs --all-targets --features "engine-full,plugins"
  cargo check -p daedalus-rs --features "dylib-plugins"
  cargo check -p daedalus-runtime --features "metrics,executor-pool,lockfree-queues"
  cargo check -p daedalus-ffi-core -p daedalus-ffi-host --no-default-features
  cargo check -p daedalus-ffi-core -p daedalus-ffi-host \
    --features "daedalus-ffi-core/image-payload,daedalus-ffi-host/image-payload"
  cargo check -p daedalus-gpu --no-default-features --features "gpu-wgpu"
  # Needs libcamera-dev + pkg-config for the styx camera example feature.
  cargo check --workspace --all-targets --all-features
  # Links every library, so `cdylib` plugins whose exported symbols clash once features unify
  # fail here (`check` does not link).
  cargo build --workspace --lib --all-features
}

cmd_clippy() {
  step "Running clippy"
  cargo clippy --workspace --all-targets --features "$CI_FEATURES" -- -D warnings
  cargo clippy -p daedalus-rs --all-targets --features "$CI_FEATURES,dylib-plugins" -- -D warnings
}

cmd_test() {
  step "Running tests"
  cargo test --workspace --all-targets --features "$CI_FEATURES"
  cargo test -p daedalus-rs --features "$CI_FEATURES,dylib-plugins"
}

cmd_macro_ui() {
  step "Running macro UI (trybuild) tests"
  cargo test -p daedalus-rs --features plugins --test transport_macro_ui -- --ignored
}

cmd_examples() {
  step "Building examples"
  cargo check -p daedalus-rs --features "$CI_FEATURES" --examples
  cargo test -p daedalus-rs --features "$CI_FEATURES" --examples
}

cmd_smoke() {
  step "Running CPU-only example binaries"
  local bin
  for bin in runtime_metrics transport_metrics ownership_metrics lifecycle_trace plan_debug \
    overhead_floor observability backpressure_diagnostics external_frame_source; do
    echo "  -> $bin"
    cargo run -p daedalus-examples --quiet --bin "$bin" >/dev/null
  done
}

# Type-check only (no linking), so no cross linker or sysroot libraries are needed beyond a C
# cross compiler for build scripts that compile C (criterion's `alloca`). On Debian/Ubuntu that
# is `gcc-aarch64-linux-gnu`; see docs/testing.md for hosts without an aarch64 glibc sysroot.
# The styx camera feature is skipped: it needs target libcamera via pkg-config.
cmd_aarch64() {
  step "Checking $AARCH64_TARGET"
  ensure_target "$AARCH64_TARGET"
  local target=(--target "$AARCH64_TARGET")
  cargo check "${target[@]}" --workspace --all-targets --features "$CI_FEATURES"
  cargo check "${target[@]}" -p daedalus-rs --all-targets --no-default-features --features "embedded"
  cargo check "${target[@]}" -p daedalus-gpu --all-targets --features "gpu-dmabuf"
  # musl: libc signatures differ from glibc (e.g. `ioctl` takes a `c_int` request). Library
  # targets only, so no musl C toolchain is needed for test-only C build scripts.
  step "Checking $AARCH64_MUSL_TARGET"
  ensure_target "$AARCH64_MUSL_TARGET"
  cargo check --target "$AARCH64_MUSL_TARGET" --workspace --features "$CI_FEATURES"
  cargo check --target "$AARCH64_MUSL_TARGET" -p daedalus-runtime --all-features
  cargo check --target "$AARCH64_MUSL_TARGET" -p daedalus-gpu --features "gpu-dmabuf"
}

# Workspace tests unify features across all members, so the daemon crate turns on the executor
# pool and metrics for everyone. This run selects only the facade, engine, and runtime crates
# (never daedalus-daemon) with defaults off, so the parked-thread worker path and the no-op
# telemetry path are what get tested.
cmd_lean() {
  step "Testing lean preset (no executor-pool, no metrics)"
  cargo test -p daedalus-rs -p daedalus-engine -p daedalus-runtime --all-targets \
    --no-default-features \
    --features "daedalus-rs/embedded,daedalus-engine/config-env,daedalus-engine/plugins,daedalus-runtime/plugins"
  step "Testing without threads (serial-only runtime and engine)"
  cargo test -p daedalus-runtime -p daedalus-engine --all-targets --no-default-features \
    --features "daedalus-runtime/plugins,daedalus-engine/plugins,daedalus-engine/config-env"
}

# On each bare-metal target: the tier-1 crates without `std`, with and without their alloc-only
# optional features; then (tier 2) the serial runtime and engine, likewise, and the
# `examples/nostd_smoke` graph. `tracing` is checked only where it builds (it needs
# compare-and-swap). The smoke tests also run natively with `std` off everywhere.
cmd_nostd() {
  local target
  for target in "${NOSTD_TARGETS[@]}"; do
    step "Checking no_std + alloc crates for $target"
    ensure_target "$target"
    cargo check --target "$target" "${NOSTD_CRATES[@]}" --no-default-features
    cargo check --target "$target" "${NOSTD_CRATES[@]}" --no-default-features --features \
      "daedalus-core/metrics,daedalus-data/json,daedalus-data/schema,daedalus-data/proto,daedalus-data/async,daedalus-registry/bundle,daedalus-registry/plugin,daedalus-planner/schema,daedalus-planner/proto"
    step "Checking the no_std serial runtime and engine for $target"
    cargo check --target "$target" -p daedalus-runtime -p daedalus-engine --no-default-features
    cargo check --target "$target" -p daedalus-runtime -p daedalus-engine --no-default-features \
      --features "daedalus-runtime/plugins,daedalus-runtime/metrics,daedalus-runtime/snapshots,daedalus-runtime/lockfree-queues,daedalus-engine/plugins,daedalus-engine/config-env"
    cargo check --target "$target" -p daedalus-nostd-smoke
  done
  step "Checking the no_std runtime and engine with tracing for ${NOSTD_TARGETS[0]}"
  cargo check --target "${NOSTD_TARGETS[0]}" -p daedalus-runtime -p daedalus-engine \
    --no-default-features --features "daedalus-engine/tracing,daedalus-engine/plugins"
  step "Running the no_std smoke test natively"
  cargo test -p daedalus-nostd-smoke
}

# `wasm32-unknown-unknown` has `std` but no threads and no clock; `wasm32-wasip1` has a clock but
# no threads. Check the embedded preset without `threads` (`engine,plugins`) for both, then run in
# Node: serial/parallel/adaptive frames (an import-free module, and a WASI command on the platform
# clock) and the wasm-bindgen host example driven from JS. Runs are skipped without `node`; the
# wasm-bindgen one without a `wasm-bindgen` CLI matching Cargo.lock (required when `$CI` is set).
cmd_wasm() {
  local out="${CARGO_TARGET_DIR:-target}" target
  for target in "$WASM_TARGET" "$WASI_TARGET"; do
    step "Checking the embedded preset for $target"
    ensure_target "$target"
    cargo check --target "$target" -p daedalus-rs --no-default-features --features "engine,plugins"
  done
  # Separate builds, so the smoke module keeps the preset's features (the host example adds
  # `metrics`).
  cargo build --target "$WASM_TARGET" --release -p daedalus-wasm-smoke --lib
  cargo build --target "$WASM_TARGET" --release -p daedalus-wasm-bindgen-host
  cargo build --target "$WASI_TARGET" --release -p daedalus-wasm-smoke --bin daedalus-wasi-smoke
  if ! command -v node >/dev/null; then
    echo "node not found: skipping the wasm smoke runs"
    return
  fi
  step "Running the wasm and WASI runtime smoke tests"
  node scripts/wasm-smoke.mjs "$out/$WASM_TARGET/release/daedalus_wasm_smoke.wasm"
  node --no-warnings scripts/wasi-smoke.mjs "$out/$WASI_TARGET/release/daedalus-wasi-smoke.wasm"
  local version
  version="$(sed -n '/^name = "wasm-bindgen"$/{n;s/^version = "\(.*\)"$/\1/p}' Cargo.lock)"
  if [[ "$(wasm-bindgen --version 2>/dev/null)" != "wasm-bindgen $version" ]]; then
    echo "wasm-bindgen $version not found (cargo install wasm-bindgen-cli --version $version):" \
      "skipping the wasm-bindgen host run"
    [[ -z "${CI:-}" ]] || return 1
    return
  fi
  step "Running the wasm-bindgen host example"
  wasm-bindgen --target nodejs --out-dir "$out/wasm-bindgen-host" \
    "$out/$WASM_TARGET/release/daedalus_wasm_bindgen_host.wasm"
  node scripts/wasm-bindgen-host.mjs "$out/wasm-bindgen-host/daedalus_wasm_bindgen_host.js"
}

# Criterion writes to `$CARGO_TARGET_DIR/criterion`; compare two such directories with
# `scripts/bench-compare.py BASELINE CURRENT`.
cmd_bench() {
  step "Running host bridge and executor benches"
  cargo bench -p daedalus-engine --features plugins --bench host_graph_drive
  cargo bench -p daedalus-runtime --bench executor_snapshot
  cargo bench -p daedalus-rs --features engine-full,plugins --bench graph_frame
}

# Run on the target device (Raspberry Pi 5 / CM5, or any Linux Vulkan GPU): the ignored dmabuf
# hardware tests, then a paste-friendly capability report. See docs/testing.md.
cmd_pi() {
  step "Running dmabuf hardware tests"
  cargo test -p daedalus-gpu --features gpu-dmabuf -- --include-ignored dmabuf
  step "Probing GPU and dmabuf support"
  cargo run -p daedalus-gpu --features gpu-dmabuf --example gpu_probe
}

cmd_all() {
  cmd_lints
  cmd_check
  cmd_features
  cmd_clippy
  cmd_test
  cmd_macro_ui
  cmd_examples
  cmd_smoke
}

main() {
  [[ $# -eq 0 ]] && set -- all
  local sub
  for sub in "$@"; do
    case "$sub" in
      -h | --help | help) usage ;;
      all | lints | check | features | clippy | test | examples | smoke | aarch64 | lean | nostd | \
        wasm | bench | pi)
        "cmd_$sub" ;;
      macro-ui) cmd_macro_ui ;;
      *)
        echo "unknown subcommand: $sub" >&2
        usage >&2
        exit 2
        ;;
    esac
  done
}

main "$@"
