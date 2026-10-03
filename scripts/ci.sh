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

step() { echo "==> $*"; }

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
  aarch64     cargo check for aarch64-unknown-linux-gnu (default, embedded, gpu-dmabuf)
  lean        tests for the lean preset (no executor pool, no metrics)
  bench       host bridge + runtime executor criterion benches
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
  cargo check -p daedalus-rs --all-targets --features "engine-full,plugins"
  cargo check -p daedalus-rs --features "dylib-plugins"
  cargo check -p daedalus-runtime --features "metrics,executor-pool,lockfree-queues"
  cargo check -p daedalus-ffi-core -p daedalus-ffi-host --no-default-features
  cargo check -p daedalus-ffi-core -p daedalus-ffi-host \
    --features "daedalus-ffi-core/image-payload,daedalus-ffi-host/image-payload"
  cargo check -p daedalus-gpu --no-default-features --features "gpu-wgpu"
  # Needs libcamera-dev + pkg-config for the styx camera example feature.
  cargo check --workspace --all-targets --all-features
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
  rustup target list --installed | grep -qx "$AARCH64_TARGET" || rustup target add "$AARCH64_TARGET"
  local target=(--target "$AARCH64_TARGET")
  cargo check "${target[@]}" --workspace --all-targets --features "$CI_FEATURES"
  cargo check "${target[@]}" -p daedalus-rs --all-targets --no-default-features --features "embedded"
  cargo check "${target[@]}" -p daedalus-gpu --all-targets --features "gpu-dmabuf"
}

# Workspace tests unify features across all members, so the daemon crate turns on the executor
# pool and metrics for everyone. This run selects only the facade, engine, and runtime crates
# (never daedalus-daemon) with defaults off, so the serial/scoped-thread executor path and the
# no-op telemetry path are what get tested.
cmd_lean() {
  step "Testing lean preset (no executor-pool, no metrics)"
  cargo test -p daedalus-rs -p daedalus-engine -p daedalus-runtime --all-targets \
    --no-default-features \
    --features "daedalus-rs/embedded,daedalus-engine/config-env,daedalus-engine/plugins,daedalus-runtime/plugins"
}

# Criterion writes to `$CARGO_TARGET_DIR/criterion`; compare two such directories with
# `scripts/bench-compare.py BASELINE CURRENT`.
cmd_bench() {
  step "Running host bridge and executor benches"
  cargo bench -p daedalus-engine --features plugins --bench host_graph_drive
  cargo bench -p daedalus-runtime --bench executor_snapshot
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
      all | lints | check | features | clippy | test | examples | smoke | aarch64 | lean | bench)
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
