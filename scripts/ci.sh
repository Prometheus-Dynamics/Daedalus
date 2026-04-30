#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

echo "==> Checking file sizes"
"$root_dir/scripts/check-file-sizes.sh"

echo "==> Checking workspace dependency centralization"
"$root_dir/scripts/check-workspace-deps.sh"

echo "==> Checking async GPU paths"
"$root_dir/scripts/check-gpu-async-blocking.sh"

echo "==> Checking formatting"
cargo fmt --all -- --check

echo "==> Checking workspace"
cargo check --workspace --all-targets

echo "==> Checking feature surfaces"
cargo check -p daedalus-rs --no-default-features
cargo check -p daedalus-rs --features "engine,plugins,gpu-mock"
cargo check -p daedalus-runtime --features "metrics,executor-pool,lockfree-queues"
cargo check -p daedalus-ffi --no-default-features
cargo check -p daedalus-ffi --features "image-payload"
cargo check -p daedalus-ffi --features "gpu-wgpu"

echo "==> Running clippy"
cargo clippy --workspace --all-targets --features "engine,plugins" -- -D warnings

echo "==> Running tests"
cargo test --workspace --all-targets --features "engine,plugins"

echo "==> Building examples"
cargo check -p daedalus-rs --features "engine,plugins" --examples

echo "==> Running CPU-only runtime debugging examples"
cargo run -p daedalus-examples --quiet --bin runtime_metrics >/dev/null
cargo run -p daedalus-examples --quiet --bin transport_metrics >/dev/null
cargo run -p daedalus-examples --quiet --bin ownership_metrics >/dev/null
cargo run -p daedalus-examples --quiet --bin lifecycle_trace >/dev/null
cargo run -p daedalus-examples --quiet --bin plan_debug >/dev/null
cargo run -p daedalus-examples --quiet --bin overhead_floor >/dev/null
cargo run -p daedalus-examples --quiet --bin observability >/dev/null
cargo run -p daedalus-examples --quiet --all-features --bin backpressure_diagnostics >/dev/null
