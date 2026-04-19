#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

echo "==> Checking file sizes"
"$root_dir/scripts/check-file-sizes.sh"

echo "==> Checking formatting"
cargo fmt --all -- --check

echo "==> Running clippy"
cargo clippy --workspace --all-targets --features "engine,plugins" -- -D warnings

echo "==> Running tests"
cargo test --workspace --all-targets --features "engine,plugins"

echo "==> Building examples"
cargo check -p daedalus-rs --features "engine,plugins" --examples
