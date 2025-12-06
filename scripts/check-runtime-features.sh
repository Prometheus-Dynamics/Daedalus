#!/usr/bin/env bash
set -euo pipefail

echo "Checking runtime (default features)..."
cargo check -p daedalus-runtime

echo "Checking runtime (no default features)..."
cargo check -p daedalus-runtime --no-default-features

echo "Checking runtime (gpu feature)..."
cargo check -p daedalus-runtime --features gpu

echo "Checking runtime (gpu-mock feature)..."
cargo check -p daedalus-runtime --features gpu-mock

echo "All runtime feature checks passed."
