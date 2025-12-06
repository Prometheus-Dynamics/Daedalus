#!/usr/bin/env bash
set -euo pipefail

echo "Checking planner (default features)..."
cargo check -p daedalus-planner

echo "Checking planner (no default features)..."
cargo check -p daedalus-planner --no-default-features

echo "Checking planner (gpu feature)..."
cargo check -p daedalus-planner --features gpu

echo "All planner feature checks passed."
