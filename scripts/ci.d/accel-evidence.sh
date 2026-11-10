#!/usr/bin/env bash
# The CPU evidence lane: runs the kernel conformance tests with evidence recording on (stable,
# then the portable kernels on the pinned nightly), writes the registry dump, and renders the
# backend matrix against docs/backend-matrix.csv. A lost pass cell fails the gate.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

results=$(mktemp -d)
export ACCEL_RESULTS_DIR="$results" ACCEL_REV="$(git rev-parse --short HEAD)"
cargo test --locked -q -p daedalus-kernels-sys

NIGHTLY=${ACCEL_NIGHTLY:-nightly-2026-10-15}
RUSTFLAGS="--cfg daedalus_portable_simd" CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target}/portable" \
  cargo "+$NIGHTLY" test --locked -q -p daedalus-kernels-sys --test conformance portable

ACCEL_DUMP="$results/daedalus.json" cargo test --locked -q -p daedalus-inventory --test registry registry_dump
mkdir -p "$results/out"
scripts/accel.sh render --registry "$results/daedalus.json" --results "$results" \
  --results testing/board-runs/accel --baseline docs/backend-matrix.csv --scan . --out "$results/out"
