#!/usr/bin/env bash
# The acceleration matrix generator (testing/crates/accel, feature `generator`), run with the
# arguments given:
#   scripts/accel.sh asm --binary <conformance test binary> --target <triple>
#   scripts/accel.sh render --registry <dump.json>... --results <dir> \
#       --baseline docs/backend-matrix.csv --scan . --out docs/ [--release]
# The CI accel lane produces the registry dumps and the evidence (see the crate README).
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build -q -p daedalus-accel --features generator --bin accel-matrix
exec "${CARGO_TARGET_DIR:-target}/debug/accel-matrix" "$@"
