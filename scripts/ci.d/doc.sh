#!/usr/bin/env bash
# bound: 690 s (three times its slowest green run, 230 s)
# The API documentation builds with no warning. The tests that read docs/ (every relative link
# resolves, no guide sends a reader to a path of the branch this one replaces) are `daedalus`'s
# `docs::` tests, which the test step runs with every other binary.
set -euo pipefail
# Its lane's build directory: no other lane builds there (scripts/gate.sh).
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}/doc-build"
RUSTDOCFLAGS="-D warnings" cargo doc --locked -q --workspace --no-deps
