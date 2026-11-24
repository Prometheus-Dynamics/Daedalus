#!/usr/bin/env bash
# bound: 1317 s (three times its slowest green run, 439 s, in a cold build on a fresh slot)
set -euo pipefail
# Its lane's build directory: no other lane builds there (scripts/gate.sh).
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}/cross"
scripts/check-boundaries.sh
# The board-run judges are checked against planted runs, so a judge that can't fail is caught.
python3 scripts/board/judge.py --self-test
