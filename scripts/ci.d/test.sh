#!/usr/bin/env bash
# bound: 2508 s (three times its slowest green run, 836 s, in a cold build on a fresh slot)
# Every test binary `cargo test --workspace --all-targets` runs, run side by side: see
# scripts/test-parallel.py. Arguments go to cargo test, as `--features x`.
set -euo pipefail
exec scripts/test-parallel.py "$@"
