#!/usr/bin/env bash
# Every test binary `cargo test --workspace --all-targets` runs, run side by side: see
# scripts/test-parallel.py. Arguments go to cargo test, as `--features x`.
set -euo pipefail
exec scripts/test-parallel.py "$@"
