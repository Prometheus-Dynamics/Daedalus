#!/usr/bin/env bash
# bound: 60 s (its slowest green run took 12 s; no bound is under a minute)
set -euo pipefail
cargo fmt --all --check
