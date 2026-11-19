#!/usr/bin/env bash
# bound: 441 s (three times its slowest green run, 147 s, in a cold build on a fresh slot)
set -euo pipefail
cargo clippy --locked --workspace --all-targets -- -D warnings
