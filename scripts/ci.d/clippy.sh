#!/usr/bin/env bash
# bound: 306 s (three times its slowest green run, 102 s)
set -euo pipefail
cargo clippy --locked --workspace --all-targets -- -D warnings
