#!/usr/bin/env bash
set -euo pipefail
cargo clippy --locked --workspace --all-targets -- -D warnings
