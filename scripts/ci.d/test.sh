#!/usr/bin/env bash
set -euo pipefail
cargo test --locked --workspace --all-targets
