#!/usr/bin/env bash
set -euo pipefail
cargo test --locked -q -p daedalus-accel --features generator
