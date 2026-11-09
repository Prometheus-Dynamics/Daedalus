#!/usr/bin/env bash
set -euo pipefail
cargo test -q -p daedalus-accel --features generator
