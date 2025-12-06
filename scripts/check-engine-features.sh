#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

run() {
  echo "+ $*"
  "$@"
}

# Minimal build (no defaults)
run cargo test -p daedalus-engine --no-default-features --tests

# Default feature set
run cargo test -p daedalus-engine

# Maximal surface with GPU mock + queues + pool + payload-value + bundle/config
run cargo test -p daedalus-engine --no-default-features \
  --features "config-env,bundle-io,gpu-mock,lockfree-queues,executor-pool,payload-value"

