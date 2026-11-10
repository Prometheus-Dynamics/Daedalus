#!/usr/bin/env bash
# The driver-lock check, and a check that it still catches a planted creation call.
set -euo pipefail

scripts/check-gpu-driver-lock.sh

planted=$(mktemp -d)
trap 'rm -rf "$planted"' EXIT
mkdir -p "$planted/backends/other/src" "$planted/backends/wgpu/src"
echo 'fn open() { adapter.request_device(&desc); }' >"$planted/backends/wgpu/src/driver.rs"
scripts/check-gpu-driver-lock.sh "$planted"
echo 'fn open() { adapter.request_device(&desc); }' >"$planted/backends/other/src/lib.rs"
if scripts/check-gpu-driver-lock.sh "$planted" 2>/dev/null; then
  echo "gpu driver-lock check missed a planted device creation" >&2
  exit 1
fi
