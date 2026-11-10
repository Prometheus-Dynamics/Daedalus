#!/usr/bin/env bash
# The blocking check, and a check that it still catches a planted wait.
set -euo pipefail

scripts/check-gpu-async-blocking.sh

planted=$(mktemp -d)
trap 'rm -rf "$planted"' EXIT
mkdir -p "$planted/backends/wgpu/src/shader"
echo 'fn wait() { device.poll(wgpu::PollType::Wait { submission_index: None, timeout: None }); }' \
  >"$planted/backends/wgpu/src/wait.rs"
scripts/check-gpu-async-blocking.sh "$planted"
echo 'fn run() { device.poll(wgpu::PollType::Wait { submission_index: None, timeout: None }); }' \
  >"$planted/backends/wgpu/src/shader/region.rs"
if scripts/check-gpu-async-blocking.sh "$planted" 2>/dev/null; then
  echo "gpu blocking check missed a planted wait" >&2
  exit 1
fi
