#!/usr/bin/env bash
# Nothing in the GPU backend blocks while a region runs: a step polls, and the runner asks
# again next tick. The waits that exist are bounded and live in backends/wgpu/src/wait.rs, and
# device setup blocks in backends/wgpu/src/driver.rs. This fails on a blocking call anywhere
# else in the backend's source.
#
#   scripts/check-gpu-async-blocking.sh [ROOT]
set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$root"

readonly PATTERN='PollType::Wait|wait_indefinitely\(|pollster::block_on|\bblock_on\('
readonly ALLOWED='^backends/wgpu/src/(wait|driver)\.rs:'

[[ -d backends/wgpu/src ]] || exit 0
hits=$(grep -rnE --include='*.rs' "$PATTERN" backends/wgpu/src | grep -vE "$ALLOWED" || true)
if [[ -n "$hits" ]]; then
  echo "$hits" >&2
  echo "gpu blocking check failed: a wait belongs in backends/wgpu/src/wait.rs, with a bound" >&2
  exit 1
fi
