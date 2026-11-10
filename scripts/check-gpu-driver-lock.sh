#!/usr/bin/env bash
# Every graphics instance and device is made under the driver lock, in one file per crate that may
# make one: backends/wgpu/src/driver.rs and backends/vk-sys/src/device.rs. The Vulkan loader
# crashes when these race (see backends/wgpu/src/driver.rs). This fails on a creation call in any
# other Rust file.
#
#   scripts/check-gpu-driver-lock.sh [ROOT]
set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$root"

readonly PATTERN='\bInstance::new\(|\.request_device\(|create_device_from_hal|Entry::(load|linked)\(|create_instance\('
readonly ALLOWED='^(backends/wgpu/src/driver\.rs|backends/vk-sys/src/device\.rs):'

dirs=()
for dir in backends interfaces languages service core examples testing; do
  [[ -d "$dir" ]] && dirs+=("$dir")
done
hits=$(grep -rnE --include='*.rs' "$PATTERN" "${dirs[@]}" | grep -vE "$ALLOWED" || true)
if [[ -n "$hits" ]]; then
  echo "$hits" >&2
  echo "gpu driver-lock check failed: instances and devices are made only in backends/wgpu/src/driver.rs and backends/vk-sys/src/device.rs" >&2
  exit 1
fi
