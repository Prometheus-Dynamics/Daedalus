#!/usr/bin/env bash
# Every wgpu instance and device in daedalus-gpu is made under the driver lock: an instance only
# through `LockedInstance::new` (driver.rs), a device only through `open_device` or the dmabuf
# `request_device`, both of which take the held guard. The Vulkan loader crashes when these race
# (see crates/gpu/src/wgpu_backend/driver.rs). This fails on a direct call anywhere else.
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
gpu="$root_dir/crates/gpu"

pattern='\bInstance::new\(|\.request_device\(|create_device_from_hal|Entry::(load|linked)\(|create_instance\('
allowed='crates/gpu/src/wgpu_backend/driver\.rs:|crates/gpu/src/wgpu_backend/dmabuf/vulkan\.rs:.*create_device_from_hal'

if hits=$(rg -n "$pattern" "$gpu/src" "$gpu/examples" "$gpu/tests" | sed "s#^$root_dir/##" | rg -v "$allowed"); then
  echo "$hits" >&2
  echo "gpu driver-lock check failed: create instances with LockedInstance::new and devices with open_device" >&2
  exit 1
fi

echo "gpu driver-lock check passed"
