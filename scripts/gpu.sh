#!/usr/bin/env bash
# Runs the tests of the GPU backend crates against a real adapter. A machine with none fails here:
# the tests skip only when DAEDALUS_GPU_REQUIRED is unset. It needs a Vulkan driver, so it has its
# own workflow, .github/workflows/ci-gpu.yml, which installs lavapipe.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

packages=()
for manifest in backends/wgpu*/Cargo.toml; do
  packages+=(-p "$(sed -n 's/^name = "\(.*\)"/\1/p' "$manifest" | head -1)")
done
DAEDALUS_GPU_REQUIRED=1 cargo test "${packages[@]}"
