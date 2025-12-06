#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

combos=(
  "--no-default-features --features gpu-noop"
  "--no-default-features --features gpu-mock"
  "--no-default-features --features gpu-wgpu"
  "--all-features"
)

for combo in "${combos[@]}"; do
  echo "==> Checking daedalus-gpu ${combo}"
  cargo check -p daedalus-gpu $combo
done
