#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

combos=(
  "--no-default-features"
  "--no-default-features --features plugin"
  "--no-default-features --features bundle"
  "--no-default-features --features ffi"
  "--all-features"
)

for combo in "${combos[@]}"; do
  echo "==> Checking daedalus-registry ${combo}"
  cargo check -p daedalus-registry $combo
done
