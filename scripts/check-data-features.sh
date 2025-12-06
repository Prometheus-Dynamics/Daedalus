#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

combos=(
  "--no-default-features"
  "--no-default-features --features json"
  "--no-default-features --features json,gpu,async"
  "--no-default-features --features json,schema,proto"
  "--all-features"
)

for combo in "${combos[@]}"; do
  echo "==> Checking daedalus-data ${combo}"
  cargo check -p daedalus-data $combo
done
