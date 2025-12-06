#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

echo "==> Checking GPU feature matrix"
"$root_dir/scripts/check-gpu-features.sh"

echo "==> Checking data feature matrix"
"$root_dir/scripts/check-data-features.sh"

echo "==> Checking registry feature matrix"
"$root_dir/scripts/check-registry-features.sh"
