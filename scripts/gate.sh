#!/usr/bin/env bash
# The check every PR into next must pass. CI runs exactly this.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
first=(format clippy test boundaries)
rest=()
for file in scripts/ci.d/*.sh; do
  name="$(basename "$file" .sh)"
  [[ " ${first[*]} " == *" $name "* ]] || rest+=("$name")
done
scripts/ci.sh "${first[@]}" "${rest[@]}"
scripts/check-file-sizes.sh
scripts/check-workspace-deps.sh
testing/boundary/run.sh
