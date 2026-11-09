#!/usr/bin/env bash
# Runs named steps. Each step is scripts/ci.d/<step>.sh, so PRs add steps without editing this file.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"

if [[ $# -eq 0 ]]; then
  set -- format clippy test boundaries
fi

for name in "$@"; do
  file="scripts/ci.d/$name.sh"
  if [[ ! -x "$file" ]]; then
    echo "no such step: $name" >&2
    exit 2
  fi
  echo "==> $name"
  start=$SECONDS
  "$file"
  echo "    $name: $((SECONDS - start)) s"
done

echo "passed: $*"
