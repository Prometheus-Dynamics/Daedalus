#!/usr/bin/env bash
# Runs named steps. Each step is scripts/ci.d/<step>.sh, so PRs add steps without editing this file.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# Incremental builds are Cargo's profile default: on for the debug and test profiles, which a gate rebuilds after
# every edit, and off for release and the firmware profile. The steps whose result is the code
# itself (firmware sizes, instruction counts) build with it off whatever the caller's environment.

# Tests that open a GPU open the default adapter, and the stress tests open hundreds of devices on
# it. Where lavapipe is installed the steps get lavapipe, so a gate never loads the machine's own
# GPU; a caller that names its driver keeps it. The GPU lanes choose their device themselves.
if [[ -z "${VK_DRIVER_FILES:-}${VK_ICD_FILENAMES:-}" ]] && icd=$(scripts/lavapipe-icd.sh); then
  export VK_DRIVER_FILES="$icd" VK_ICD_FILENAMES="$icd"
fi

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
