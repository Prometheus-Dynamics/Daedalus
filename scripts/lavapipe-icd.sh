#!/usr/bin/env bash
# Prints the path of the installed lavapipe driver (Mesa's Vulkan on the CPU) for this machine's
# architecture, or fails when there is none.
set -euo pipefail
for candidate in /usr/share/vulkan/icd.d/lvp_icd.*.json /usr/share/vulkan/icd.d/lvp_icd.json \
  /etc/vulkan/icd.d/lvp_icd.*.json; do
  if [[ -f "$candidate" && "$candidate" != *i686* ]]; then
    echo "$candidate"
    exit 0
  fi
done
exit 1
