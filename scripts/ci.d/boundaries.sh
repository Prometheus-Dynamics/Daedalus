#!/usr/bin/env bash
set -euo pipefail
# scripts/check-boundaries.sh is added by PR A2.
if [[ -x scripts/check-boundaries.sh ]]; then
  scripts/check-boundaries.sh
fi
