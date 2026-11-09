#!/usr/bin/env bash
# The check every PR into next must pass. CI runs exactly this.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
scripts/ci.sh format clippy test boundaries
scripts/check-file-sizes.sh
scripts/check-workspace-deps.sh
testing/boundary/run.sh
