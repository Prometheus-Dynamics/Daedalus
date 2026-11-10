#!/usr/bin/env bash
# The architecture rules. Usage: check-boundaries.sh [--root DIR]
# Exit 1 on any violation. Each violation prints the rule name, the path and the line.
set -euo pipefail
exec python3 "$(dirname "${BASH_SOURCE[0]}")/boundaries.py" "$@"
