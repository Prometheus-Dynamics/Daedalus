#!/usr/bin/env bash
# The name of the validation layer's cache directory: what its build is made from, the pinned
# commit and the zig that compiles it (scripts/vvl-build.sh). A new pin or a new zig is a new
# directory, built once by scripts/dev/setup.sh.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
commit="$(sed -n 's/^VVL_COMMIT=//p' "$here/vvl.pin")"
echo "${commit:0:12}-zig$(zig version 2>/dev/null || echo none)"
