#!/usr/bin/env bash
# A linker for cross builds. `ZIG_TARGET` says which: arm-linux-musleabihf and so on.
set -euo pipefail
exec zig cc -target "${ZIG_TARGET:?}" "$@"
