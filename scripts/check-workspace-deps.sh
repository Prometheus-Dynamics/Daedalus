#!/usr/bin/env bash
# Rule workspace-deps: a third-party dependency in a member manifest uses `workspace = true`,
# so its version lives only in the root. A crate in this repository is a relative path.
# Usage: check-workspace-deps.sh [--root DIR]
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ "${1:-}" == "--root" ]]; then root="$2"; fi
cd "$root"
status=0
for manifest in core/*/Cargo.toml backends/*/Cargo.toml languages/*/Cargo.toml service/*/Cargo.toml \
  testing/crates/*/Cargo.toml examples/*/Cargo.toml; do
  [[ -f "$manifest" ]] || continue
  out=$(awk '
    /^\[/ { dep = ($0 ~ /dependencies(\.[^]]*)?\]/ && $0 !~ /^\[workspace/) ; next }
    !dep || /^[[:space:]]*(#|$)/ { next }
    /workspace[[:space:]]*=[[:space:]]*true/ { next }
    /path[[:space:]]*=/ { next }
    /^[A-Za-z0-9_-]+\.workspace[[:space:]]*=/ { next }
    { printf "workspace-deps: %s:%d: %s\n", FILENAME, FNR, $0 }
  ' "$manifest")
  if [[ -n "$out" ]]; then
    echo "$out"
    status=1
  fi
done
exit "$status"
