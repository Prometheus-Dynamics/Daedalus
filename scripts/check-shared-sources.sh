#!/usr/bin/env bash
# The shared sources in crates/ (build_features.rs, portable.rs, trace.rs) are copied into the
# crates that use them, as regular files: a path outside a crate root cannot be packaged, and
# symlinks need `core.symlinks` (Windows). This fails when a copy differs from its source;
# `--fix` rewrites every copy from its source. Edit the source in crates/, then run `--fix`.
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

# copy <- source
shared=(
  "crates/core/build.rs crates/build_features.rs"
  "crates/data/build.rs crates/build_features.rs"
  "crates/planner/build.rs crates/build_features.rs"
  "crates/registry/build.rs crates/build_features.rs"
  "crates/runtime/build.rs crates/build_features.rs"
  "crates/transport/build.rs crates/build_features.rs"
  "crates/core/src/portable.rs crates/portable.rs"
  "crates/data/src/portable.rs crates/portable.rs"
  "crates/engine/src/portable.rs crates/portable.rs"
  "crates/planner/src/portable.rs crates/portable.rs"
  "crates/runtime/src/portable.rs crates/portable.rs"
  "crates/transport/src/portable.rs crates/portable.rs"
  "crates/engine/src/trace.rs crates/trace.rs"
  "crates/runtime/src/trace.rs crates/trace.rs"
)

fix=0
[[ "${1:-}" == "--fix" ]] && fix=1
stale=0
for pair in "${shared[@]}"; do
  read -r copy source <<<"$pair"
  if [[ -L "$copy" ]] || ! cmp -s "$copy" "$source"; then
    if ((fix)); then
      rm -f "$copy"
      cp "$source" "$copy"
      echo "updated $copy from $source"
    else
      echo "$copy differs from $source (run scripts/check-shared-sources.sh --fix)" >&2
      stale=1
    fi
  fi
done
exit "$stale"
