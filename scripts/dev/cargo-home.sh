#!/usr/bin/env bash
# The gate's own cargo homes, filled before it starts, so it never downloads and never waits on
# another program's lock on ~/.cargo's package cache: a step blocked there makes no progress of its
# own, and on this machine every other dependency's builds take that lock.
#
#   scripts/dev/cargo-home.sh LANE...     (scripts/dev/env.sh sourced)
#
# Fetches every committed lock file's crates into DAEDALUS_CARGO_HOME (`cargo fetch --locked`, the
# only step that goes online), whose config.toml sets cargo offline and turns its automatic cleaning
# of the package cache off. Says each lock file's sha256, the sources (the registry, each git source
# at its commit), the number of crate files and a sha256 over their sums. Then each LANE gets a home
# of its own, DAEDALUS_CARGO_HOME.lanes/LANE, a copy (reflinks on this disk) brought up to the fetch
# when the fetch changed: cargo takes the package-cache lock for writing while it resolves, even
# offline, so lanes sharing one home would wait on each other. The paths stay the same from fetch to
# fetch: a dependency's source path is in what it builds (panic locations, debug info) and in the
# test step's reuse keys (scripts/lane-reuse.py common), so a new path would rebuild and rerun all.
# A lane's home is only ever added to, never emptied. A failed fetch exits 1, and nothing is copied.
set -uo pipefail
home="${DAEDALUS_CARGO_HOME:?source scripts/dev/env.sh}"
cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 1
mkdir -p "$home"
config=$'# Written by scripts/dev/cargo-home.sh: the gate\'s own CARGO_HOME.\n[net]\noffline = true\n[cache]\nauto-clean-frequency = "never"'
[[ "$(cat "$home/config.toml" 2>/dev/null)" == "$config" ]] || printf '%s\n' "$config" > "$home/config.toml"
locks="$(git ls-files '*Cargo.lock')"
echo "fetch: CARGO_HOME=$home"
status=0
for lock in $locks; do
  echo "fetch: $lock sha256 $(sha256sum "$lock" | cut -d' ' -f1)"
  CARGO_HOME="$home" cargo fetch -q --locked --config net.offline=false \
    --manifest-path "$(dirname "$lock")/Cargo.toml" || { echo "fetch: $lock: cargo fetch failed"; status=1; }
done
[[ $status == 0 ]] || exit 1
# shellcheck disable=SC2086 # the lock files' paths have no spaces
echo "fetch: sources: $(sed -n 's/^source = "\([a-z]*+[^?#"]*\)[^#"]*\(#[0-9a-f]*\)\{0,1\}".*/\1\2/p' $locks | sort -u | tr '\n' ' ')"
sums="$(cd "$home/registry/cache" && find . -name '*.crate' -print0 | LC_ALL=C sort -z | xargs -0r sha256sum)"
over="$(printf '%s\n' "$sums" | sha256sum | cut -c1-64)"
echo "fetch: $(printf '%s' "$sums" | grep -c .) registry crate files, sha256 over their sums $over"
echo "fetch: $(find "$home/git/checkouts" -mindepth 2 -maxdepth 2 -type d 2>/dev/null | wc -l) git checkouts"
# What a lane's home was last brought up to: the crates, the git sources and the config.
key="$( { echo "$over"; find "$home/git/db" "$home/git/checkouts" -mindepth 2 -maxdepth 2 2>/dev/null | LC_ALL=C sort; echo "$config"; } | sha256sum | cut -c1-16)"
for lane in "$@"; do
  dir="$home.lanes/$lane"
  [[ "$(cat "$dir/.fetched" 2>/dev/null)" == "$key" ]] && continue
  mkdir -p "$dir"
  cp -a --reflink=auto -u "$home/." "$dir/" || { echo "fetch: the $lane lane's home $dir could not be brought up to the fetch"; exit 1; }
  echo "$key" > "$dir/.fetched"
  echo "fetch: the $lane lane's home brought up to $key"
done
