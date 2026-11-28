#!/usr/bin/env bash
# Builds the daemon in two cargo homes, at paths of different lengths, and compares the bytes: with
# scripts/home-remap.sh, a build names no home, so the two must be identical, in the debug and the
# release profile. `--without-remap` builds the same with the remap left out: those must differ, so
# the check is shown to see a home in what is built.
#
#   scripts/check-home-remap.sh [--without-remap] [PACKAGE BINARY]   (default: daedalusd daedalusd)
#
# Both builds use one build directory, one after the other, so the checkout and the build
# directory are the same and only the home moves. The homes are reflinked copies of CARGO_HOME,
# offline, in a scratch directory that is removed. Every dependency is built twice per profile:
# this is a check run by hand (docs/development.md), not a gate step.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
remap=1
if [[ ${1:-} == --without-remap ]]; then
  remap=0
  shift
fi
package="${1:-daedalusd}" binary="${2:-${1:-daedalusd}}"
base="${CARGO_HOME:-$HOME/.cargo}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
homes=("$work/a" "$work/a-much-longer-second-cargo-home")
for home in "${homes[@]}"; do
  # The crates already fetched, read offline: nothing is downloaded and the base home is unchanged.
  mkdir -p "$home"
  cp -a --reflink=auto "$base/registry" "$home/"
  [[ ! -d $base/git ]] || cp -a --reflink=auto "$base/git" "$home/"
done
source scripts/home-remap.sh
target="$work/target"
same=1
for profile in debug release; do
  out="$target/$profile/$binary" args=()
  [[ $profile == debug ]] || args=(--release)
  for home in "${homes[@]}"; do
    (
      export CARGO_HOME="$home" CARGO_TARGET_DIR="$target"
      unset CARGO_BUILD_RUSTFLAGS
      [[ $remap == 0 ]] || home_remap
      cargo build --locked --offline -q "${args[@]}" -p "$package" --bin "$binary"
    )
    cp "$out" "$home.$profile"
  done
  if cmp -s "$work/a.$profile" "$work/a-much-longer-second-cargo-home.$profile"; then
    echo "home-remap: $binary, $profile: identical from two homes"
  else
    held=$(grep -c -a -F "$work/a" "$work/a.$profile" || true)
    echo "home-remap: $binary, $profile: differs between two homes ($held places name the first home)"
    same=0
  fi
done
if [[ $remap == 1 && $same == 1 ]] || [[ $remap == 0 && $same == 0 ]]; then
  exit 0
fi
[[ $remap == 1 ]] && echo "home-remap: a build still names its cargo home" >&2 ||
  echo "home-remap: without the remap the builds matched: the check can't see a home" >&2
exit 1
