#!/usr/bin/env bash
# The fixtures build on their own, outside the root workspace, and their lock files aren't
# committed. A fixture with no lock file gets one here, made from the root lock: cargo keeps the
# root lock's version of every package the fixture shares with the workspace and drops the rest.
# Otherwise its first build resolves it afresh, to whatever the registry has then, and a check that
# reads it `--locked` before that build fails, as one did on a fresh checkout while the tests were
# still building. A fixture that already has one keeps it while it still fits: a lock that a change
# to the workspace's dependencies has left behind is made again from the root lock, since nothing
# else would (the file isn't committed, so a checkout that built before the change keeps the old
# one, and every `--locked` read of it fails).
# --self-test: a fixture's lock made here holds the root lock's versions, not the newest (offline,
# from planted packages).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
derive() {
  local root_lock="$1" dir="$2"
  if [[ -f "$dir/Cargo.lock" ]] && cargo metadata -q --locked --offline --format-version 1 \
      --manifest-path "$dir/Cargo.toml" > /dev/null 2>&1; then
    return 0
  fi
  cp "$root_lock" "$dir/Cargo.lock.new"
  mv "$dir/Cargo.lock.new" "$dir/Cargo.lock"
  cargo metadata -q --offline --format-version 1 --manifest-path "$dir/Cargo.toml" > /dev/null \
    || cargo metadata -q --format-version 1 --manifest-path "$dir/Cargo.toml" > /dev/null
}
if [[ "${1:-}" == "--self-test" ]]; then
  work="$(mktemp -d)"
  trap 'rm -rf "$work"' EXIT
  # Offline, in a cargo home of its own whose one source is a directory of planted packages, so it
  # needs nothing fetched: `plant` at 1.0.0 and at 1.0.1. A planted root holding plant at 1.0.0,
  # older than the newest, and a fixture that uses plant: its lock must hold the root's 1.0.0,
  # where resolving afresh takes 1.0.1.
  export CARGO_HOME="$work/home"
  mkdir -p "$CARGO_HOME"
  printf '[net]\noffline = true\n[source.crates-io]\nreplace-with = "plants"\n[source.plants]\ndirectory = "%s"\n' \
    "$work/plants" > "$CARGO_HOME/config.toml"
  for version in 1.0.0 1.0.1; do
    mkdir -p "$work/plants/plant-$version/src"
    printf '[package]\nname = "plant"\nversion = "%s"\nedition = "2021"\n' "$version" > "$work/plants/plant-$version/Cargo.toml"
    : > "$work/plants/plant-$version/src/lib.rs"
    printf '{"files":{},"package":null}' > "$work/plants/plant-$version/.cargo-checksum.json"
  done
  for crate in root fx; do
    mkdir -p "$work/$crate/src"
    printf '[package]\nname = "%s"\nversion = "0.0.0"\nedition = "2024"\n[workspace]\n[dependencies]\nplant = "1"\n' "$crate" > "$work/$crate/Cargo.toml"
    : > "$work/$crate/src/lib.rs"
  done
  # The root's lock, written as it is (`cargo update --precise` does nothing against a directory
  # source), and read `--locked`; resolving afresh, the planted fixture takes 1.0.1, so a fixture
  # that got 1.0.0 got it from the root lock.
  printf 'version = 4\n\n[[package]]\nname = "plant"\nversion = "1.0.0"\nsource = "registry+https://github.com/rust-lang/crates.io-index"\n\n[[package]]\nname = "root"\nversion = "0.0.0"\ndependencies = [\n "plant",\n]\n' \
    > "$work/root/Cargo.lock"
  cargo metadata -q --locked --format-version 1 --manifest-path "$work/root/Cargo.toml" > /dev/null
  cargo generate-lockfile -q --manifest-path "$work/fx/Cargo.toml"
  if ! grep -qA1 '^name = "plant"$' "$work/fx/Cargo.lock" || [[ "$(grep -A1 '^name = "plant"$' "$work/fx/Cargo.lock" | tail -1)" != 'version = "1.0.1"' ]]; then
    echo "fixture-locks self-test: resolving afresh didn't take the newest planted plant, 1.0.1" >&2
    exit 1
  fi
  mv "$work/fx/Cargo.lock" "$work/fx/Cargo.lock.afresh"
  derive "$work/root/Cargo.lock" "$work/fx"
  got="$(grep -A1 '^name = "plant"$' "$work/fx/Cargo.lock" | sed -n 's/^version = "\(.*\)"/\1/p')"
  if [[ "$got" != "1.0.0" ]]; then
    echo "fixture-locks self-test: the fixture's plant is ${got:-none}, the root lock's 1.0.0" >&2
    exit 1
  fi
  # A lock left behind by a change of dependencies (here, one from before the fixture used plant)
  # is made again, with the root lock's version.
  printf 'version = 4\n\n[[package]]\nname = "fx"\nversion = "0.0.0"\n' > "$work/fx/Cargo.lock"
  derive "$work/root/Cargo.lock" "$work/fx"
  got="$(grep -A1 '^name = "plant"$' "$work/fx/Cargo.lock" | sed -n 's/^version = "\(.*\)"/\1/p')"
  if [[ "$got" != "1.0.0" ]]; then
    echo "fixture-locks self-test: a stale lock was kept: its plant is ${got:-none}" >&2
    exit 1
  fi
  echo "fixture-locks self-test: a fixture's lock keeps the root lock's versions, and a stale one is made again"
  exit 0
fi
shopt -s nullglob
for manifest in testing/fixtures/*/Cargo.toml; do
  derive Cargo.lock "$(dirname "$manifest")"
done
