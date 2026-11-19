#!/usr/bin/env bash
# The fixtures build on their own, outside the root workspace, and their lock files aren't
# committed. A fixture with no lock file gets one here, made from the root lock: cargo keeps the
# root lock's version of every package the fixture shares with the workspace and drops the rest.
# Otherwise its first build resolves it afresh, to whatever the registry has then, and a check that
# reads it `--locked` before that build fails, as one did on a fresh checkout while the tests were
# still building. A fixture that already has one keeps it.
# --self-test: a fixture's lock made here holds the root lock's versions, not the newest.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
derive() {
  local root_lock="$1" dir="$2"
  [[ -f "$dir/Cargo.lock" ]] && return 0
  cp "$root_lock" "$dir/Cargo.lock.new"
  mv "$dir/Cargo.lock.new" "$dir/Cargo.lock"
  cargo metadata -q --offline --format-version 1 --manifest-path "$dir/Cargo.toml" > /dev/null \
    || cargo metadata -q --format-version 1 --manifest-path "$dir/Cargo.toml" > /dev/null
}
if [[ "${1:-}" == "--self-test" ]]; then
  work="$(mktemp -d)"
  trap 'rm -rf "$work"' EXIT
  # A planted root holding itoa at 1.0.18, older than the newest, and a fixture that uses itoa:
  # its lock must hold the root's 1.0.18, where resolving afresh takes the newest.
  for crate in root fx; do
    mkdir -p "$work/$crate/src"
    printf '[package]\nname = "%s"\nversion = "0.0.0"\nedition = "2024"\n[workspace]\n[dependencies]\nitoa = "1"\n' "$crate" > "$work/$crate/Cargo.toml"
    : > "$work/$crate/src/lib.rs"
  done
  cargo generate-lockfile -q --manifest-path "$work/root/Cargo.toml"
  cargo update -q --manifest-path "$work/root/Cargo.toml" -p itoa --precise 1.0.18
  derive "$work/root/Cargo.lock" "$work/fx"
  got="$(grep -A1 '^name = "itoa"$' "$work/fx/Cargo.lock" | sed -n 's/^version = "\(.*\)"/\1/p')"
  if [[ "$got" != "1.0.18" ]]; then
    echo "fixture-locks self-test: the fixture's itoa is ${got:-none}, the root lock's 1.0.18" >&2
    exit 1
  fi
  echo "fixture-locks self-test: a fixture's lock keeps the root lock's versions"
  exit 0
fi
shopt -s nullglob
for manifest in testing/fixtures/*/Cargo.toml; do
  derive Cargo.lock "$(dirname "$manifest")"
done
