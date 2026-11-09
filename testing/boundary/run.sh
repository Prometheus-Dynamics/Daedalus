#!/usr/bin/env bash
# Each checker must fail on its planted tree and name the rule, and pass on the clean one.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scripts="$here/../../scripts"
fixtures="$here/fixtures"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
failed=0

expect_fail() { # rule, checker, root
  local out
  if out="$("$2" --root "$3" --no-build 2>&1)"; then
    echo "FAIL $1: expected exit 1, got 0"; failed=1
  elif ! grep -q "^$1:" <<<"$out"; then
    echo "FAIL $1: rule name not in output: $out"; failed=1
  else
    echo "ok   $1"
  fi
}


for rule in core-deps no-lemnos core-names unsafe heap-free glob-folders names; do
  expect_fail "$rule" "$scripts/check-boundaries.sh" "$fixtures/$rule"
done
expect_fail unsafe "$scripts/check-boundaries.sh" "$fixtures/unsafe-comment"

if out="$("$scripts/check-boundaries.sh" --root "$fixtures/clean" --no-build 2>&1)"; then
  echo "ok   clean tree"
else
  echo "FAIL clean tree: $out"; failed=1
fi

if out="$("$scripts/check-workspace-deps.sh" --root "$fixtures/workspace-deps" 2>&1)"; then
  echo "FAIL workspace-deps: expected exit 1"; failed=1
elif grep -q '^workspace-deps:' <<<"$out"; then echo "ok   workspace-deps"; else echo "FAIL workspace-deps: $out"; failed=1; fi
"$scripts/check-workspace-deps.sh" --root "$fixtures/clean" || { echo "FAIL workspace-deps on clean"; failed=1; }

git -C "$tmp" init -q
seq 1 801 > "$tmp/big.rs"
if out="$("$scripts/check-file-sizes.sh" --root "$tmp" 2>&1)"; then
  echo "FAIL file-size: expected exit 1"; failed=1
elif grep -q '^file-size:' <<<"$out"; then echo "ok   file-size"; else echo "FAIL file-size: $out"; failed=1; fi

exit "$failed"
