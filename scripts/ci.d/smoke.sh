#!/usr/bin/env bash
# Builds and runs each example binary, which must exit 0, and runs each example's test, which
# checks what it printed against its golden lines.
#
#   scripts/ci.d/smoke.sh               the self-test, then every example
#   scripts/ci.d/smoke.sh NAME...       only the named examples (the self-test runs a planted one)
#   scripts/ci.d/smoke.sh --self-test   only the self-test: a planted example with a wrong golden
#                                       line must fail on the comparison, and a right one must pass
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

readonly EXAMPLES=(quickstart basics host metrics)

fail() {
  echo "smoke: $*" >&2
  exit 1
}

run_example() { # name: the example runs and exits 0, and its test finds its output golden
  local package="daedalus-example-$1" out
  cargo run --locked -q -p "$package" >/dev/null || fail "$package did not run to exit 0"
  out="$(cargo test --locked -q -p "$package" 2>&1)" || {
    printf '%s\n' "$out" >&2
    fail "$package: its output is not the golden one"
  }
  echo "  $package: ran, and its output is the golden one"
}

planted() { # dir, the answer its golden line gives: a crate shaped like an example
  mkdir -p "$1/src" "$1/tests"
  cat >"$1/Cargo.toml" <<'EOF'
[package]
name = "daedalus-example-planted"
version = "0.0.0"
edition = "2021"
publish = false
EOF
  cat >"$1/src/lib.rs" <<'EOF'
pub fn run() -> String {
    format!("planted: 2 doubled is {}\n", 2 * 2)
}
EOF
  cat >"$1/src/main.rs" <<'EOF'
fn main() {
    print!("{}", daedalus_example_planted::run());
}
EOF
  cat >"$1/tests/planted.rs" <<EOF
const GOLDEN: &str = "planted: 2 doubled is $2\n";

#[test]
fn planted_output_matches() {
    assert_eq!(daedalus_example_planted::run(), GOLDEN);
}
EOF
  (cd "$1" && cargo generate-lockfile -q)
}

self_test() {
  local work status=0 out failed=0
  work="$(mktemp -d)"
  trap "rm -rf '$work'" EXIT
  planted "$work/good" 4
  planted "$work/bad" 5

  # Each planted crate builds into its own target directory, so the gate's cache is not touched.
  out="$(cd "$work/good" && CARGO_TARGET_DIR="$work/target-good" "$here/smoke.sh" planted 2>&1)" || status=$?
  if [[ $status -eq 0 ]]; then
    echo "  ok   planted: a golden line that matches passes the step"
  else
    echo "  FAIL planted: a golden line that matches was refused:"
    printf '%s\n' "$out"
    failed=1
  fi

  status=0
  out="$(cd "$work/bad" && CARGO_TARGET_DIR="$work/target-bad" "$here/smoke.sh" planted 2>&1)" || status=$?
  if [[ $status -ne 0 ]] && grep -qF "is not the golden one" <<<"$out" && grep -qF "assertion" <<<"$out"; then
    echo "  ok   planted: a golden line that is wrong fails the step on the comparison"
  else
    echo "  FAIL planted: a golden line that is wrong did not fail on the comparison, status $status:"
    printf '%s\n' "$out"
    failed=1
  fi

  [[ $failed -eq 0 ]] || fail "self-test failed"
  echo "smoke: the planted cases behave: a wrong golden line fails the step, a right one passes"
}

case "${1:-}" in
  --self-test) self_test ;;
  "")
    self_test
    for example in "${EXAMPLES[@]}"; do run_example "$example"; done
    ;;
  *)
    for example in "$@"; do run_example "$example"; done
    ;;
esac
