#!/usr/bin/env bash
# bound: 636 s (three times its slowest green run, 212 s)
# The host and the daemon built without the planner: their normal dependency trees must not name
# daedalus-planner, the planner-less host's tests must pass, and a made plan must load and run in a
# daemon built without the planner as it runs in one built with it. The full build's test writes
# the plan the lean build loads, so the full run comes first.
#
#   scripts/ci.d/no-planner.sh              the checks below
#   scripts/ci.d/no-planner.sh --self-test  only the tree check: the default build must name the planner
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# These builds of the daemon would replace the one the test step's tests run, so they go in a
# build directory of their own, which lets this step run beside the test step.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}/variants"

fail() {
  echo "no-planner: $*" >&2
  exit 1
}

names_planner() { # cargo tree arguments: true when the normal tree names daedalus-planner
  cargo tree --locked -e normal "$@" | grep -q "daedalus-planner v"
}

# The check is only worth something if it can see the planner where it is linked.
names_planner -p daedalus-host --features listen || fail "self-test: the planner build does not name daedalus-planner"
names_planner -p daedalusd || fail "self-test: the default daemon does not name daedalus-planner"
[[ "${1:-}" == "--self-test" ]] && { echo "no-planner: tree self-test ok"; exit 0; }

names_planner -p daedalus-host --no-default-features --features listen &&
  fail "daedalus-host without the planner still links daedalus-planner"
names_planner -p daedalusd --no-default-features &&
  fail "daedalusd without the planner still links daedalus-planner"
echo "  trees: no daedalus-planner in the planner-less host or daemon"

cargo test --locked -q -p daedalus-host --no-default-features --features listen ||
  fail "the planner-less host's tests fail"
echo "  daedalus-host without the planner: its tests pass"

cargo test --locked -q -p daedalusd --test all -- made:: || fail "made plan with the planner: the daemon test fails"
cargo test --locked -q -p daedalusd --no-default-features --test all -- made:: ||
  fail "made plan without the planner: the daemon test fails"
echo "  daedalusd: a made plan runs the same with and without the planner"
