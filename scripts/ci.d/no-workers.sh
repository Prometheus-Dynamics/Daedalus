#!/usr/bin/env bash
# bound: 174 s (three times its slowest green run, 58 s)
# The daemon built without `workers` links no process backend: its normal dependency tree must not
# name daedalus-backend-process, and its worker tests must pass there: a config that names workers
# is refused at --check-config, and a made plan that needs a worker is refused at bind. The
# `workers` build must name the backend, or the tree check checks nothing.
#
#   scripts/ci.d/no-workers.sh              the checks below
#   scripts/ci.d/no-workers.sh --self-test  only the tree check: the workers build must name the backend
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# These builds of the daemon would replace the one the test step's tests run, so they go in a
# build directory of their own, which lets this step run beside the test step.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}/variants"

fail() {
  echo "no-workers: $*" >&2
  exit 1
}

names_backend() { # cargo tree arguments: true when the normal tree names the process backend
  cargo tree --locked -e normal "$@" | grep -q "daedalus-backend-process v"
}

# The check is only worth something if it can see the process backend where it is linked.
names_backend -p daedalusd --features workers || fail "self-test: the workers build does not name daedalus-backend-process"
[[ "${1:-}" == "--self-test" ]] && { echo "no-workers: tree self-test ok"; exit 0; }

names_backend -p daedalusd --no-default-features &&
  fail "daedalusd without workers still links daedalus-backend-process"
echo "  trees: no daedalus-backend-process in the daemon built without workers"

# The full build's test writes the made plan that uses a worker, which the lean build refuses by name.
# So the full run comes first.
cargo test --locked -q -p daedalusd --features workers --test all -- workers:: ||
  fail "the daemon with workers: its worker tests fail"
cargo test --locked -q -p daedalusd --no-default-features --test all -- workers:: ||
  fail "the daemon without workers: its worker test fails"
echo "  daedalusd without workers: a config that names workers is refused, and a made plan that needs one is refused"
