#!/usr/bin/env bash
# bound: 174 s (three times its slowest green run, 58 s, in a cold build on a fresh slot)
# Checks the daemon's packaging files against the daemon: both units verify, the sample config
# is accepted, and the paths agree. Nothing is installed or started. The check's planted
# breakages must each fail (its --self-test), so the check is shown to catch what it claims.
set -euo pipefail
# Its `-p` builds of the daemon and the examples have features of their own, and would replace the
# binaries the test step's tests run, so they go in a build directory of their own, which lets
# this step run beside the test step (the ship lane of scripts/gate.sh).
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}/ship"
service/daedalusd/packaging/check.sh
service/daedalusd/packaging/check.sh --self-test
