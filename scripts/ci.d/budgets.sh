#!/usr/bin/env bash
# bound: 1185 s (three times its slowest green run, 395 s, in a cold build on a fresh slot)
# The budget table: instruction counts under callgrind, allocations per tick, and firmware
# section sizes, each against its limit in testing/budgets/budgets.toml, and the planted
# regressions that must fail. A row that needs valgrind is not run where valgrind is not
# installed. The budgets workflow installs it and sets BUDGETS_REQUIRE_ALL=1.
set -euo pipefail
# Its lane's build directory: no other lane builds there (scripts/gate.sh).
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}/variants"
# In the local gate, what this build directory never measured is left to the budgets workflow
# (check.py says when); CI measures every row.
[[ -n ${CI:-} ]] || export BUDGETS_CI_ONLY=1
exec python3 testing/crates/budget-harness/check.py "$@"
