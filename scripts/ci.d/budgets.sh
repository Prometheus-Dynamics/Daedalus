#!/usr/bin/env bash
# The budget table: instruction counts under callgrind, allocations per tick, and firmware
# section sizes, each against its limit in testing/budgets/budgets.toml, and the planted
# regressions that must fail. A row that needs valgrind is not run where valgrind is not
# installed. The budgets workflow installs it and sets BUDGETS_REQUIRE_ALL=1.
set -euo pipefail
exec python3 testing/crates/budget-harness/check.py "$@"
