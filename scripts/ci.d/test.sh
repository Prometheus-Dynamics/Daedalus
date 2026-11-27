#!/usr/bin/env bash
# bound: 2508 s (three times its slowest green run, 836 s, in a cold build on a fresh slot)
# Every test binary `cargo test --workspace --all-targets` runs, run side by side: see
# scripts/test-parallel.py. Arguments go to cargo test, as `--features x`.
#
# In the gate (GATE_LANE_STATE_DIR set), every binary is built as the workspace builds it, then a
# package whose tests passed before on exactly what was built and what they read is not run again
# (scripts/lane-reuse.py plan): only the others' run. Those that pass are recorded, even when
# another fails. A package keyed by less than the whole tree (lane-reuse.py) has its tests run
# seeing only what its key holds of the checkout, and every unit it builds from must compile from
# only the paths it may see (rustc's dep-info): a package that starts reading outside itself fails
# here, named, before any record.
set -euo pipefail
if [[ -z "${GATE_LANE_STATE_DIR:-}" ]]; then
  exec scripts/test-parallel.py "$@"
fi
state="$GATE_LANE_STATE_DIR/test"
rm -f "$state" "$state.visible" "$state.passed" "$state.artifacts"
status=0
DAEDALUS_TEST_PLAN="$state" DAEDALUS_TEST_PASSED="$state.passed" \
  DAEDALUS_TEST_ARTIFACTS="$state.artifacts" scripts/test-parallel.py "$@" || status=$?
if [[ ! -s "$state" ]]; then
  echo "test: no package was keyed: the build failed before the plan" >&2
  exit 1
fi
[[ -n "$(scripts/lane-reuse.py runs "$state")" ]] || echo "  every package's tests passed before on these inputs"
if [[ -s "$state.passed" ]]; then
  if ! scripts/lane-reuse.py reads "$state.artifacts"; then
    echo "test: no package is recorded: what the units compiled from was not shown to be keyed" >&2
    exit 1
  fi
  xargs scripts/lane-reuse.py record "$state" < "$state.passed"
fi
[[ $status != 0 ]] || scripts/lane-reuse.py status "$state" > "$state.status"
exit $status
