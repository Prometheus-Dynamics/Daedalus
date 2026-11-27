#!/usr/bin/env bash
# bound: 2508 s (three times its slowest green run, 836 s, in a cold build on a fresh slot)
# Every test binary `cargo test --workspace --all-targets` runs, run side by side: see
# scripts/test-parallel.py. Arguments go to cargo test, as `--features x`.
#
# In the gate (GATE_LANE_STATE_DIR set), a package whose tests passed before on exactly these
# inputs is not run again (scripts/lane-reuse.py): every binary is still built as the workspace
# builds it, and only the others' run. Those that pass are recorded, even when another fails.
# A package keyed by less than the whole tree (lane-reuse.py) has its tests run seeing only what
# its key holds of the checkout, and every unit it builds from must compile from only that (rustc's
# dep-info): a package that starts reading outside itself fails here, named, before any record.
set -euo pipefail
if [[ -z "${GATE_LANE_STATE_DIR:-}" ]]; then
  exec scripts/test-parallel.py "$@"
fi
state="$GATE_LANE_STATE_DIR/test"
mapfile -t names < <(scripts/lane-reuse.py members)
scripts/lane-reuse.py plan "$state" "${names[@]}" -- "$@"
runs="$(scripts/lane-reuse.py runs "$state" | tr '\n' ' ')"
status=0
if [[ -z "${runs// }" ]]; then
  echo "  every package's tests passed before on these inputs"
else
  rm -f "$state.passed" "$state.artifacts"
  scripts/lane-reuse.py visible > "$state.visible"
  DAEDALUS_TEST_PACKAGES="$runs" DAEDALUS_TEST_PASSED="$state.passed" \
    DAEDALUS_TEST_VISIBLE="$state.visible" DAEDALUS_TEST_ARTIFACTS="$state.artifacts" \
    scripts/test-parallel.py "$@" || status=$?
  if [[ ! -s "$state.artifacts" ]] || ! scripts/lane-reuse.py reads "$state.artifacts"; then
    echo "test: no package is recorded: what the units compiled from was not shown to be keyed" >&2
    exit 1
  fi
  [[ ! -s "$state.passed" ]] || xargs scripts/lane-reuse.py record "$state" < "$state.passed"
fi
[[ $status != 0 ]] || scripts/lane-reuse.py status "$state" > "$state.status"
exit $status
