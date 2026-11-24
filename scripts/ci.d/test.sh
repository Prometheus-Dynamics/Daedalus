#!/usr/bin/env bash
# bound: 2508 s (three times its slowest green run, 836 s, in a cold build on a fresh slot)
# Every test binary `cargo test --workspace --all-targets` runs, run side by side: see
# scripts/test-parallel.py. Arguments go to cargo test, as `--features x`.
#
# In the gate (GATE_LANE_STATE_DIR set), a package whose tests passed before on exactly these
# inputs is not run again (scripts/lane-reuse.py): every binary is still built as the workspace
# builds it, and only the others' run. Those that pass are recorded, even when another fails.
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
  rm -f "$state.passed"
  DAEDALUS_TEST_PACKAGES="$runs" DAEDALUS_TEST_PASSED="$state.passed" \
    scripts/test-parallel.py "$@" || status=$?
  [[ ! -s "$state.passed" ]] || xargs scripts/lane-reuse.py record "$state" < "$state.passed"
fi
[[ $status != 0 ]] || scripts/lane-reuse.py status "$state" > "$state.status"
exit $status
