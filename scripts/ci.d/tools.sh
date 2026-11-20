#!/usr/bin/env bash
# bound: 81 s (three times its slowest green run, 27 s, in a cold build on a fresh slot)
# The working tools check themselves against planted cases: which packages a change reaches
# (scripts/check.sh runs those), which build directories may be deleted, what a seed keeps, that
# the boundaries scan blanks exactly what its one-character-at-a-time oracle did, and
# what the budget table's kept measurements are keyed by, and that a step or a test binary past its
# bound goes red, named, with nothing left running.
set -euo pipefail
python3 -I scripts/affected.py --self-test
python3 -I scripts/prune-targets.py --self-test
python3 -I scripts/seed-target.py --self-test
scripts/fixture-locks.sh --self-test
python3 -I testing/crates/budget-harness/record.py --self-test
python3 -I scripts/bounded.py --self-test
python3 -I scripts/boundaries.py --self-test
# A build directory another checkout built in is refused before anything builds.
planted="$(mktemp -d)"
trap 'rm -rf "$planted"' EXIT
echo /a/checkout/elsewhere > "$planted/.daedalus-checkout"
status=0
refused="$(CARGO_TARGET_DIR="$planted" scripts/ci.sh format 2>&1)" || status=$?
if [[ $status != 0 ]] && grep -q "belongs to the checkout /a/checkout/elsewhere" <<< "$refused" &&
  ! grep -q "^==> " <<< "$refused"; then
  echo "tools: a build directory of another checkout is refused"
else
  echo "tools: a build directory of another checkout was used" >&2
  exit 1
fi
