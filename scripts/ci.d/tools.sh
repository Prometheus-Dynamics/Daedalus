#!/usr/bin/env bash
# bound: 276 s (three times 92 s: its slowest green run, 27 s, in a cold build on a fresh slot,
#   the 10 s the starved-test cases of scripts/bounded.py added (side by side now), the 35 s of the
#   reuse keys' self-test and the 20 s at load 60 of the gate's self-test; the longer self-tests
#   each run only when what they read changed, scripts/tools-reuse.py)
# The working tools check themselves against planted cases: which packages a change reaches
# (scripts/check.sh runs those), which build directories may be deleted, what a seed keeps, that
# the boundaries scan blanks exactly what its one-character-at-a-time oracle did, that a crate whose
# file listing warns or fails is named (scripts/ci.d/package.sh), and
# what the budget table's kept measurements are keyed by, what a test's and a step's reuse keys hold, and that a step or a test binary past its
# bound goes red, named, with nothing left running, and that a red step or trailing script fails
# its lane and the gate and records no pass, and that a release is refused while a budget table the
# gate left to CI has no green budgets job on its exact commit.
set -euo pipefail
python3 -I scripts/affected.py --self-test
python3 -I scripts/prune-targets.py --self-test
python3 -I scripts/seed-target.py --self-test
scripts/fixture-locks.sh --self-test
python3 -I scripts/boundaries.py --self-test
python3 -I scripts/board/pick-binaries.py --self-test
scripts/ci.d/package.sh --self-test
python3 -I scripts/tools-reuse.py --self-test
# The longer self-tests, each run only when what it reads changed (scripts/tools-reuse.py STAMPS).
stamps="$(mktemp)"
trap 'rm -f "$stamps"' EXIT
python3 -I scripts/tools-reuse.py plan "$stamps"
while IFS=$'\t' read -r name _; do
  python3 -I scripts/tools-reuse.py run "$stamps" "$name"
done < "$stamps"
# A build directory another checkout built in is refused before anything builds.
planted="$(mktemp -d)"
trap 'rm -rf "$planted" "$stamps"' EXIT
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
