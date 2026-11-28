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
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# The self-tests run side by side: each plants in a directory of its own. Each one's output is kept
# whole in a file of its own and printed in the order they were started, once all have ended;
# each that failed is named. `started NAME COMMAND...` starts one; `ended` waits for them all.
names=() pids=()
started() {
  local name=$1
  shift
  names+=("$name")
  "$@" > "$work/out-${#names[@]}" 2>&1 &
  pids+=($!)
}
ended() {
  local i failed=()
  for i in "${!names[@]}"; do
    wait "${pids[$i]}" || failed+=("${names[$i]}")
    cat "$work/out-$((i + 1))"
  done
  names=() pids=()
  if [[ ${#failed[@]} != 0 ]]; then
    echo "tools: failed: ${failed[*]}" >&2
    return 1
  fi
}

if [[ "${1:-}" == --self-test ]]; then
  # Three planted self-tests that end in the opposite order, two of them red: the output is each
  # one's whole, in the order started, and both red ones are named. Each waits until all three
  # have started, so run one after another none would see the others.
  status=0
  all="[ -e $work/m-first ] && [ -e $work/m-second ] && [ -e $work/m-third ]"
  meet="for _ in \$(seq 100); do $all && break; sleep 0.1; done; $all || { echo alone; exit 9; }"
  started first sh -c "touch $work/m-first; $meet; sleep 0.4; echo first-a; echo first-b"
  started second sh -c "touch $work/m-second; $meet; sleep 0.2; echo second-a; exit 3"
  started third sh -c "touch $work/m-third; $meet; echo third-a; exit 1"
  ended > "$work/said" 2>&1 && status=1
  said="$(cat "$work/said")"
  want=$'first-a\nfirst-b\nsecond-a\nthird-a\ntools: failed: second third'
  if [[ $status != 0 || "$said" != "$want" ]]; then
    echo "tools self-test: side by side, said:" >&2
    echo "$said" >&2
    exit 1
  fi
  echo "tools self-test: side by side, each output whole and in order, each red one named"
  exit 0
fi

scripts/ci.d/tools.sh --self-test
started affected python3 -I scripts/affected.py --self-test
started prune-targets python3 -I scripts/prune-targets.py --self-test
started seed-target python3 -I scripts/seed-target.py --self-test
started fixture-locks scripts/fixture-locks.sh --self-test
started boundaries python3 -I scripts/boundaries.py --self-test
started pick-binaries python3 -I scripts/board/pick-binaries.py --self-test
started package scripts/ci.d/package.sh --self-test
started tools-reuse python3 -I scripts/tools-reuse.py --self-test
# The longer self-tests, each run only when what it reads changed (scripts/tools-reuse.py STAMPS).
# bounded's measures how long a child waited for a CPU under a quota: it runs alone, after the rest.
stamps="$work/stamps"
python3 -I scripts/tools-reuse.py plan "$stamps"
alone=()
while IFS=$'\t' read -r name _; do
  if [[ $name == bounded ]]; then
    alone+=("$name")
  else
    started "$name" python3 -I scripts/tools-reuse.py run "$stamps" "$name"
  fi
done < "$stamps"
ended
for name in "${alone[@]}"; do
  started "$name" python3 -I scripts/tools-reuse.py run "$stamps" "$name"
  ended
done
# A build directory another checkout built in is refused before anything builds.
planted="$work/planted"
mkdir "$planted"
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
