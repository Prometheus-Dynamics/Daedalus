#!/usr/bin/env bash
# The gate on a planted tree: its own scripts/gate.sh and scripts/ci.sh, with every step and
# trailing script a stub that passes, fails, or leaves a mark, run as the gate runs them. Around
# them, scripts/bounded.py runs the step in its place and scripts/seed-target.py makes the build
# directory: what those two do is their own self-tests', and the hundreds of starts of the real ones
# cost this test most of its CPU, so that under a loaded gate it ran past the tools step's bound. A red
# step anywhere in a lane fails the gate, names the lane, stops the lane's later steps, and leaves
# no `Gate-Lanes:` line, the record of a pass. Then the same checks against a gate planted with
# each way of losing a failure (a lane's status not read, a lane of steps returning only its last
# step's status, a trailing script's status dropped): each must be caught, so this test has teeth.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
failures=0

plant() { # TREE GATE_SH: a tree of stub steps around the given gate.sh and the real ci.sh
  local tree="$1" name
  mkdir -p "$tree/scripts/ci.d" "$tree/testing/boundary"
  cp "$2" "$tree/scripts/gate.sh"
  cp scripts/ci.sh "$tree/scripts/"
  # Called as the gate calls them: `bounded.py --step NAME SCRIPT`, `seed-target.py root`.
  printf 'import os, sys\nos.execv(sys.argv[3], [sys.argv[3]])\n' > "$tree/scripts/bounded.py"
  printf 'import os\nos.makedirs(os.environ.get("CARGO_TARGET_DIR") or "target", exist_ok=True)\n' \
    > "$tree/scripts/seed-target.py"
  printf '#!/usr/bin/env bash\nexit 0\n' > "$tree/scripts/fixture-locks.sh"
  printf '#!/usr/bin/env bash\nexit 1\n' > "$tree/scripts/lavapipe-icd.sh"
  for name in $(sed -nE '/^(lanes|trailing)=/d; s/^[a-z]+=\(([a-z -]+)\)$/\1/p' "$tree/scripts/gate.sh"); do
    printf '#!/usr/bin/env bash\n# bound: 30 s (planted)\ntouch "%s/ran-%s"\nexit "${FAIL_%s:-0}"\n' \
      "$tree" "$name" "${name//-/_}" > "$tree/scripts/ci.d/$name.sh"
  done
  # The budget table left to CI, as testing/crates/budget-harness/check.py leaves it.
  printf '[ -z "${PLANT_CI_ONLY:-}" ] || echo "not-run(ci-only:release+callgrind+sizes)" > "$GATE_LANE_STATE_DIR/budgets.status"\n' \
    | sed -i '2r /dev/stdin' "$tree/scripts/ci.d/budgets.sh"
  # A step reused, as scripts/step-reuse.py marks it.
  printf '[ -z "${PLANT_CI_ONLY:-}" ] || echo "passed(reused:0123456789ab)" > "$GATE_LANE_STATE_DIR/nostd.status"\n' \
    | sed -i '2r /dev/stdin' "$tree/scripts/ci.d/nostd.sh"
  for name in scripts/check-file-sizes.sh scripts/check-workspace-deps.sh testing/boundary/run.sh; do
    printf '#!/usr/bin/env bash\n# bound: 30 s (planted)\nexit "${FAIL_TRAILING:-0}"\n' > "$tree/$name"
  done
  chmod +x "$tree"/scripts/*.sh "$tree"/scripts/ci.d/*.sh "$tree/testing/boundary/run.sh"
}

# One run of the planted gate: EXPECT is `pass`, or `fail` naming LANE, and NOT_RAN, if given, is
# the step after the red one in its lane, which must not have run.
case_in() { # TREE EXPECT LANE NOT_RAN [VAR=VALUE...]
  local tree="$1" expect="$2" lane="$3" not_ran="$4"; shift 4
  rm -f "$tree"/ran-*
  local out code=0
  out="$(cd "$tree" && env -u DAEDALUS_TARGET_SEED -u GATE_LANE_STATE_DIR CARGO_TARGET_DIR="$tree/target" \
    "$@" bash scripts/gate.sh 2>&1)" || code=$?
  local ok=1
  if [[ $expect == pass ]]; then
    [[ $code == 0 ]] && grep -q '^Gate-Lanes: host=passed' <<< "$out" || ok=0
    # Every step's line, and the gate's, give wall and CPU seconds: scripts/dev/land.sh reads them.
    [[ $(grep -cE '^    [a-z0-9.-]+: [0-9]+ s, [0-9]+ cpu-s$' <<< "$out") == "$steps" ]] || ok=0
    grep -qE '^gate: [0-9]+ s, [0-9]+ cpu-s$' <<< "$out" || ok=0
  else
    [[ $code != 0 ]] || ok=0
    grep -q "^gate: failed in lane: .*\b$lane\b" <<< "$out" || ok=0
    ! grep -q '^Gate-Lanes:' <<< "$out" || ok=0
    [[ -z $not_ran || ! -e "$tree/ran-$not_ran" ]] || ok=0
  fi
  echo $ok
}

check() { # LABEL GATE_SH EXPECT_ALL_CAUGHT
  local tree="$work/$1" caught=1 got steps
  plant "$tree" "$2"
  steps=$(( $(ls "$tree/scripts/ci.d" | wc -l) + 3 ))
  for spec in "pass||" "fail|host|test|FAIL_clippy=1" "fail|scripts||FAIL_TRAILING=1" \
              "fail|host||FAIL_vvl_wgpu=1" "fail|ship|smoke|FAIL_packaging=1"; do
    IFS='|' read -r expect lane not_ran setting <<< "$spec"
    got="$(case_in "$tree" "$expect" "$lane" "$not_ran" ${setting:+"$setting"})"
    [[ $got == 1 ]] && continue
    caught=0
    # A planted bug needs one case to see it; the real gate is checked against every case.
    [[ $3 == 1 ]] || break
    echo "gate self-test: ${setting:-all steps passing}: not $expect${lane:+ in $lane}" >&2
  done
  echo $caught
}

if [[ "$(check real scripts/gate.sh 1)" == 1 ]]; then
  echo "gate self-test: a red step, first or last in its lane, or a red trailing script fails the gate, names its lane, stops the lane, and records no pass"
else
  failures=1
fi

# A budget table the gate left to CI, and a step it reused, are on the Gate-Lanes line, where
# scripts/release-check.sh reads the first: a gate that drops it would let a release through unmeasured. So must a planted gate
# that drops it be caught.
ci_only_named() { # LABEL GATE_SH: whether the line names it
  local tree="$work/ci-only-$1" out
  plant "$tree" "$2"
  out="$(cd "$tree" && env -u DAEDALUS_TARGET_SEED -u GATE_LANE_STATE_DIR CARGO_TARGET_DIR="$tree/target" \
    PLANT_CI_ONLY=1 bash scripts/gate.sh 2>&1)"
  grep -qE '^Gate-Lanes: host=passed .* budgets=not-run\(ci-only:release\+callgrind\+sizes\)( |$)' <<< "$out" &&
    grep -qE '^Gate-Lanes: host=passed .* nostd=passed\(reused:0123456789ab\)( |$)' <<< "$out"
}
sed -E '/line\+=" \$name=/d' scripts/gate.sh > "$work/drops-budgets.sh"
if ! ci_only_named real scripts/gate.sh; then
  echo "gate self-test: a budget table left to CI is missing from the Gate-Lanes line" >&2
  failures=1
elif cmp -s "$work/drops-budgets.sh" scripts/gate.sh || ci_only_named dropped "$work/drops-budgets.sh"; then
  echo "gate self-test: a gate that drops the budget table's ci-only status was not caught" >&2
  failures=1
else
  echo "gate self-test: a budget table left to CI and a reused step are named on the Gate-Lanes line; a gate that drops them is caught"
fi

# Each way of losing a failure, planted in a copy of the gate: the cases above must see it.
mutant() { # LABEL SED_EXPR
  local copy="$work/$1.sh"
  sed -E "$2" scripts/gate.sh > "$copy"
  if cmp -s "$copy" scripts/gate.sh; then
    echo "gate self-test: the planted bug '$1' no longer applies to scripts/gate.sh: update it" >&2
    failures=1
  elif [[ "$(check "$1" "$copy" 0)" == 1 ]]; then
    echo "gate self-test: a gate that $1 passed every case: the cases have no teeth" >&2
    failures=1
  else
    echo "gate self-test: a gate that $1 is caught"
  fi
}
mutant "ignores a lane's status" 's/wait "\$\{pids\[\$i\]\}" \|\| failed\+=\("\$\{lanes\[\$i\]\}"\)/wait "${pids[$i]}" || true/'
mutant "keeps only a lane's last step's status" 's/scripts\/ci.sh "\$\{!steps\}"/for s in "${!steps}"; do scripts\/ci.sh "$s"; done/'
mutant "drops a trailing script's status" 's/"\$script" \|\| status=1/"$script" || true/'
exit $failures
