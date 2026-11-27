#!/usr/bin/env bash
# The check every PR into next must pass. CI runs exactly this.
#
# The steps run in seven lanes at once, and each lane runs its steps one after another. No two lanes
# build in the same build directory, so no lane waits on another's lock:
#   host:    the cargo steps that build for this machine into the build directory, and run what
#            they build. vvl-wgpu runs the wgpu tests the test step built, under the validation
#            layer, so it runs here, after them.
#   docs:    the API documentation, built in `doc-build` in the build directory.
#   chips:   the firmware, at the size profile, in `chip` in the build directory.
#   ship:    what a release ships: the crates packaged, the daemon's packaging files, and the
#            examples run. Their `-p` builds have features of their own and would replace the
#            binaries the host lane's tests run, so packaging and smoke build in `ship` in the build
#            directory. Packaging a crate builds in `package` there already.
#   beside:  the steps whose builds never replace a binary the host lane's tests run. The host and
#            the daemon built without the planner or workers build in a directory of their own
#            (`variants` in the build directory); the budget table builds release binaries, the
#            firmware, and the fixture the tests build with the same command.
#   cross:   the steps that build only for other targets (bare metal, WebAssembly), in `cross` in
#            the build directory.
#   scripts: the checks that read the tree and build nothing.
# In the test step, a package whose tests passed before on exactly these inputs is not run again
# (scripts/lane-reuse.py); the gate's last line, `Gate-Lanes:`, says what each lane reused. On CI,
# or with GATE_LANE_REUSE=0, nothing is reused.
# A lane stops at its first failed step, as the gate always has; the other lanes go on. Each lane's
# output is printed whole, in lane order, and the gate fails if any lane failed. `scripts/ci.sh <step>` still runs one step by name.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# The gate runs every test binary, whatever a caller's scripts/check.sh setting says.
unset DAEDALUS_TEST_PACKAGES

host=(clippy test vvl vvl-wgpu)
chips=(chip)
docs=(doc)
ship=(package packaging smoke semver)
beside=(no-planner no-workers budgets)
cross=(boundaries nostd wasm)
scripts=(format capability gpu-async-blocking gpu-driver-lock tools)
trailing=(scripts/check-file-sizes.sh scripts/check-workspace-deps.sh testing/boundary/run.sh)

# A step in scripts/ci.d that no lane names is a mistake: it would never run.
for file in scripts/ci.d/*.sh; do
  name="$(basename "$file" .sh)"
  if [[ " ${host[*]} ${docs[*]} ${chips[*]} ${ship[*]} ${beside[*]} ${cross[*]} ${scripts[*]} " != *" $name "* ]]; then
    echo "gate: scripts/ci.d/$name.sh is in no lane of scripts/gate.sh" >&2
    exit 2
  fi
done

# Seeded or made once here, before the lanes, rather than by each lane's first step.
if [[ -n "${DAEDALUS_TARGET_SEED:-}" ]]; then
  scripts/seed-target.py use "$DAEDALUS_TARGET_SEED"
fi
python3 -I scripts/seed-target.py root
# The fixtures' lock files too: lanes that build a fixture and a lane that reads its lock run at once.
scripts/fixture-locks.sh

logs="$(mktemp -d)"
# What the lanes reused (scripts/lane-reuse.py): the test step keeps its plan here.
export GATE_LANE_STATE_DIR="$logs/state"
mkdir -p "$GATE_LANE_STATE_DIR"
# Each lane runs in a process group of its own, whose id is the lane's pid, captured as it starts.
# A gate that ends early (a signal, a timeout) stops the lanes it started that are still running,
# with everything they started, and nothing else.
running=()
stop_lanes() {
  local pid
  for pid in "${running[@]}"; do
    kill -TERM -- "-$pid" 2>/dev/null
  done
  rm -rf "$logs"
}
trap stop_lanes EXIT
trap 'exit 130' INT TERM
set -m

# As in scripts/ci.sh: the CPU seconds of the children this shell has waited for, by `times`.
cpu_seen=0
children_cpu() {
  local file="${TMPDIR:-/tmp}/gate-times.$BASHPID"
  times > "$file"
  cpu_seen="$(awk 'NR == 2 { for (i = 1; i <= 2; i++) { split($i, t, "m"); s += t[1] * 60 + t[2] } }
    END { printf "%d", s }' "$file")"
  rm -f "$file"
}

scripts_lane() {
  scripts/ci.sh "${scripts[@]}" || return 1
  local status=0 script start cpu
  for script in "${trailing[@]}"; do
    echo "==> $script"
    start=$SECONDS
    children_cpu; cpu=$cpu_seen
    python3 -I scripts/bounded.py --step "$(basename "$script")" "$script" || status=1
    children_cpu
    echo "    $(basename "$script"): $((SECONDS - start)) s, $((cpu_seen - cpu)) cpu-s"
  done
  return $status
}

start=$SECONDS
children_cpu; gate_cpu=$cpu_seen
lanes=(host docs chips ship beside cross scripts)
pids=()
for lane in "${lanes[@]}"; do
  if [[ $lane == scripts ]]; then
    scripts_lane > "$logs/$lane" 2>&1 &
  else
    steps="$lane[@]"
    scripts/ci.sh "${!steps}" > "$logs/$lane" 2>&1 &
  fi
  pids+=("$!")
  running+=("$!")
done
set +m

failed=()
for i in "${!lanes[@]}"; do
  wait "${pids[$i]}" || failed+=("${lanes[$i]}")
  running=("${pids[@]:i+1}")
done

for lane in "${lanes[@]}"; do
  echo "=== lane: $lane"
  cat "$logs/$lane"
done
children_cpu
echo "gate: $((SECONDS - start)) s, $((cpu_seen - gate_cpu)) cpu-s"
if ((${#failed[@]})); then
  echo "gate: failed in lane: ${failed[*]}" >&2
  exit 1
fi
# What each lane did, for the commit: `passed`, or `passed(K-of-N-reused)` for a lane whose test
# step reused packages that passed before on the same inputs.
line="Gate-Lanes:"
for lane in "${lanes[@]}"; do
  result=passed
  if [[ $lane == host && -s "$GATE_LANE_STATE_DIR/test.status" ]]; then
    result="$(cat "$GATE_LANE_STATE_DIR/test.status")"
  fi
  line+=" $lane=$result"
done
# Each step that says more than its lane's result: the budget table left to the budgets workflow
# (testing/crates/budget-harness/check.py; scripts/release-check.sh reads it), and a step that
# passed before on the same inputs (scripts/step-reuse.py), with its key.
for file in "$GATE_LANE_STATE_DIR"/*.status; do
  name="$(basename "$file" .status)"
  [[ $name == test || ! -s $file ]] && continue
  line+=" $name=$(cat "$file")"
done
echo "$line"
