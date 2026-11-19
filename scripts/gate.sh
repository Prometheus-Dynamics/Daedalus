#!/usr/bin/env bash
# The check every PR into next must pass. CI runs exactly this.
#
# The steps run in four lanes at once, and each lane runs its steps one after another:
#   host:    the cargo steps that build for this machine into the build directory, and run what
#            they build.
#   beside:  the steps whose builds never replace a binary the host lane's tests run. The host and
#            the daemon built without the planner or workers build in a directory of their own
#            (`variants` in the build directory); the budget table builds release binaries, the
#            firmware, and the fixture the tests build with the same command.
#   cross:   the steps that build only for other targets (bare metal, WebAssembly). Cargo locks each
#            target's build directory, so they never wait on the host lane's.
#   scripts: the checks that read the tree and build nothing.
# A lane stops at its first failed step, as the gate always has; the other lanes go on. Each lane's
# output is printed whole, in lane order, and the gate fails if any lane failed. `scripts/ci.sh <step>` still runs one step by name.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# The gate runs every test binary, whatever a caller's scripts/check.sh setting says.
unset DAEDALUS_TEST_PACKAGES

host=(clippy test chip doc package packaging smoke semver)
beside=(no-planner no-workers budgets)
cross=(boundaries nostd wasm)
scripts=(format capability gpu-async-blocking gpu-driver-lock tools)
trailing=(scripts/check-file-sizes.sh scripts/check-workspace-deps.sh testing/boundary/run.sh)

# A step in scripts/ci.d that no lane names is a mistake: it would never run.
for file in scripts/ci.d/*.sh; do
  name="$(basename "$file" .sh)"
  if [[ " ${host[*]} ${beside[*]} ${cross[*]} ${scripts[*]} " != *" $name "* ]]; then
    echo "gate: scripts/ci.d/$name.sh is in no lane of scripts/gate.sh" >&2
    exit 2
  fi
done

# Seeded once here, before the lanes, rather than by each lane's first step.
if [[ -n "${DAEDALUS_TARGET_SEED:-}" ]]; then
  scripts/seed-target.py use "$DAEDALUS_TARGET_SEED"
fi
# The fixtures' lock files too: lanes that build a fixture and a lane that reads its lock run at once.
scripts/fixture-locks.sh

logs="$(mktemp -d)"
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

scripts_lane() {
  scripts/ci.sh "${scripts[@]}" || return 1
  local status=0 script start
  for script in "${trailing[@]}"; do
    echo "==> $script"
    start=$SECONDS
    python3 -I scripts/bounded.py --step "$(basename "$script")" "$script" || status=1
    echo "    $(basename "$script"): $((SECONDS - start)) s"
  done
  return $status
}

start=$SECONDS
lanes=(host beside cross scripts)
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
echo "gate: $((SECONDS - start)) s"
if ((${#failed[@]})); then
  echo "gate: failed in lane: ${failed[*]}" >&2
  exit 1
fi
