#!/usr/bin/env bash
# The check every PR into next must pass. CI runs exactly this.
#
# The steps run in three lanes at once, and each lane runs its steps one after another:
#   host:    the cargo steps that build for this machine. They stay in one lane because some build
#            the same binary with different features (the daemon with and without the planner or
#            workers), and a test running one must not see another step replace it.
#   cross:   the steps that build only for other targets (bare metal, WebAssembly). Cargo locks each
#            target's build directory, so they never wait on the host lane's.
#   scripts: the checks that read the tree and build nothing.
# A lane stops at its first failed step, as the gate always has; the other lanes go on. Each lane's
# output is printed whole, in lane order, and the gate fails if any lane failed. `scripts/ci.sh <step>` still runs one step by name.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# The gate runs every test binary, whatever a caller's scripts/check.sh setting says.
unset DAEDALUS_TEST_PACKAGES

host=(clippy test budgets chip doc no-planner no-workers package packaging smoke semver)
cross=(boundaries nostd wasm)
scripts=(format capability gpu-async-blocking gpu-driver-lock tools)
trailing=(scripts/check-file-sizes.sh scripts/check-workspace-deps.sh testing/boundary/run.sh)

# A step in scripts/ci.d that no lane names is a mistake: it would never run.
for file in scripts/ci.d/*.sh; do
  name="$(basename "$file" .sh)"
  if [[ " ${host[*]} ${cross[*]} ${scripts[*]} " != *" $name "* ]]; then
    echo "gate: scripts/ci.d/$name.sh is in no lane of scripts/gate.sh" >&2
    exit 2
  fi
done

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
    "$script" || status=1
    echo "    $(basename "$script"): $((SECONDS - start)) s"
  done
  return $status
}

start=$SECONDS
scripts/ci.sh "${host[@]}" > "$logs/host" 2>&1 &
host_pid=$!
running+=("$host_pid")
scripts/ci.sh "${cross[@]}" > "$logs/cross" 2>&1 &
cross_pid=$!
running+=("$cross_pid")
scripts_lane > "$logs/scripts" 2>&1 &
scripts_pid=$!
running+=("$scripts_pid")
set +m

failed=()
wait "$host_pid" || failed+=(host)
running=("$cross_pid" "$scripts_pid")
wait "$cross_pid" || failed+=(cross)
running=("$scripts_pid")
wait "$scripts_pid" || failed+=(scripts)
running=()

for lane in host cross scripts; do
  echo "=== lane: $lane"
  cat "$logs/$lane"
done
echo "gate: $((SECONDS - start)) s"
if ((${#failed[@]})); then
  echo "gate: failed in lane: ${failed[*]}" >&2
  exit 1
fi
