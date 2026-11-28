#!/usr/bin/env bash
# Runs named steps. Each step is scripts/ci.d/<step>.sh, so PRs add steps without editing this file.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# Incremental builds are Cargo's profile default: on for the debug and test profiles, which a gate rebuilds after
# every edit, and off for release and the firmware profile. The steps whose result is the code
# itself (firmware sizes, instruction counts) build with it off whatever the caller's environment.

# Tests that open a GPU open the default adapter, and the stress tests open hundreds of devices on
# it. Where lavapipe is installed the steps get lavapipe, so a gate never loads the machine's own
# GPU; a caller that names its driver keeps it. The GPU lanes choose their device themselves.
if [[ -z "${VK_DRIVER_FILES:-}${VK_ICD_FILENAMES:-}" ]] && icd=$(scripts/lavapipe-icd.sh); then
  export VK_DRIVER_FILES="$icd" VK_ICD_FILENAMES="$icd"
fi

# Whatever is built names the cargo home's sources as /cargo/..., not as the builder's home
# (scripts/home-remap.sh).
source scripts/home-remap.sh
home_remap
# A build directory belongs to one checkout. Cargo would reuse what another checkout built there,
# and those builds carry that checkout's paths: its tests would read the other checkout's files,
# or files that are gone. The directory records its checkout, which scripts/prune-targets.py reads
# to tell when it is finished with; a directory with no record is never pruned.
# A build directory that doesn't exist yet can start from a seed of dependency builds.
if [[ -n "${DAEDALUS_TARGET_SEED:-}" ]]; then
  scripts/seed-target.py use "$DAEDALUS_TARGET_SEED"
fi
scripts/fixture-locks.sh
target="${CARGO_TARGET_DIR:-target}"
# Unseeded, it starts empty, in a btrfs subvolume of its own where it can (scripts/seed-target.py).
python3 -I scripts/seed-target.py root
owner="$(cat "$target/.daedalus-checkout" 2>/dev/null || true)"
if [[ -n "$owner" && "$owner" != "$PWD" ]]; then
  echo "ci: the build directory $target belongs to the checkout $owner; its builds carry that" >&2
  echo "ci: checkout's paths. Give this checkout a build directory of its own." >&2
  exit 2
fi
echo "$PWD" > "$target/.daedalus-checkout"

if [[ $# -eq 0 ]]; then
  set -- format clippy test boundaries
fi

# The CPU seconds of the children this shell has waited for (bash's `times`, second line), which
# hold each step's own and those of everything it waited for: cargo's rustc, the test binaries.
# `times` runs in this shell, not a subshell, where it would count only the subshell's children.
cpu_seen=0
children_cpu() {
  local file="${TMPDIR:-/tmp}/ci-times.$$"
  times > "$file"
  cpu_seen="$(awk 'NR == 2 { for (i = 1; i <= 2; i++) { split($i, t, "m"); s += t[1] * 60 + t[2] } }
    END { printf "%d", s }' "$file")"
  rm -f "$file"
}

for name in "$@"; do
  file="scripts/ci.d/$name.sh"
  if [[ ! -x "$file" ]]; then
    echo "no such step: $name" >&2
    exit 2
  fi
  echo "==> $name"
  start=$SECONDS
  children_cpu; cpu_start=$cpu_seen
  # Each step carries its bound beside its code, and runs in a process group of its own: past its
  # bound the group is stopped and the step fails, named; what it leaves running is stopped too.
  python3 -I scripts/bounded.py --step "$name" "$file"
  children_cpu
  echo "    $name: $((SECONDS - start)) s, $((cpu_seen - cpu_start)) cpu-s"
done

echo "passed: $*"
