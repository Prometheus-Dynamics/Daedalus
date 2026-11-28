#!/usr/bin/env bash
# The seed's upkeep, run by the idle timer dd-reseed (scripts/dev/setup.sh --timer) from wt/seeder.
#
# When next's seed key (scripts/seed-target.py: the lock, the toolchain, cargo's configuration, the
# profiles, rustc) is not the seed's, it runs the gate from an empty build directory, saves its
# dependency builds as the seed, and removes the build directory. It never runs while a landing
# (a dd-land-* unit) does, and waits for one to end. It also keeps the gate clone's build
# directory under twice one gate's size: past it, outside a landing, the directory goes, and the
# next landing starts from the seed.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
source scripts/dev/env.sh
# The homes the gate builds in (scripts/dev/cargo-home.sh): the seed's key holds their paths.
export CARGO_HOME="$DAEDALUS_CARGO_HOME" GATE_LANE_HOMES="$DAEDALUS_CARGO_HOME.lanes"
landing() { systemctl --user list-units 'dd-land-*' --state=active --no-legend | command grep -q .; }
seed="$DAEDALUS_TARGET_SEED"
size_file="$seed.one-gate"
gate_dir="$DAEDALUS_WORK/target/gate"
if ! landing && [[ -f "$size_file" && -d "$gate_dir" ]]; then
  used=$(du -sk "$gate_dir" | cut -f1)
  if (( used > 2 * $(cat "$size_file") )); then
    rm -rf "$gate_dir" && echo "reseed: the gate clone's build directory was over twice one gate; removed"
  fi
fi
if scripts/seed-target.py stale "$seed"; then
  exit 0
fi
while landing; do sleep 60; done
# In the gate's own cargo homes, as land.sh gates: a dependency's source path is in what it builds.
scripts/dev/cargo-home.sh host docs chips ship beside cross scripts > "$DAEDALUS_LOGS/fetch-reseed.log" 2>&1 \
  || { echo "reseed: the fetch failed; the seed is unchanged (log $DAEDALUS_LOGS/fetch-reseed.log)"; exit 1; }
rm -rf "$CARGO_TARGET_DIR"
unset DAEDALUS_TARGET_SEED
# Every step builds everything: a seed is what one whole run built from empty
# (scripts/seed-target.py save refuses any other), so nothing is reused.
if GATE_LANE_REUSE=0 taskset -c "$LAND_CPUS" ionice -c3 nice -n"$NICE" scripts/gate.sh \
    > "$DAEDALUS_LOGS/reseed.log" 2>&1; then
  du -sk "$CARGO_TARGET_DIR" | cut -f1 > "$size_file"
  scripts/seed-target.py save "$seed" && echo "reseed: seed saved"
else
  echo "reseed: the gate failed on next; the seed is unchanged (log $DAEDALUS_LOGS/reseed.log)"
fi
rm -rf "$CARGO_TARGET_DIR"
