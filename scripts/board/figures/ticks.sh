#!/bin/sh
# Ticks (D2 and I2): the CPU tick figures at the board's current commit, before E11. Three rounds
# of the budget harness's wall-clock runs, 10,000,000 ticks each: the 16-stage chain merged and
# unmerged, the 1-stage chain, and the 16-stage idle tick; each prints its nanoseconds a tick.
# Then criterion's medians from the ticks bench: idle_tick_16, fused_chain_16_tick,
# unfused_chain_16_tick and detector_frame. Criterion keeps its files under the bundle, not
# results/, so nothing is left on the board when the bundle is removed.
F=$(cd "$(dirname "$0")" && pwd)
. "$F/common.sh"
fig_begin ticks
ticks=${BATCH_TICKS:-10000000}
for round in 1 2 3; do
  step "ticks-merged-$round" "$B/daedalus-budget-harness" chain "$ticks" 16
  step "ticks-unmerged-$round" "$B/daedalus-budget-harness" chain "$ticks" 16 unfused
  step "ticks-one-$round" "$B/daedalus-budget-harness" chain "$ticks" 1
  step "ticks-idle-$round" "$B/daedalus-budget-harness" idle "$ticks" 16
done
step ticks-bench env CRITERION_HOME="$D/criterion" "$B/ticks-bench" --bench --noplot \
  '^(idle_tick_16|fused_chain_16_tick|unfused_chain_16_tick|detector_frame)$'
gate "open: run"
