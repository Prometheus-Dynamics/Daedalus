#!/bin/sh
# E6: merged and unmerged element chains on V3D at 1280x800, 4 and 8 nodes, three rounds. Each
# round gives four forms: a merged fragment pass, one fragment pass per node, a merged compute
# pass, and one compute region per node, each with its submits and passes (the e6 lines).
# The box 3x3 comparison is dropped: the pixel vocabulary is out of Daedalus.
F=$(cd "$(dirname "$0")" && pwd)
. "$F/common.sh"
fig_begin e6
for round in 1 2 3; do
  step "e6-elements-$round" "$B/daedalus-gpu-budgets" elements
done
gate "open: run"
