#!/bin/sh
# E9: the fence a dma-buf exports while V3D is writing it. A 1 MiB dma-heap buffer is imported
# into the device, a GPU write long enough to outlive its export runs, and the fence is exported
# right after the submit: a sync_file or not, unsignalled at export or not, the time to signal
# (median of 50 after 5), and a CPU read of the dma-buf afterwards.
F=$(cd "$(dirname "$0")" && pwd)
. "$F/common.sh"
fig_begin e9
step e9-fence "$B/daedalus-gpu-budgets" dma-fence
gate "open: run"
