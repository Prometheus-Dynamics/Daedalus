#!/bin/sh
# E14: dma-buf buffers on V3D, the buffer half only. The producer-frame tests (tiled, UIF or SAND,
# XRGB, NV12) are dropped: Daedalus has no camera, image or frame-layout code. Kept: eight
# dma-heap buffers imported in rotation for 1000 imports, and the device-side wait on a buffer V3D
# is still writing (the dma-wait lines of the E9 section, run again here).
F=$(cd "$(dirname "$0")" && pwd)
. "$F/common.sh"
fig_begin e14
step e14-steady "$B/daedalus-gpu-budgets" dma-steady
step e14-wait "$B/daedalus-gpu-budgets" dma-fence
gate "open: run"
