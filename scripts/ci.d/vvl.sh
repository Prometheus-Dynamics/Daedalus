#!/usr/bin/env bash
# bound: 60 s (its slowest green run took 10 s; no bound is under a minute)
# The vk-sys tests on lavapipe under the pinned Khronos validation layer, one at a time so each
# message is that test's. scripts/vvl-run.py says how: the layer and its pin, the binaries the
# `test` step built, the planted call first, and "vvl: not run" when there is no layer.
set -euo pipefail
exec python3 -I scripts/vvl-run.py --step vvl --threads 1 daedalus-vk-sys:lib
