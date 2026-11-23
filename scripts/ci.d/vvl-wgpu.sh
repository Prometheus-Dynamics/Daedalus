#!/usr/bin/env bash
# bound: 135 s (its slowest green run took 45 s, warm, beside a cross build)
# wait: 2508 s (the `test` step's build, whose build directory it waits for: that step's own bound)
# The wgpu backend's tests, every one that opens a device, on lavapipe under the pinned Khronos
# validation layer: where a synchronisation mistake would show next. Off the critical path, in the
# scripts lane, on the binaries the `test` step built (it waits for that build, then reuses it).
# scripts/vvl-run.py says how, as for the `vvl` step.
set -euo pipefail
exec python3 -I scripts/vvl-run.py --step vvl-wgpu daedalus-wgpu
