#!/bin/sh
# E13: raw Vulkan on V3D under the Khronos validation layer, with synchronization validation. It
# runs first. The planted call comes before anything else: the layer has to name its rule, or a
# clean log proves nothing, and then no GPU figure after this one runs.
# Pass: the planted run names a VUID- rule, and the clean runs (vk-sys, fragment, region,
# raw-submit) print no validation line.
F=$(cd "$(dirname "$0")" && pwd)
. "$F/common.sh"
fig_begin e13

if [ ! -e "$D/vvl/VkLayer_khronos_validation.json" ]; then
  if [ "${BATCH_REHEARSAL:-}" = 1 ]; then
    gate "skipped: rehearsal: no validation layer in the rehearsal bundle (set VVL_DIR_NATIVE for one)"
    note "skipped: rehearsal: no validation layer in the rehearsal bundle"
  else
    gate "closed: no validation layer in the bundle (VVL_DIR)"
    note "closed: no validation layer in the bundle"
  fi
  exit 0
fi

export VK_LAYER_PATH="$D/vvl" VK_INSTANCE_LAYERS=VK_LAYER_KHRONOS_validation
export VK_KHRONOS_VALIDATION_VALIDATE_SYNC=1 VK_LAYER_VALIDATE_SYNC=1

step_to "$R/e13-planted.txt" e13-planted env DAEDALUS_PLANT_VALIDATION_ERROR=1 \
  "$B/vk-sys.lib" planted --nocapture --test-threads=1

if grep -q 'VUID-' "$R/e13-planted.txt"; then
  gate "open: the layer named the planted call's rule"
  step e13-clean-vk-sys "$B/vk-sys.lib" --nocapture --test-threads=1
  step e13-clean-fragment "$B/wgpu.all" fragment:: --nocapture --test-threads=1
  step e13-clean-region "$B/wgpu.region" --nocapture --test-threads=1
  step e13-clean-raw-submit "$B/daedalus-gpu-budgets" raw-submit fragment-pass
else
  gate "closed: the planted call went unnamed (no VUID- line), so the layer is not reporting; the clean runs and every GPU figure after this one do not run"
  note "closed: the planted call went unnamed (no VUID- line in e13-planted.txt)"
fi
