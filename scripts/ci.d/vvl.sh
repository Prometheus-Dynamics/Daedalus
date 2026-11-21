#!/usr/bin/env bash
# bound: 60 s (its slowest green run took 10 s; no bound is under a minute)
# The vk-sys tests on lavapipe under the pinned Khronos validation layer (scripts/vvl.pin), built by
# scripts/vvl-build.sh into $DAEDALUS_VVL_DIR. First the planted call, which the layer has to name,
# so a layer that isn't loaded can't pass; then every vk-sys test, and any validation line fails.
#
# With no layer it says "vvl: not run" and why, and fails: a gate that couldn't validate hasn't
# passed. GitHub's runners have no pinned layer, so there (CI=true) the same line is printed and
# the step passes; the lander's gate is the merge bar.
set -euo pipefail
source scripts/vvl.pin

not_run() {
  echo "vvl: not run: $1" >&2
  if [[ "${CI:-}" == true ]]; then
    echo "vvl: on a CI runner with no pinned layer; the lander's gate runs this step"
    exit 0
  fi
  exit 1
}

layer="${DAEDALUS_VVL_DIR:-}"
[[ -n "$layer" ]] || not_run "DAEDALUS_VVL_DIR is not set (scripts/vvl-build.sh --arch x86_64 --out DIR builds the layer)"
lib="$layer/libVkLayer_khronos_validation.so"
[[ -f "$lib" && -f "$layer/VkLayer_khronos_validation.json" ]] ||
  not_run "no layer in $layer (scripts/vvl-build.sh --arch x86_64 --out $layer builds it)"
have=$(sha256sum "$lib" | cut -d' ' -f1)
if [[ "$have" != "$VVL_SHA256_X86_64" ]]; then
  echo "vvl: the layer in $layer has sha256 $have, not the pinned ${VVL_SHA256_X86_64:-(none)}" >&2
  exit 1
fi
icd=$(scripts/lavapipe-icd.sh) || not_run "no lavapipe driver is installed (mesa-vulkan-drivers)"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export VK_DRIVER_FILES="$icd" VK_ICD_FILENAMES="$icd" VK_LAYER_PATH="$layer"
export VK_INSTANCE_LAYERS=VK_LAYER_KHRONOS_validation VK_LOADER_LAYERS_DISABLE='~implicit~'
export VK_KHRONOS_VALIDATION_DEBUG_ACTION=VK_DBG_LAYER_ACTION_LOG_MSG
export VK_KHRONOS_VALIDATION_REPORT_FLAGS=error,warn
export DAEDALUS_GPU_ADAPTERS=software DAEDALUS_GPU_REQUIRED=1
messages='Validation (Error|Warning|Performance Warning)|VUID-|SYNC-HAZARD|UNASSIGNED-'

# The vk-sys test binary the `test` step built: `cargo test --workspace --all-targets`, which it
# runs, is fresh then, and a `-p daedalus-vk-sys` build would unify features differently and
# build wgpu again.
bin=$(cargo test --workspace --all-targets --no-run --message-format=json 2> "$work/build.err" |
  python3 -I -c '
import json, sys
for line in sys.stdin:
    try:
        message = json.loads(line)
    except ValueError:
        continue
    target = message.get("target") or {}
    if (message.get("reason") == "compiler-artifact" and message.get("executable")
            and target.get("name") == "daedalus_vk_sys" and "lib" in target.get("kind", [])):
        print(message["executable"])
') || true
[[ -n "$bin" && -x "$bin" ]] ||
  { cat "$work/build.err" >&2; echo "vvl: no vk-sys test binary was built" >&2; exit 1; }

# The planted call: the layer is loaded and its messages reach the log.
VK_KHRONOS_VALIDATION_LOG_FILENAME="$work/planted.log" DAEDALUS_PLANT_VALIDATION_ERROR=1 \
  "$bin" planted > "$work/planted.out" 2>&1 ||
  { cat "$work/planted.out" >&2; echo "vvl: the planted run failed" >&2; exit 1; }
if ! grep -q "VUID-VkBufferCreateInfo-size-00912" "$work/planted.log" 2>/dev/null; then
  echo "vvl: the layer didn't name the planted call (VUID-VkBufferCreateInfo-size-00912)" >&2
  exit 1
fi

# Every test, one at a time so each message is that test's.
VK_KHRONOS_VALIDATION_LOG_FILENAME="$work/tests.log" \
  "$bin" --test-threads=1 > "$work/tests.out" 2>&1 ||
  { cat "$work/tests.out" >&2; echo "vvl: a vk-sys test failed" >&2; exit 1; }
found=$(grep -E "$messages" "$work/tests.log" "$work/tests.out" 2>/dev/null || true)
if [[ -n "$found" ]]; then
  echo "$found" | head -20 >&2
  echo "vvl: the layer reported $(echo "$found" | wc -l) validation lines" >&2
  exit 1
fi
passed=$(awk '/^test result: ok\./ { total += $4 } END { print total + 0 }' "$work/tests.out")
[[ "$passed" -gt 0 ]] || { echo "vvl: no vk-sys test ran" >&2; exit 1; }
echo "vvl: $passed vk-sys tests under $VVL_TAG, the planted call named, no validation line"
