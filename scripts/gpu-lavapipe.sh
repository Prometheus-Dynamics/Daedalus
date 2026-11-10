#!/usr/bin/env bash
# The lavapipe lane: the GPU backend's tests, its conformance cases and the counted GPU budgets,
# on Mesa's software Vulkan, under the Vulkan validation layer.
#
# It fails when: no lavapipe is installed, the device that opens isn't lavapipe, a test fails,
# no GPU test ran, or the validation layer said anything that isn't in
# scripts/gpu-lavapipe.known. Unit tests skip without an adapter everywhere else; here
# DAEDALUS_GPU_REQUIRED makes that a failure, so a lane that skips everything can't pass.
#
# The validation layer is used when it is installed. With DAEDALUS_VALIDATION_REQUIRED=1, as
# in CI, its absence is a failure.
#
#   scripts/gpu-lavapipe.sh              run the lane
#   scripts/gpu-lavapipe.sh --self-test  check that the lane's own checks catch what they should
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

known=scripts/gpu-lavapipe.known

# Prints the validation messages in the log $1 that the known list doesn't cover.
unknown_messages() {
  local patterns
  patterns=$(grep -v -e '^#' -e '^[[:space:]]*$' "$known" || true)
  grep -E 'Validation (Error|Warning|Performance Warning)|VUID-|UNASSIGNED-' "$1" |
    { if [[ -n "$patterns" ]]; then grep -v -F -e "$patterns"; else cat; fi; } || true
}

# The number of tests that passed, from cargo's output in $1.
passed_tests() {
  awk '/^test result: ok\./ { total += $4 } END { print total + 0 }' "$1"
}

self_test() {
  local dir failed=0
  dir=$(mktemp -d)
  printf '%s\n' \
    'Validation Error: [ VUID-vkCmdDispatch-None-08114 ] Object 0: planted | a planted error' \
    > "$dir/planted.log"
  if [[ -z "$(unknown_messages "$dir/planted.log")" ]]; then
    echo "self-test: a planted validation error was not caught" >&2
    failed=1
  fi
  : > "$dir/clean.log"
  if [[ -n "$(unknown_messages "$dir/clean.log")" ]]; then
    echo "self-test: an empty log was reported" >&2
    failed=1
  fi
  printf 'test result: ok. 0 passed; 0 failed; 0 ignored\n' > "$dir/none.out"
  printf 'test result: ok. 3 passed; 0 failed\ntest result: ok. 4 passed; 0 failed\n' > "$dir/some.out"
  if [[ "$(passed_tests "$dir/none.out")" != 0 || "$(passed_tests "$dir/some.out")" != 7 ]]; then
    echo "self-test: passed tests were miscounted" >&2
    failed=1
  fi
  rm -rf "$dir"
  [[ $failed == 0 ]] && echo "gpu-lavapipe self-test: ok"
  return $failed
}

if [[ "${1:-}" == "--self-test" ]]; then
  self_test
  exit
fi

# lavapipe, and nothing else: the driver list is this one file.
icd=""
for candidate in /usr/share/vulkan/icd.d/lvp_icd.*.json /usr/share/vulkan/icd.d/lvp_icd.json \
  /etc/vulkan/icd.d/lvp_icd.*.json; do
  if [[ -f "$candidate" && "$candidate" != *i686* ]]; then
    icd="$candidate"
    break
  fi
done
if [[ -z "$icd" ]]; then
  echo "gpu-lavapipe: no lavapipe driver is installed (mesa-vulkan-drivers)" >&2
  exit 1
fi
export VK_DRIVER_FILES="$icd" VK_ICD_FILENAMES="$icd"
export DAEDALUS_GPU_REQUIRED=1

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

layer=""
for dir in /usr/share/vulkan/explicit_layer.d /etc/vulkan/explicit_layer.d \
  /usr/local/share/vulkan/explicit_layer.d; do
  if compgen -G "$dir/VkLayer_khronos_validation*.json" > /dev/null; then
    layer="$dir"
    break
  fi
done
log="$work/validation.log"
if [[ -n "$layer" ]]; then
  export VK_INSTANCE_LAYERS=VK_LAYER_KHRONOS_validation
  export VK_KHRONOS_VALIDATION_DEBUG_ACTION=VK_DBG_LAYER_ACTION_LOG_MSG
  export VK_KHRONOS_VALIDATION_LOG_FILENAME="$log"
  export VK_KHRONOS_VALIDATION_REPORT_FLAGS=error,warn
elif [[ "${DAEDALUS_VALIDATION_REQUIRED:-}" == 1 ]]; then
  echo "gpu-lavapipe: the Vulkan validation layer isn't installed (vulkan-validationlayers)" >&2
  exit 1
else
  echo "gpu-lavapipe: no validation layer here, so validation is not checked"
fi

adapter=$(cargo run -q -p daedalus-gpu-budgets -- limits | sed -n 's/^adapter: //p')
case "$adapter" in
  *llvmpipe* | *lavapipe*) echo "gpu-lavapipe: on $adapter" ;;
  *)
    echo "gpu-lavapipe: the device that opened is \`$adapter\`, not lavapipe" >&2
    exit 1
    ;;
esac

packages=()
for manifest in backends/wgpu*/Cargo.toml backends/vk-sys/Cargo.toml \
  testing/crates/wgpu-conformance/Cargo.toml testing/crates/gpu-budgets/Cargo.toml \
  examples/04_gpu/Cargo.toml; do
  packages+=(-p "$(sed -n 's/^name = "\(.*\)"/\1/p' "$manifest" | head -1)")
done
out="$work/tests.out"
cargo test "${packages[@]}" 2>&1 | tee "$out"
# And optimised: a test that reads memory it no longer owns can pass unoptimised and fail here,
# which is what boards run.
cargo test --release "${packages[@]}" 2>&1 | tee -a "$out"

# The GPU examples: each binary checks its own answer against the CPU's and exits 0, and has
# no way to skip.
for example in fragment mixed upload_download; do
  cargo run -q -p daedalus-example-gpu --bin "$example"
done

ran=$(passed_tests "$out")
if [[ "$ran" == 0 ]]; then
  echo "gpu-lavapipe: no GPU test ran" >&2
  exit 1
fi
echo "gpu-lavapipe: $ran tests passed"

if [[ -n "$layer" ]]; then
  if [[ ! -f "$log" ]]; then
    echo "gpu-lavapipe: the validation layer wrote no log, so nothing was checked" >&2
    exit 1
  fi
  unknown=$(unknown_messages "$log")
  if [[ -n "$unknown" ]]; then
    echo "gpu-lavapipe: validation messages that aren't in $known:" >&2
    echo "$unknown" | sort | uniq -c | head -40 >&2
    exit 1
  fi
  echo "gpu-lavapipe: validation is clean"
fi
