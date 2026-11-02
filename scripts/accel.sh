#!/usr/bin/env bash
# The acceleration inventory lane (`ci.sh accel`): run the kernel conformance tests on every
# CPU backend, record evidence, check the kernels' vector code, and render
# docs/backend-matrix.{md,csv}. Fails on a failing or regressed cell, or an unregistered SIMD
# file; `ACCEL_RELEASE=1` also fails on gaps and untested cells.
#
# Backends and runners:
#   x86_64 native (SSE2, and AVX2 when the CPU has it)
#   aarch64-unknown-linux-musl under qemu-aarch64-static (NEON)
#   armv7-unknown-linux-musleabihf with +neon under qemu-arm-static -cpu cortex-a7 (NEON32)
#   riscv64gc-unknown-linux-musl with +v under qemu-riscv64-static at VLEN 128, 256, 512 (RVV)
# Cross targets need zig linker wrappers in $ZIGCC_DIR (default ~/zigcc: aarch64-musl-cc,
# armv7-cc, riscv64-cc; see docs/testing.md) and qemu-user (`qemu-ARCH-static` or `qemu-ARCH`);
# a missing one is skipped with a note, and its cells stay untested. qemu must be 9.0 or newer:
# older qemu-user does not report Zvbc through riscv_hwprobe, so the CRC's rvv+zvbc cell falls
# back to untested.
set -euo pipefail
cd "$(dirname "$0")/.."

ZIGCC_DIR=${ZIGCC_DIR:-$HOME/zigcc}
TARGET_DIR=${CARGO_TARGET_DIR:-target}
OUT=$TARGET_DIR/accel
RESULTS=$OUT/results
rm -rf "$OUT"
mkdir -p "$RESULTS"
ACCEL_RESULTS_DIR=$(realpath "$RESULTS")
export ACCEL_RESULTS_DIR
export ACCEL_REV=${ACCEL_REV:-$(git rev-parse --short HEAD)}

step() { echo "==> $*"; }

# llvm-objdump from the toolchain's llvm-tools component, for the asm checks.
if [[ -z ${OBJDUMP:-} ]]; then
  sysroot_objdump="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/^host: //p')/bin/llvm-objdump"
  if [[ -x $sysroot_objdump ]]; then
    export OBJDUMP=$sysroot_objdump
  fi
fi

cargo build -q -p daedalus-accel --features generator --bin accel-matrix
MATRIX=$TARGET_DIR/debug/accel-matrix
ACCEL_DUMP=$(realpath "$OUT")/daedalus.json

# The conformance test binary for a target, built (not run). `TOOLCHAIN=+nightly-...` selects
# another toolchain.
test_binary() {
  local target=$1
  cargo ${TOOLCHAIN:-} test -q -p daedalus-kernels --test conformance --target "$target" --no-run \
    --message-format=json 2>/dev/null |
    python3 -c 'import json,sys
for line in sys.stdin:
    m = json.loads(line)
    if m.get("reason") == "compiler-artifact" and m.get("executable") and m["target"]["name"] == "conformance":
        print(m["executable"])' | tail -n1
}

# run_target TRIPLE RUNNER_NAME RUNNER... : run the conformance tests and the asm check.
run_target() {
  local target=$1 runner_name=$2
  shift 2
  local var
  var=$(echo "$target" | tr 'a-z-' 'A-Z_')
  step "Conformance on $target ($runner_name)"
  if [[ $# -gt 0 ]]; then
    export "CARGO_TARGET_${var}_RUNNER=$*"
  fi
  ACCEL_TARGET=$target ACCEL_RUNNER=$runner_name ACCEL_DUMP=$ACCEL_DUMP \
    cargo test -q -p daedalus-kernels --test conformance --target "$target"
  local bin
  bin=$(test_binary "$target")
  "$MATRIX" asm --registry "$ACCEL_DUMP" --binary "$bin" --target "$target" --out "$RESULTS"
}

host=$(rustc -vV | sed -n 's/^host: //p')
run_target "$host" native

# GPU and fixed-function kernels: the GPU crate's conformance tests on this machine's Vulkan
# adapter (lavapipe in CI). Without an adapter they skip and their cells stay untested.
step "Fixed-function conformance (V4L2 mem2mem: vim2m here, rockchip-rga on Rockchip boards)"
ACCEL_TARGET=$host ACCEL_RUNNER=native \
  cargo test -q -p daedalus-rga --test v4l2

step "GPU conformance (wgpu adapter)"
ACCEL_TARGET=$host ACCEL_RUNNER=native ACCEL_REQUIRE_GPU=1 \
  cargo test -q -p daedalus-gpu --features gpu-dmabuf,gpu-vulkan-raw --lib -- storage_frames_copy_into_textures kernels_tests zero_copy raw_vulkan --test-threads 1
# A repeated kernel dispatch allocates no more than the same dispatch written in raw wgpu.
cargo test -q -p daedalus-gpu --features gpu-wgpu --test gpu_allocations

# The qemu-user binary for ARCH: `qemu-ARCH-static` (Ubuntu 24.04's qemu-user-static), else
# `qemu-ARCH` (Ubuntu 26.04's qemu-user-binfmt, where qemu-user-static is only a virtual package).
qemu_for() {
  local name
  for name in "qemu-$1-static" "qemu-$1"; do
    command -v "$name" >/dev/null && { echo "$name"; return; }
  done
  echo "qemu-$1-static"
}
QEMU_AARCH64=$(qemu_for aarch64)
QEMU_ARM=$(qemu_for arm)
QEMU_RISCV64=$(qemu_for riscv64)
command -v "$QEMU_RISCV64" >/dev/null && step "qemu-user: $("$QEMU_RISCV64" --version | head -1)"

# cross TRIPLE CC_WRAPPER QEMU: set up the linker; false (with a note) when a tool is missing.
cross() {
  local target=$1 cc=$2 qemu=$3
  if [[ ! -x $ZIGCC_DIR/$cc ]] || ! command -v "$qemu" >/dev/null; then
    echo "skipping $target: needs $ZIGCC_DIR/$cc and $qemu (its cells stay untested)" >&2
    return 1
  fi
  rustup target list --installed | grep -qx "$target" || rustup target add "$target"
  local var
  var=$(echo "$target" | tr 'a-z-' 'A-Z_')
  export "CARGO_TARGET_${var}_LINKER=$ZIGCC_DIR/$cc" "CC_$(echo "$target" | tr '-' '_')=$ZIGCC_DIR/$cc"
}

if cross aarch64-unknown-linux-musl aarch64-musl-cc "$QEMU_AARCH64"; then
  export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C link-self-contained=no"
  run_target aarch64-unknown-linux-musl qemu "$QEMU_AARCH64"
fi
if cross armv7-unknown-linux-musleabihf armv7-cc "$QEMU_ARM"; then
  export CARGO_TARGET_ARMV7_UNKNOWN_LINUX_MUSLEABIHF_RUSTFLAGS="-C link-self-contained=no -C target-feature=+neon"
  run_target armv7-unknown-linux-musleabihf qemu-cortex-a7 "$QEMU_ARM" -cpu cortex-a7
fi
if cross riscv64gc-unknown-linux-musl riscv64-cc "$QEMU_RISCV64"; then
  # Board-shaped `+v` builds; Zvbc (the CRC's vclmul kernel) is chosen at runtime through
  # riscv_hwprobe, so qemu offers it, and a last run without it checks the fallback.
  export CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C target-feature=+v"
  for vlen in 128 256 512; do
    run_target riscv64gc-unknown-linux-musl "qemu-vlen$vlen" \
      "$QEMU_RISCV64" -cpu "rv64,v=true,zvbc=true,vlen=$vlen"
  done
  run_target riscv64gc-unknown-linux-musl "qemu-vlen128-no-zvbc" \
    "$QEMU_RISCV64" -cpu "rv64,v=true,zvbc=false,vlen=128"
fi

# Portable SIMD (`core::simd`) needs nightly: the pinned toolchain builds the `portable` kernels
# with `--cfg daedalus_portable_simd`; the product stays on stable.
NIGHTLY=${ACCEL_NIGHTLY:-nightly-2026-10-15}
if rustup run "$NIGHTLY" rustc --version >/dev/null 2>&1; then
  step "Portable SIMD on $NIGHTLY ($host)"
  # A separate target dir: the cfg changes every crate's fingerprint.
  nightly_dir=$TARGET_DIR/nightly
  CARGO_TARGET_DIR=$nightly_dir RUSTFLAGS="--cfg daedalus_portable_simd" \
    ACCEL_TARGET=$host ACCEL_RUNNER="native-$NIGHTLY" \
    cargo "+$NIGHTLY" test -q -p daedalus-kernels --test conformance --target "$host" portable
  bin=$(CARGO_TARGET_DIR=$nightly_dir RUSTFLAGS="--cfg daedalus_portable_simd" TOOLCHAIN="+$NIGHTLY" \
    test_binary "$host")
  "$MATRIX" asm --registry "$ACCEL_DUMP" --binary "$bin" --target "$host" --out "$RESULTS" --portable
else
  echo "skipping portable SIMD: rustup toolchain $NIGHTLY is not installed (its cells stay untested)" >&2
fi

step "Rendering docs/backend-matrix.{md,csv}"
release=()
[[ ${ACCEL_RELEASE:-0} = 1 ]] && release=(--release)
# Board evidence CI cannot reproduce (the Raze, later Rockchip, K230D, Flame) is committed under
# docs/accel-evidence/.
"$MATRIX" render --registry "$ACCEL_DUMP" --results "$RESULTS" --results docs/accel-evidence \
  --baseline docs/backend-matrix.csv --scan . --out docs "${release[@]}"
