#!/usr/bin/env bash
# Checks the CPU backend for targets other than this machine's, and runs its tests under
# qemu-user with zig as the linker. It needs zig and qemu-user, so it has its own workflow job.
# GNU targets also need that target's libc at QEMU_LD_PREFIX.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

cargo check -q --target aarch64-unknown-linux-gnu -p daedalus-cpu

# rust target : zig target : qemu binary : libc prefix
readonly TARGETS=(
  "armv7-unknown-linux-musleabihf:armv7-linux-musleabihf:qemu-arm:"
  "riscv64gc-unknown-linux-musl:riscv64-linux-musl:qemu-riscv64:"
  "riscv64gc-unknown-linux-gnu:riscv64-linux-gnu:qemu-riscv64:/usr/riscv64-linux-gnu"
)

for entry in "${TARGETS[@]}"; do
  IFS=: read -r target zig qemu prefix <<<"$entry"
  upper=$(tr '[:lower:]-' '[:upper:]_' <<<"$target")
  echo "==> $target"
  cargo check -q --target "$target" -p daedalus-cpu
  env \
    ZIG_TARGET="$zig" \
    QEMU_LD_PREFIX="$prefix" \
    "CARGO_TARGET_${upper}_LINKER=$PWD/scripts/zigcc.sh" \
    "CARGO_TARGET_${upper}_RUNNER=$qemu" \
    cargo test -q --target "$target" -p daedalus-cpu
done
