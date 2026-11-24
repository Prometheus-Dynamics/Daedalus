#!/usr/bin/env bash
# bound: 375 s (three times its slowest green run, 125 s)
# Checks the crates a program with no operating system uses, for chips with and without
# compare-and-swap, then the example that puts them together.
set -euo pipefail
# Its lane's build directory: no other lane builds there (scripts/gate.sh).
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}/cross"

readonly TARGETS=(thumbv7em-none-eabihf thumbv6m-none-eabi riscv32imac-unknown-none-elf)
readonly CRATES=(daedalus-abi daedalus-types daedalus-value daedalus-graph daedalus-plan daedalus-runner daedalus-cpu)

for target in "${TARGETS[@]}"; do
  for crate in "${CRATES[@]}"; do
    cargo check --locked -q --target "$target" -p "$crate" --no-default-features
  done
done

# The example uses the host, whose values are reference counted. A chip with no compare-and-swap
# has no `Arc`, and runs the heap-free forms in chip.sh instead.
for target in thumbv7em-none-eabihf riscv32imac-unknown-none-elf; do
  cargo check --locked -q --target "$target" -p daedalus-nostd-smoke
done
