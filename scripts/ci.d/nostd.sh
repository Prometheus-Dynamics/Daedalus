#!/usr/bin/env bash
# Checks the crates a program with no operating system uses, for chips with and without
# compare-and-swap, then the example that puts them together.
set -euo pipefail

readonly TARGETS=(thumbv7em-none-eabihf thumbv6m-none-eabi riscv32imac-unknown-none-elf)
readonly CRATES=(daedalus-abi daedalus-types daedalus-value daedalus-graph daedalus-plan daedalus-runner daedalus-cpu)

for target in "${TARGETS[@]}"; do
  for crate in "${CRATES[@]}"; do
    cargo check -q --target "$target" -p "$crate" --no-default-features
  done
done

# The example uses the host, whose values are reference counted. A chip with no compare-and-swap
# has no `Arc`, and runs the heap-free forms in chip.sh instead.
for target in thumbv7em-none-eabihf riscv32imac-unknown-none-elf; do
  cargo check -q --target "$target" -p daedalus-nostd-smoke
done
