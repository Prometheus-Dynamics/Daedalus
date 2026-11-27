#!/usr/bin/env bash
# bound: 375 s (three times its slowest green run, 125 s)
# Checks the crates a program with no operating system uses, for chips with and without
# compare-and-swap, then the example that puts them together.
set -euo pipefail
# Its lane's build directory: no other lane builds there (scripts/gate.sh).
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}/cross"

readonly TARGETS=(thumbv7em-none-eabihf thumbv6m-none-eabi riscv32imac-unknown-none-elf)
readonly CRATES=(daedalus-abi daedalus-types daedalus-value daedalus-graph daedalus-plan daedalus-runner daedalus-cpu)

# Passed before on exactly these inputs: not run again (scripts/step-reuse.py says what they are).
read -r how key < <(python3 -I scripts/step-reuse.py begin nostd "${CRATES[@]}" daedalus-nostd-smoke)
[[ $how == run || $how == reused ]] || { echo "nostd: scripts/step-reuse.py begin failed" >&2; exit 1; }
[[ $how == reused ]] && exit 0
# What each build compiled from, for the check at the end: cargo's messages, diagnostics as text.
artifacts="$(mktemp)"
trap 'rm -f "$artifacts"' EXIT
check() { cargo check --locked -q --message-format=json-render-diagnostics "$@" >> "$artifacts"; }

for target in "${TARGETS[@]}"; do
  for crate in "${CRATES[@]}"; do
    check --target "$target" -p "$crate" --no-default-features
  done
done

# The example uses the host, whose values are reference counted. A chip with no compare-and-swap
# has no `Arc`, and runs the heap-free forms in chip.sh instead.
for target in thumbv7em-none-eabihf riscv32imac-unknown-none-elf; do
  check --target "$target" -p daedalus-nostd-smoke
done
python3 -I scripts/step-reuse.py end nostd "$key" "$artifacts" "${CRATES[@]}" daedalus-nostd-smoke
