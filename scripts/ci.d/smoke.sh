#!/usr/bin/env bash
# Builds and runs each example binary, which must exit 0, and runs each example's test, which
# checks what it printed against its golden lines.
set -euo pipefail

readonly EXAMPLES=(quickstart basics host metrics)

for example in "${EXAMPLES[@]}"; do
  package="daedalus-example-$example"
  cargo run --locked -q -p "$package" >/dev/null
  cargo test --locked -q -p "$package" >/dev/null
  echo "  $package: ran, and its output is the golden one"
done
