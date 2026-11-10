#!/usr/bin/env bash
# Prints the wasm-bindgen version this workspace builds with, which the CLI has to match.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
cargo tree -q -p daedalus-wasm-bindgen-host --target wasm32-unknown-unknown -i wasm-bindgen --depth 0 |
  sed -n 's/^wasm-bindgen v\([0-9.]*\).*/\1/p' | head -1
