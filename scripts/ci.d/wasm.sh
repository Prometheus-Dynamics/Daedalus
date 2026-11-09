#!/usr/bin/env bash
# Builds for WebAssembly without threads and runs the results under Node.
set -euo pipefail

out="${CARGO_TARGET_DIR:-target}"

cargo rustc -q --target wasm32-unknown-unknown -p daedalus-wasm-smoke --lib --crate-type cdylib
node scripts/wasm-smoke.mjs "$out/wasm32-unknown-unknown/debug/daedalus_wasm_smoke.wasm"

cargo build -q --target wasm32-wasip1 -p daedalus-wasm-smoke --bin daedalus-wasi-smoke
echo "wasi smoke: $(node --no-warnings scripts/wasi-smoke.mjs "$out/wasm32-wasip1/debug/daedalus-wasi-smoke.wasm")"

# The CLI has to match the library, or its output won't load.
locked=$(awk '/^name = "wasm-bindgen"$/ { getline; gsub(/[^0-9.]/, ""); print; exit }' Cargo.lock)
found=$(wasm-bindgen --version | awk '{ print $2 }')
if [[ "$locked" != "$found" ]]; then
  echo "wasm-bindgen CLI is $found, and Cargo.lock has $locked" >&2
  exit 1
fi
cargo rustc -q --target wasm32-unknown-unknown -p daedalus-wasm-bindgen-host --lib --crate-type cdylib
bindings=$(mktemp -d)
trap 'rm -rf "$bindings"' EXIT
wasm-bindgen --target nodejs --out-dir "$bindings" \
  "$out/wasm32-unknown-unknown/debug/daedalus_wasm_bindgen_host.wasm"
node scripts/wasm-bindgen-host.mjs "$bindings/daedalus_wasm_bindgen_host.js"

# Threads aren't there on this target, and asking for them must fail at compile time.
if failed=$(cargo check -q -p daedalus-runner --features threads --target wasm32-unknown-unknown 2>&1); then
  echo "daedalus-runner built with threads for wasm32-unknown-unknown" >&2
  exit 1
fi
grep -q "threads aren't available on wasm32-unknown-unknown" <<<"$failed"
