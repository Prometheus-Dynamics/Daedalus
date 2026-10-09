#!/usr/bin/env bash
# Local equivalent of the CI workflows. Run with no arguments for the full default loop, or
# pass one or more subcommands (see `usage`). CI jobs call the same subcommands, so the
# commands only live here.
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

# Default CI feature set for workspace-wide builds.
readonly CI_FEATURES="engine,plugins"
readonly AARCH64_TARGET="aarch64-unknown-linux-gnu"
readonly AARCH64_MUSL_TARGET="aarch64-unknown-linux-musl"
# Bare metal without `std`: Cortex-M4F (32-bit atomics, no 64-bit ones) and Cortex-M0 (no
# compare-and-swap: atomics, locks and `Arc` go through `portable-atomic`).
readonly NOSTD_TARGETS=("thumbv7em-none-eabihf" "thumbv6m-none-eabi")
readonly NOSTD_CRATES=(-p daedalus-core -p daedalus-transport -p daedalus-data -p daedalus-registry
  -p daedalus-planner)
# MCU profile firmware (`examples/mcu_blink`, `--profile mcu`), one binary per mode:
# "mode:binary suffix:flash budget:static RAM budget", in bytes per target in NOSTD_TARGETS
# (flash = .vector_table + .text + .rodata + .data, static RAM = .data + .bss + .uninit).
# Measured sizes are in docs/mcu.md.
readonly MCU_MODES=("compiled::8192:512" "tunable:-tunable:10240:512" "loaded:-loaded:16384:1024")
# The compiled firmware's exact flash:RAM per target (NOSTD_TARGETS order): the tunable and
# loaded modes cost a compiled firmware nothing, and constants without parameters stay literals.
readonly MCU_COMPILED_SIZE=("3072:160" "3392:160")
readonly WASM_TARGET="wasm32-unknown-unknown"
readonly WASI_TARGET="wasm32-wasip1"

step() { echo "==> $*"; }

ensure_target() {
  rustup target list --installed | grep -qx "$1" || rustup target add "$1"
}

usage() {
  cat <<'EOF'
usage: scripts/ci.sh [subcommand...]

  all         lints features clippy link test macro-ui smoke (default)
  lints       file-size, workspace-deps, GPU async lints and rustfmt
  features    release feature-surface checks
  clippy      clippy with -D warnings: default CI features, dylib plugins, all features
  link        build (and link) every library with all features
  test        workspace tests (default CI features) and dylib plugin tests
  macro-ui    trybuild macro UI tests
  smoke       run the CPU-only example binaries (sharing the `test` build)
  doc         rustdoc for the workspace with -D warnings
  aarch64     cargo check for aarch64 gnu (default, embedded, gpu-dmabuf) and musl (libraries)
  lean        tests for the lean preset (no executor pool, no metrics) and without threads
  nostd       no_std + alloc checks for thumbv7em and thumbv6m (no CAS): tier-1 crates, the
              serial runtime and engine, and the no_std smoke graph
  mcu         MCU profile: build the blink firmwares (compiled, tunable, loaded) for thumbv7em
              and thumbv6m, check flash and static RAM against per-mode budgets (readelf), and
              run the MCU crates' native tests
  wasm        engine,plugins (embedded, no threads) dev builds and Node runs for wasm32 and WASI
  bench       host bridge, runtime executor and graph frame criterion benches
  pi          on-device dmabuf hardware tests and the gpu_probe report (Raspberry Pi 5 / CM5)
  vvl         the dmabuf hardware tests and gpu_probe under the Khronos validation layer
EOF
}

cmd_lints() {
  step "Checking file sizes"
  ./scripts/check-file-sizes.sh
  step "Checking workspace dependency centralization"
  ./scripts/check-workspace-deps.sh
  step "Checking async GPU paths"
  ./scripts/check-gpu-async-blocking.sh
  step "Checking formatting"
  cargo fmt --all -- --check
}

cmd_features() {
  step "Checking feature surfaces"
  cargo check -p daedalus-rs --no-default-features
  cargo check -p daedalus-rs --features "engine,plugins,gpu-mock"
  cargo check -p daedalus-rs --no-default-features --features "embedded"
  cargo check -p daedalus-rs --no-default-features --features "engine,plugins"
  cargo check -p daedalus-rs --all-targets --features "engine-full,plugins"
  cargo check -p daedalus-rs --features "dylib-plugins"
  cargo check -p daedalus-runtime --features "metrics,executor-pool,lockfree-queues"
  cargo check -p daedalus-ffi-core -p daedalus-ffi-host --no-default-features
  cargo check -p daedalus-ffi-core -p daedalus-ffi-host \
    --features "daedalus-ffi-core/image-payload,daedalus-ffi-host/image-payload"
  cargo check -p daedalus-gpu --no-default-features --features "gpu-wgpu"
  cargo check -p daedalus-gpu --all-targets --no-default-features --features "gpu-gles,gpu-image"
}

cmd_clippy() {
  step "Running clippy"
  cargo clippy --workspace --all-targets --features "$CI_FEATURES" -- -D warnings
  cargo clippy -p daedalus-rs --all-targets --features "$CI_FEATURES,dylib-plugins" -- -D warnings
  # Type-checks every target with every feature (the all-features `cargo check`) and lints it.
  # Needs libcamera-dev + pkg-config for the styx camera example feature.
  cargo clippy --workspace --all-targets --all-features -- -D warnings
}

# Links every library, so `cdylib` plugins whose exported symbols clash once features unify fail
# here (`check` and clippy do not link). Needs libcamera-dev + pkg-config (styx camera example).
cmd_link() {
  step "Building every library with all features"
  cargo build --workspace --lib --all-features
}

cmd_test() {
  step "Running tests"
  cargo test --workspace --all-targets --features "$CI_FEATURES"
  cargo test -p daedalus-rs --features "$CI_FEATURES,dylib-plugins"
}

cmd_macro_ui() {
  step "Running macro UI (trybuild) tests"
  cargo test -p daedalus-rs --features plugins --test transport_macro_ui -- --ignored
}

# Builds with the `test` step's crate graph (`--workspace`, CI features, examples pull in the
# dev-dependencies), so after `test` only the binaries themselves compile; `cargo run -p` would
# resolve features for one package and rebuild about 30 crates.
cmd_smoke() {
  step "Running CPU-only example binaries"
  local bins=(runtime_metrics transport_metrics ownership_metrics lifecycle_trace plan_debug
    overhead_floor observability backpressure_diagnostics external_frame_source frame_chain)
  local args=(--examples) bin exes exe
  for bin in "${bins[@]::${#bins[@]}-1}"; do args+=(--bin "$bin"); done
  exes="$(cargo build --workspace --features "$CI_FEATURES" "${args[@]}" \
    --message-format=json-render-diagnostics | json_field executable)"
  for bin in "${bins[@]}"; do
    echo "  -> $bin"
    exe="$(grep -m1 "/$bin\$" <<<"$exes")" || { echo "no executable for $bin" >&2 && return 1; }
    FRAME_CHAIN_TICKS=200 "$exe" >/dev/null
  done
}

# Prints string field $1 of each JSON message on stdin that has it (cargo's --message-format=json).
json_field() { sed -n "s/.*\"$1\":\"\\([^\"]*\\)\".*/\\1/p"; }

# Type-check only (no linking), so no cross linker or sysroot libraries are needed beyond a C
# cross compiler for build scripts that compile C (criterion's `alloca`). On Debian/Ubuntu that
# is `gcc-aarch64-linux-gnu`; see docs/testing.md for hosts without an aarch64 glibc sysroot.
# The styx camera feature is skipped: it needs target libcamera via pkg-config.
cmd_aarch64() {
  step "Checking $AARCH64_TARGET"
  ensure_target "$AARCH64_TARGET"
  local target=(--target "$AARCH64_TARGET")
  cargo check "${target[@]}" --workspace --all-targets --features "$CI_FEATURES"
  cargo check "${target[@]}" -p daedalus-rs --all-targets --no-default-features --features "embedded"
  cargo check "${target[@]}" -p daedalus-gpu --all-targets --features "gpu-dmabuf"
  # musl: libc signatures differ from glibc (e.g. `ioctl` takes a `c_int` request). Library
  # targets only, so no musl C toolchain is needed for test-only C build scripts.
  step "Checking $AARCH64_MUSL_TARGET"
  ensure_target "$AARCH64_MUSL_TARGET"
  cargo check --target "$AARCH64_MUSL_TARGET" --workspace --features "$CI_FEATURES"
  cargo check --target "$AARCH64_MUSL_TARGET" -p daedalus-runtime --all-features
  cargo check --target "$AARCH64_MUSL_TARGET" -p daedalus-gpu --features "gpu-dmabuf"
}

# Workspace tests unify features across all members, so the daemon crate turns on the executor
# pool and metrics for everyone. This run selects only the facade, engine, and runtime crates
# (never daedalus-daemon) with defaults off, so the parked-thread worker path and the no-op
# telemetry path are what get tested.
cmd_lean() {
  step "Testing lean preset (no executor-pool, no metrics)"
  cargo test -p daedalus-rs -p daedalus-engine -p daedalus-runtime --all-targets \
    --no-default-features \
    --features "daedalus-rs/embedded,daedalus-engine/config-env,daedalus-engine/plugins,daedalus-runtime/plugins"
  step "Testing without threads (serial-only runtime and engine)"
  cargo test -p daedalus-runtime -p daedalus-engine --all-targets --no-default-features \
    --features "daedalus-runtime/plugins,daedalus-engine/plugins,daedalus-engine/config-env"
}

# On each bare-metal target: the tier-1 crates without `std`, with and without their alloc-only
# optional features; then (tier 2) the serial runtime and engine, likewise, and the
# `examples/nostd_smoke` graph. `tracing` is checked only where it builds (it needs
# compare-and-swap). The smoke tests also run natively with `std` off everywhere.
cmd_nostd() {
  local target
  for target in "${NOSTD_TARGETS[@]}"; do
    step "Checking no_std + alloc crates for $target"
    ensure_target "$target"
    cargo check --target "$target" "${NOSTD_CRATES[@]}" --no-default-features
    cargo check --target "$target" "${NOSTD_CRATES[@]}" --no-default-features --features \
      "daedalus-core/metrics,daedalus-data/json,daedalus-data/schema,daedalus-data/proto,daedalus-data/async,daedalus-registry/bundle,daedalus-registry/plugin,daedalus-planner/schema,daedalus-planner/proto"
    step "Checking the no_std serial runtime and engine for $target"
    cargo check --target "$target" -p daedalus-runtime -p daedalus-engine --no-default-features
    cargo check --target "$target" -p daedalus-runtime -p daedalus-engine --no-default-features \
      --features "daedalus-runtime/plugins,daedalus-runtime/metrics,daedalus-runtime/snapshots,daedalus-runtime/lockfree-queues,daedalus-engine/plugins,daedalus-engine/config-env"
    cargo check --target "$target" -p daedalus-nostd-smoke
  done
  step "Checking the no_std runtime and engine with tracing for ${NOSTD_TARGETS[0]}"
  cargo check --target "${NOSTD_TARGETS[0]}" -p daedalus-runtime -p daedalus-engine \
    --no-default-features --features "daedalus-engine/tracing,daedalus-engine/plugins"
  step "Linting and testing the tier-1 crates natively without std"
  cargo clippy "${NOSTD_CRATES[@]}" --all-targets --no-default-features -- -D warnings
  cargo test "${NOSTD_CRATES[@]}" --no-default-features
  step "Running the no_std smoke test natively"
  cargo test -p daedalus-nostd-smoke
}

# The MCU profile (docs/mcu.md): the blink graph is planned on the host by its build script and
# runs on the device as generated code (compiled, compiled + tunable) or as a plan blob loaded by
# the interpreter (loaded). Builds the three firmwares per bare-metal target with the `mcu` size
# profile, prints their flash and static RAM (section sizes from `readelf`), fails above each
# mode's budgets or if the compiled firmware changed size; checks the device crate's optional
# features; runs the native tests (every mode checked against the node functions, with an
# allocation counter).
cmd_mcu() {
  local out="${CARGO_TARGET_DIR:-target}" target mode name suffix flash_budget ram_budget
  local flash ram failed=0 index=0
  for target in "${NOSTD_TARGETS[@]}"; do
    step "Building the MCU blink firmwares for $target"
    ensure_target "$target"
    cargo build --target "$target" --profile mcu -p daedalus-mcu-blink --bins
    cargo check --target "$target" -p daedalus-mcu --features "alloc,defmt,loaded"
    for mode in "${MCU_MODES[@]}"; do
      IFS=: read -r name suffix flash_budget ram_budget <<<"$mode"
      read -r flash ram < <(mcu_size "$out/$target/mcu/daedalus-mcu-blink$suffix")
      echo "  $target $name: flash $flash B (budget $flash_budget), static RAM $ram B (budget $ram_budget)"
      if ((flash > flash_budget || ram > ram_budget)); then
        echo "  $target $name: MCU firmware over budget" >&2
        failed=1
      fi
      if [[ $name == compiled && "$flash:$ram" != "${MCU_COMPILED_SIZE[index]}" ]]; then
        echo "  $target compiled: expected ${MCU_COMPILED_SIZE[index]} (flash:RAM); update MCU_COMPILED_SIZE and docs/mcu.md if intended" >&2
        failed=1
      fi
    done
    index=$((index + 1))
  done
  step "Testing the MCU crates and the blink graph natively"
  cargo test -p daedalus-mcu --features loaded
  cargo test -p daedalus-mcu-build -p daedalus-mcu-blink
  return "$failed"
}

# Prints "flash ram" of firmware ELF $1 (see MCU_MODES).
mcu_size() {
  local name size flash=0 ram=0
  while read -r name size; do
    size=$((16#$size))
    case "$name" in
      .vector_table | .text | .rodata) flash=$((flash + size)) ;;
      .data) flash=$((flash + size)) ram=$((ram + size)) ;;
      .bss | .uninit) ram=$((ram + size)) ;;
    esac
  done < <(readelf -S -W "$1" | sed -n 's/^ *\[ *[0-9]*\] *\(\.[^ ]*\) *[A-Z_]* *[0-9a-f]* *[0-9a-f]* *\([0-9a-f]*\) .*/\1 \2/p')
  echo "$flash $ram"
}

# `wasm32-unknown-unknown` has `std` but no threads and no clock; `wasm32-wasip1` has a clock but
# no threads. Build the embedded preset without `threads` (`engine,plugins`) for both, then run in
# Node: serial/parallel/adaptive frames (an import-free module, and a WASI command on the platform
# clock) and the wasm-bindgen host example driven from JS. Runs are skipped without `node`; the
# wasm-bindgen one without a `wasm-bindgen` CLI matching Cargo.lock (required when `$CI` is set).
# Dev profile: the smoke runs check behavior (with overflow checks and debug assertions on), not
# optimized code, and a release build of the graph costs about four times as much.
cmd_wasm() {
  local out="${CARGO_TARGET_DIR:-target}" target
  for target in "$WASM_TARGET" "$WASI_TARGET"; do
    ensure_target "$target"
  done
  # Separate builds, so the smoke module keeps the preset's features (`daedalus` with exactly
  # `engine,plugins`; this also type-checks the preset for both targets); the host example adds
  # `metrics`.
  step "Building the embedded preset for $WASM_TARGET and $WASI_TARGET"
  cargo build --target "$WASM_TARGET" -p daedalus-wasm-smoke --lib
  cargo build --target "$WASM_TARGET" -p daedalus-wasm-bindgen-host
  cargo build --target "$WASI_TARGET" -p daedalus-wasm-smoke --bin daedalus-wasi-smoke
  if ! command -v node >/dev/null; then
    echo "node not found: skipping the wasm smoke runs"
    return
  fi
  step "Running the wasm and WASI runtime smoke tests"
  node scripts/wasm-smoke.mjs "$out/$WASM_TARGET/debug/daedalus_wasm_smoke.wasm"
  node --no-warnings scripts/wasi-smoke.mjs "$out/$WASI_TARGET/debug/daedalus-wasi-smoke.wasm"
  local version
  version="$(sed -n '/^name = "wasm-bindgen"$/{n;s/^version = "\(.*\)"$/\1/p}' Cargo.lock)"
  if [[ "$(wasm-bindgen --version 2>/dev/null)" != "wasm-bindgen $version" ]]; then
    echo "wasm-bindgen $version not found (cargo install wasm-bindgen-cli --version $version):" \
      "skipping the wasm-bindgen host run"
    [[ -z "${CI:-}" ]] || return 1
    return
  fi
  step "Running the wasm-bindgen host example"
  wasm-bindgen --target nodejs --out-dir "$out/wasm-bindgen-host" \
    "$out/$WASM_TARGET/debug/daedalus_wasm_bindgen_host.wasm"
  node scripts/wasm-bindgen-host.mjs "$out/wasm-bindgen-host/daedalus_wasm_bindgen_host.js"
}

# Criterion writes to `$CARGO_TARGET_DIR/criterion`; compare two such directories with
# `scripts/bench-compare.py BASELINE CURRENT`.
cmd_bench() {
  step "Running host bridge and executor benches"
  cargo bench -p daedalus-engine --features plugins --bench host_graph_drive
  cargo bench -p daedalus-runtime --bench executor_snapshot
  cargo bench -p daedalus-rs --features engine-full,plugins --bench graph_frame
}

# Run on the target device (Raspberry Pi 5 / CM5, or any Linux Vulkan GPU): the ignored dmabuf
# hardware tests, then a paste-friendly capability report. See docs/testing.md.
cmd_pi() {
  step "Running dmabuf hardware tests"
  cargo test -p daedalus-gpu --features gpu-dmabuf -- --include-ignored dmabuf
  step "Probing GPU and dmabuf support"
  cargo run -p daedalus-gpu --features gpu-dmabuf --example gpu_probe
}

# The `pi` tests and probe under VK_LAYER_KHRONOS_validation with synchronization validation; see
# "Vulkan Validation Layers" in docs/testing.md. Uses an installed layer, or the manifests under
# $VK_LAYER_PATH (an unpacked package: the library is looked up in ../../../lib64 and ../../../lib
# of each manifest directory). Implicit layers (overlays, capture tools) are disabled unless
# VK_LOADER_LAYERS_DISABLE says otherwise. Fails on any validation message except the known
# timeline-wait one (VUID-vkQueueSubmit-pWaitSemaphores-03238, from wgpu's binary semaphore chain
# behind a pending timeline wait, which only the opt-in `Timeline` fence mode creates).
readonly VVL_KNOWN='VUID-vkQueueSubmit-pWaitSemaphores-03238'

cmd_vvl() {
  local dir lib log="${CARGO_TARGET_DIR:-target}/vvl.log"
  if [[ -n "${VK_LAYER_PATH:-}" ]]; then
    local -a dirs
    IFS=: read -r -a dirs <<<"$VK_LAYER_PATH"
    for dir in "${dirs[@]}"; do
      for lib in "$dir/../../../lib64" "$dir/../../../lib"; do
        if [[ -d "$lib" ]]; then
          LD_LIBRARY_PATH="$(cd "$lib" && pwd)${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
        fi
      done
    done
    export LD_LIBRARY_PATH
  fi
  export VK_INSTANCE_LAYERS=VK_LAYER_KHRONOS_validation
  export VK_LOADER_LAYERS_DISABLE="${VK_LOADER_LAYERS_DISABLE-~implicit~}"
  export VK_KHRONOS_VALIDATION_VALIDATE_SYNC=true
  export VK_KHRONOS_VALIDATION_DEBUG_ACTION=VK_DBG_LAYER_ACTION_LOG_MSG
  export VK_KHRONOS_VALIDATION_REPORT_FLAGS=error,warn,perf
  export VK_KHRONOS_VALIDATION_LOG_FILENAME=stdout
  step "Checking that the validation layer loads"
  local loaded
  loaded="$(VK_LOADER_DEBUG=layer cargo run -q -p daedalus-gpu --features gpu-dmabuf \
    --example gpu_probe 2>&1 || true)"
  if ! grep -q 'Insert instance layer "VK_LAYER_KHRONOS_validation"' <<<"$loaded"; then
    echo "VK_LAYER_KHRONOS_validation not found: install it or set VK_LAYER_PATH" >&2
    return 1
  fi
  mkdir -p "$(dirname "$log")"
  step "Running dmabuf hardware tests and gpu_probe under the validation layer (log: $log)"
  {
    cargo test -p daedalus-gpu --features gpu-dmabuf -- --include-ignored dmabuf --test-threads=1
    cargo run -p daedalus-gpu --features gpu-dmabuf --example gpu_probe
  } 2>&1 | tee "$log"
  step "Validation messages"
  grep -E '^Validation (Error|Warning|Performance)' "$log" | sed 's/ | MessageID.*//' | sort | uniq -c || true
  if grep -E '^Validation (Error|Warning)' "$log" | grep -qv "$VVL_KNOWN"; then
    echo "unexpected validation messages; see $log" >&2
    return 1
  fi
}

cmd_doc() {
  step "Building docs"
  RUSTDOCFLAGS="${RUSTDOCFLAGS:+$RUSTDOCFLAGS }-Dwarnings" cargo doc --workspace --no-deps
}

cmd_all() {
  cmd_lints
  cmd_features
  cmd_clippy
  cmd_link
  cmd_test
  cmd_macro_ui
  cmd_smoke
}

main() {
  [[ $# -eq 0 ]] && set -- all
  local sub
  for sub in "$@"; do
    case "$sub" in
      -h | --help | help) usage ;;
      all | lints | features | clippy | link | test | smoke | doc | aarch64 | lean | nostd | mcu | \
        wasm | bench | pi | vvl)
        "cmd_$sub" ;;
      macro-ui) cmd_macro_ui ;;
      *)
        echo "unknown subcommand: $sub" >&2
        usage >&2
        exit 2
        ;;
    esac
  done
}

main "$@"
