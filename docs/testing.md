# Testing

Daedalus validation is split into a default workspace loop, focused feature checks, FFI fixture checks, GPU checks, and Docker-backed example validation.

## Default Surface

This is the canonical validation loop; the README and development guide link here. Run it before
sending broad changes. `./scripts/repo-clean.sh` first applies `cargo fmt` and `clippy --fix`,
then runs the full [local CI runner](#local-ci-runner).

```bash
cargo fmt --all -- --check
./scripts/check-file-sizes.sh
./scripts/check-workspace-deps.sh
./scripts/check-gpu-async-blocking.sh
cargo test --workspace --all-targets --features "engine,plugins"
cargo clippy --workspace --all-targets --features "engine,plugins" -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings   # needs libcamera-dev
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps
```

Clippy type-checks everything `cargo check` would (the default-feature workspace graph is the
`engine,plugins` one: `daedalus-examples` turns those features on for the facade anyway), so
there is no separate `cargo check` step.

The facade-only suites run in their own CI job (`facade`), in parallel with the workspace tests:

```bash
# native Rust cdylib plugin loading
cargo test -p daedalus-rs --features "engine,plugins,dylib-plugins"
# trybuild macro UI tests (ignored in the default test run; `plugins` only, so the `#[node]`
# expansions are checked without the engine)
cargo test -p daedalus-rs --features plugins --test transport_macro_ui -- --ignored
```

Integration tests share one binary per crate (`tests/it/main.rs`, one module per area): run one
area with `cargo test -p <crate> --test it -- <module>::`. Tests that install a counting
`#[global_allocator]`, the `dylib-plugins` tests (`export_plugin!` exports fixed symbols), the
trybuild and the Docker tests keep their own binaries. The tests run under `cargo test`, not
cargo-nextest: process-per-test tripled the CPU time of the test run and made the timing
assertions (`adaptive_mode`) fail under load, for about 15 s of wall time.

## Local CI Runner

`scripts/ci.sh` runs the same commands as CI. With no arguments it runs the full default loop
(`all`); otherwise pass one or more subcommands, e.g. `scripts/ci.sh lints test`. Run
`scripts/ci.sh help` for the list. CI jobs call these subcommands, so edit commands there.

| Subcommand | What it runs |
|---|---|
| `lints` | file-size, workspace-deps, GPU async lints, `cargo fmt --check` |
| `features` | release feature-surface checks and `cargo build --workspace --lib --all-features` (needs `libcamera-dev`) |
| `clippy`, `test`, `macro-ui` | clippy `-D warnings` (`engine,plugins`, dylib plugins, all features; needs `libcamera-dev`); workspace + dylib tests; trybuild |
| `smoke` | the CPU-only example binaries (`runtime_metrics` ... `external_frame_source`); `test` already built every `daedalus-examples` binary |
| `aarch64` | `cargo check --target aarch64-unknown-linux-gnu` (see below) |
| `lean` | lean-preset tests (see below) |
| `nostd`, `wasm` | `no_std` check of the tier-1 crates (with and without CAS), the runtime and engine, and the no_std smoke graph; wasm and WASI `engine,plugins` checks and Node runs (see below) |
| `mcu` | MCU profile firmware for `thumbv7em` and `thumbv6m`, flash/RAM budgets, native tests (see below) |
| `bench` | host bridge and executor criterion benches (see below) |
| `pi` | on-device dmabuf hardware tests and the `gpu_probe` report; not part of `all` (see [Validating on a Raspberry Pi 5](#validating-on-a-raspberry-pi-5)) |
| `vvl` | the `pi` tests and probe under the Khronos validation layer; not part of `all` (see [Vulkan Validation Layers](#vulkan-validation-layers)) |

### aarch64

```bash
scripts/ci.sh aarch64
```

Type-checks (no linking) the workspace with the default CI features (`engine,plugins`), the
facade `embedded` preset, and `daedalus-gpu` with `gpu-dmabuf` for `aarch64-unknown-linux-gnu`.
The script adds the rustup target if missing. Build scripts that compile C (criterion's
`alloca`, pulled in by `--all-targets`) need an aarch64 C compiler with libc headers: on
Debian/Ubuntu install `gcc-aarch64-linux-gnu` (CI does). On hosts whose cross gcc has no aarch64
glibc sysroot (e.g. Fedora's `gcc-aarch64-linux-gnu`), set
`CFLAGS_aarch64_unknown_linux_gnu=-ffreestanding`. The `styx-camera-example` feature is not
checked: it needs target `libcamera` via pkg-config.

### Lean preset

```bash
scripts/ci.sh lean
# = cargo test -p daedalus-rs -p daedalus-engine -p daedalus-runtime --all-targets --no-default-features \
#     --features "daedalus-rs/embedded,daedalus-engine/config-env,daedalus-engine/plugins,daedalus-runtime/plugins"
#   then runtime + engine without `threads`:
#   cargo test -p daedalus-runtime -p daedalus-engine --all-targets --no-default-features \
#     --features "daedalus-runtime/plugins,daedalus-engine/plugins,daedalus-engine/config-env"
```

`cargo test --workspace` unifies features across members, and `daedalus-daemon` enables the
executor pool and metrics, so workspace tests never exercise the parked-thread worker path, the
serial-only (no `threads`) build or no-op telemetry. The lean run selects only the facade, engine, and runtime crates (never the
daemon) with default features off. Verify with `cargo tree -i rayon` using the same flags: rayon
should only appear under criterion.

### no_std and WASM

```bash
scripts/ci.sh nostd wasm
```

`nostd` checks, for `thumbv7em-none-eabihf` and `thumbv6m-none-eabi` (no compare-and-swap):
`daedalus-core`, `-transport`, `-data`, `-registry` and `-planner` with
`--no-default-features`, once bare and once with their alloc-only optional features;
`daedalus-runtime` and `daedalus-engine` without default features, bare and with `plugins`,
`metrics`, `snapshots`, `lockfree-queues` and `config-env`; and `examples/nostd_smoke` (a
`#![no_std]` crate driving a one-node graph through the serial executor and through
`Engine`/`HostGraph` on an injected counter clock). It also checks the runtime and engine with
`tracing` for `thumbv7em` (it needs compare-and-swap) and runs the smoke tests natively, where
every Daedalus crate builds without `std`. `riscv32imc-unknown-none-elf` (also without
compare-and-swap) is not in CI; check it locally with the same commands. `wasm` builds the
facade's `engine,plugins` (the `embedded` preset without `threads`) for `wasm32-unknown-unknown`
and `wasm32-wasip1` as the smoke modules (dev profile: overflow checks and debug assertions on, at
about a quarter of the release build's cost) and runs them in Node:

- `node scripts/wasm-smoke.mjs`: `examples/wasm_smoke` as a `cdylib` with no imports. Serial,
  parallel and adaptive runs of a fan-out graph, timed by an injected `Clock`, must match. A
  panic (say, a direct `std::time::Instant::now()` on the runtime path) traps and fails the run.
- `node --no-warnings scripts/wasi-smoke.mjs`: the same runs in the `daedalus-wasi-smoke` WASI
  command, on the platform clock, under Node's `node:wasi`.
- `node scripts/wasm-bindgen-host.mjs`: `examples/wasm_bindgen_host` after
  `wasm-bindgen --target nodejs`, driven from JavaScript (`push`/`tick`/`take`) on a simulated
  `performance.now()`; outputs and tick durations must match. It needs the `wasm-bindgen` CLI at
  the `Cargo.lock` version (`cargo install wasm-bindgen-cli --version <version>`); without it the
  run is skipped, unless `$CI` is set.

Without `node` the runs are skipped after the builds. The script adds the rustup targets if
missing; the `portability` CI job runs both subcommands. See
[development.md](development.md#portability) for what each target supports.

### MCU profile

```bash
scripts/ci.sh mcu
```

Builds the `examples/mcu_blink` firmware with the `mcu` size profile for `thumbv7em-none-eabihf`
and `thumbv6m-none-eabi` (its `build.rs` plans `graph.json` on the host), prints its flash
(`.vector_table + .text + .rodata + .data`) and static RAM (`.data + .bss + .uninit`) from
`readelf -S`, and fails above `MCU_FLASH_BUDGET`/`MCU_RAM_BUDGET` in `scripts/ci.sh`. It also
checks `daedalus-mcu` with `alloc,defmt` for both targets and runs the native tests: the
compiler's unit tests and the blink graph checked against its node functions with an allocation
counter. The `portability` CI job runs it. See [mcu.md](mcu.md).

### Benchmarks

Benches do not run on pull requests. The `bench` workflow (`.github/workflows/bench.yml`) runs
on `workflow_dispatch`, weekly, and on pushes to `main` that touch runtime/engine/transport. It
runs `scripts/ci.sh bench`, uploads `target/criterion` as the `criterion` artifact (90 days),
downloads the same artifact from the newest earlier completed run on the default branch, and
runs:

```bash
python3 scripts/bench-compare.py BASELINE_DIR CURRENT_DIR [--threshold 15] [--stat median|mean]
```

The script reads every `new/estimates.json` under both criterion roots, prints a table (and
appends it to the job summary), emits a warning annotation per benchmark whose median grew by
more than the threshold, and exits 1 if any did. With no baseline it just reports. Locally:

```bash
cp -r target/criterion /tmp/criterion-before   # after a run on the base commit
scripts/ci.sh bench
python3 scripts/bench-compare.py /tmp/criterion-before target/criterion
```

Shared CI runners are noisy; treat a single flagged run as a prompt to re-run, and a flag that
repeats as a real regression. Since the baseline is always the previous run, an accepted
regression only flags once.

## Validating on a Raspberry Pi 5

CI has no Pi, so the zero-copy dmabuf path (`daedalus-gpu` with `gpu-dmabuf`) is validated by
running one command on the device and pasting its output back. The same command works on any
Linux machine with a Vulkan GPU.

**Install** (64-bit Raspberry Pi OS, Pi 5 or CM5):

```bash
sudo apt install build-essential pkg-config git mesa-vulkan-drivers vulkan-tools
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh   # rustup; the first cargo run installs the pinned toolchain
sudo usermod -aG video "$USER"   # read/write access to /dev/dma_heap/*; log in again afterwards
```

`vulkaninfo --summary` should list a V3D adapter (Mesa `v3dv`). The kernel needs to be 6.0 or
newer for `DMA_BUF_IOCTL_EXPORT_SYNC_FILE` (current Raspberry Pi OS kernels are).

**Run** from the repository root (4 build jobs keep an 8 GB board out of swap):

```bash
CARGO_BUILD_JOBS=4 ./scripts/ci.sh pi 2>&1 | tee pi-report.txt
# Allocate the probe/test buffers from another heap (default: /dev/dma_heap/system):
DAEDALUS_DMA_HEAP=/dev/dma_heap/linux,cma CARGO_BUILD_JOBS=4 ./scripts/ci.sh pi
```

It runs `cargo test -p daedalus-gpu --features gpu-dmabuf -- --include-ignored dmabuf` (the
`#[ignore]`d hardware tests: single-plane import, plane offsets, fences in every wait mode the
device has (`SYNC_FD`, timeline with its timeout and watcher, CPU) and the fence-mode selection
(default, per-import override, non-`sync_file` fallback), a late fence from another
device's GPU job, NV12 in one dmabuf and disjoint, and every renderable modifier exported by a
Vulkan producer and read back on a second device; cases the device cannot run print a skip
reason) and then `cargo run -p daedalus-gpu --features gpu-dmabuf --example gpu_probe`.

Expected on a Pi 5 with current Mesa: all tests pass; `dmabuf_acquire_fence_mode: auto`,
`dmabuf_acquire_fence_wait: sync_fd` and `dmabuf_acquire_fence_waits: sync_fd, timeline, cpu`
(v3dv imports `SYNC_FD` semaphores and has Vulkan 1.3 timeline semaphores); the modifier test
round-trips `LINEAR` plus whichever tiled modifiers v3dv advertises as renderable for `R8` and
`XRGB8888` (Broadcom `UIF` is `0x700000000000006`); V3D has no compressed modifiers, so no `aux`
planes are expected. These are expectations, not yet confirmed on a Pi. Without lavapipe installed, a `skipping: no lavapipe consumer` line is
expected: the fence tests then use a second v3dv device. Run with `-- --nocapture` to see the
measurements (watcher hop, blocked submission) and the round-tripped modifiers.

**Paste back** the `test result:` line of the dmabuf tests (plus any failure or `skipping` output)
and the whole probe report, from `daedalus-gpu gpu_probe` to the end. The probe never panics on
missing hardware; a failed check prints its error in place of the value.

| Line | Meaning |
|---|---|
| `kernel`, `kernel_build`, `machine`, `model` | `uname` and `/proc/device-tree/model` (e.g. `Raspberry Pi 5 Model B Rev 1.0`) |
| `name`, `backend`, `device_type`, `vendor_id`, `device_id` | the adapter `WgpuBackend` selected; on a Pi a V3D adapter on `Vulkan` |
| `driver`, `driver_info` | Vulkan driver name and Mesa version (e.g. `V3DV Mesa`, `Mesa 25.x`) |
| `dmabuf_import` | `GpuContextHandle::dmabuf_import_support()`: `supported`, or `unsupported:` with the reason (non-Vulkan adapter, missing extension) |
| `dmabuf_acquire_fence_mode` | the backend's default `AcquireFenceMode` (`auto` unless `GpuOptions::acquire_fence_mode` says otherwise) |
| `dmabuf_acquire_fence_wait` | the wait that mode resolves to on this device: `sync_fd` (GPU waits on the imported `sync_file`, no timeout; the `auto` choice where available), `timeline` (GPU waits on a timeline value a watcher thread signals on fence or `acquire_timeout`), or `cpu` (the import polls the `sync_file`) |
| `dmabuf_acquire_fence_waits` | every wait the device has; imports can pick another one with `with_acquire_fence_mode` |
| `VK_KHR_external_semaphore_fd`, `VK_EXT_queue_family_foreign` | whether the device enabled them: the first is needed for `sync_fd` waits, the second acquires and releases imports through the foreign queue family (otherwise `EXTERNAL`) |
| `timeline_semaphore`, `sync_fd_semaphore_import` | the device features behind the `timeline` and `sync_fd` waits |
| `texture_format_nv12` | the device has wgpu `TEXTURE_FORMAT_NV12`; without it NV12 must be imported per plane (`R8` + `GR88`) |
| `vulkan_api` | Vulkan version the physical device reports |
| `nv12_modifiers` | DRM modifiers the driver advertises for `G8_B8R8_2PLANE_420_UNORM` (`0x0` is `LINEAR`) |
| `nv12_linear`, `nv12_linear_memory_planes`, `nv12_linear_tiling_features` | whether `LINEAR` NV12 is advertised, its memory plane count and format features |
| `nv12_linear_disjoint_feature` | the `LINEAR` NV12 modifier has `DISJOINT`, so Y and UV can live in separate dmabufs |
| `nv12_linear_import_one_dmabuf`, `nv12_linear_import_disjoint` | `ok`, or `rejected:` with the reason, for the image-format query a sampled NV12 import makes with both planes in one dmabuf / one dmabuf per plane |
| `nv12_linear_needs_disjoint` | `yes` when only the disjoint import is accepted, so a producer must hand out separate Y and UV dmabufs |
| `r8_modifiers`, `xrgb8888_modifiers`, `xbgr8888_modifiers`, `nv12_modifiers` | every modifier the driver advertises for the format: memory plane count, `aux` when it has more memory planes than format planes (compression metadata), its usable features (`sample`, `render`, `storage`, `disjoint`), and `tested` when the modifier round-trip test exercises it |
| `/dev/dma_heap/<heap>` | each dma-heap and whether it opens read/write (`open failed: Permission denied` means the `video` group is missing) |
| `heap`, `export_sync_file`, `fence_signaled` | a page allocated from that heap and whether `DMA_BUF_IOCTL_EXPORT_SYNC_FILE` works on it (`unsupported` on kernels before 6.0); an idle buffer's fence is already signaled |

## Vulkan Validation Layers

The dmabuf path drives Vulkan directly (image creation over dmabuf memory, queue family ownership
transfers, semaphore waits), so run its hardware tests and `gpu_probe` under the Khronos
validation layer (`VK_LAYER_KHRONOS_validation`) after changing it:

```bash
# With the layer installed (Fedora: vulkan-validation-layers, Debian/Ubuntu:
# vulkan-validationlayers, or the LunarG SDK):
./scripts/ci.sh vvl
# Without root: unpack the distro package (or an official Vulkan-ValidationLayers release) and
# point VK_LAYER_PATH at its manifest directory; the library is found in ../../../lib64 or lib:
dnf download vulkan-validation-layers --destdir /tmp/vvl
(cd /tmp/vvl && rpm2cpio vulkan-validation-layers-*.rpm | cpio -idm)
VK_LAYER_PATH=/tmp/vvl/usr/share/vulkan/explicit_layer.d ./scripts/ci.sh vvl
```

`vvl` sets `VK_INSTANCE_LAYERS=VK_LAYER_KHRONOS_validation`, turns on synchronization validation
(`VK_KHRONOS_VALIDATION_VALIDATE_SYNC=true`), and has the layer print errors, warnings and
performance warnings to stdout (`VK_KHRONOS_VALIDATION_DEBUG_ACTION=VK_DBG_LAYER_ACTION_LOG_MSG`,
`..._LOG_FILENAME=stdout`, so the messages do not depend on a `log` subscriber for wgpu's debug
messenger). It disables implicit layers (`VK_LOADER_LAYERS_DISABLE=~implicit~`): overlay and
capture layers such as OBS `vkcapture` inject device extensions of their own (seen as
`WARNING-CreateDevice-extension-wrong-type` for `VK_KHR_get_physical_device_properties2`). It
checks that the layer actually loads, runs the dmabuf tests with `--test-threads=1` and the
probe, prints a count per message ID, keeps the log in `$CARGO_TARGET_DIR/vvl.log`, and fails on
any error or warning other than the known one below. Set the same variables by hand to validate
other commands.

Known message: `VUID-vkQueueSubmit-pWaitSemaphores-03238` from the tests that opt into the
`Timeline` fence mode. wgpu-hal orders submissions with a chain of binary semaphores; a
submission after one that waits for a not-yet-signaled timeline value waits on a binary semaphore
whose signal depends on that timeline value, which the spec only allows once the timeline signal
was submitted (the watcher signals it from the host later). Mesa copes by holding back the next
submission's thread (the documented `Timeline` caveat). It cannot be fixed outside wgpu-hal, and
the default `Auto` mode (`SyncFd` first) does not produce it.

Last run (RADV, RX 6800 XT, Mesa 26.2, validation layer 1.4.341): no other errors, warnings or
performance warnings, with synchronization validation on.

## Release Feature Surface

```bash
cargo check -p daedalus-rs --no-default-features
cargo check -p daedalus-rs --all-targets --features "engine,plugins"
cargo check -p daedalus-rs --all-targets --features "gpu-mock,plugins,engine"
cargo check -p daedalus-runtime --features "metrics,executor-pool,lockfree-queues"
cargo check -p daedalus-ffi-core --no-default-features   # without `integrity` (sha2)
cargo check -p daedalus-ffi-core --features "image-payload"
cargo check -p daedalus-ffi-host --no-default-features
cargo check -p daedalus-ffi-host --features "image-payload"
cargo check -p daedalus-gpu --no-default-features --features gpu-wgpu
cargo check -p daedalus-gpu --no-default-features --features gpu-wgpu,gpu-async
cargo check -p daedalus-gpu --all-targets --no-default-features --features gpu-gles,gpu-image
```

Use `gpu-wgpu` only where hardware and drivers are available. Use `gpu-mock` for CI-stable GPU-path coverage.

## Examples

The runnable examples are the `daedalus-examples` binaries (`examples/0*`); the workspace test
run builds all of them, and `scripts/ci.sh smoke` runs the CPU-only ones:

```bash
cargo check -p daedalus-examples --all-targets
cargo run -p daedalus-examples --quiet --bin runtime_metrics
cargo run -p daedalus-examples --quiet --bin transport_metrics
cargo run -p daedalus-examples --quiet --bin ownership_metrics
cargo run -p daedalus-examples --quiet --bin lifecycle_trace
cargo run -p daedalus-examples --quiet --bin plan_debug
cargo run -p daedalus-examples --quiet --bin overhead_floor
cargo run -p daedalus-examples --quiet --bin external_frame_source
```

## FFI

FFI tests should cover both contract-level validation and host execution behavior:

- generated canonical fixture snapshots,
- package descriptor validation and integrity stamping,
- persistent worker handshake and repeated invocation,
- state synchronization,
- payload lease ownership and release accounting,
- normalized response decoding across Python, Node, Java, C/C++, and Rust fixture paths.

Run package-specific tests while the FFI rewrite is active:

```bash
cargo test -p daedalus-ffi-core
cargo test -p daedalus-ffi-host
cargo test -p daedalus-ffi-python
cargo test -p daedalus-ffi-node
cargo test -p daedalus-ffi-java
cargo test -p daedalus-ffi-cpp
```

## Docker

```bash
cargo test -p daedalus-rs --test docker_examples -- --ignored --nocapture
```

The Docker suite uses [`testing/docker/daedalus-examples.Dockerfile`](../testing/docker/daedalus-examples.Dockerfile) and validates real facade examples in a controlled image.

## Other Coverage

- UI and macro diagnostics live under `crates/nodes/tests/ui` and `crates/daedalus/tests/ui`.
- Planner/runtime integration tests live under `crates/planner/tests` and `crates/runtime/tests`.
- Runtime plan goldens live under `crates/runtime/tests/goldens`.
- File-size linting warns about Rust files over 800 lines that are not listed in
  `testing/ci/file-size-baseline.txt`, and fails when a baseline entry is stale (the file is
  missing or back within the limit), so the baseline only ever shrinks.
