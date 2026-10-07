# Development

Daedalus is a layered Rust workspace. Keep changes inside the layer that owns the behavior.

## Layout

- `crates/core`: shared primitive types only.
- `crates/transport`: generic payload transport contracts.
- `crates/data`: portable value/type/descriptor model.
- `crates/registry`: declarations and capability snapshots.
- `crates/planner`: graph validation, lowerings, adapter selection, and scheduling inputs.
- `crates/runtime`: runtime plans, executor, host bridge, streaming, state, and telemetry.
- `crates/engine`: application-facing facade over registry/planner/runtime.
- `crates/gpu`: backend selection and GPU resource/dispatch support.
- `crates/ffi`: language-neutral contracts, host runner, and language SDK integration.
- `crates/frame-bench`: frame-path overhead harness and the `frame_chain` bench (unpublished).
- `examples`: runnable examples and plugin fixtures.
- `scripts` and `testing`: local validation and CI support.

## Validation Loop

Run the default loop in [testing.md](testing.md#default-surface) before sending broad changes.
Use `gpu-mock` for deterministic GPU-path tests and `gpu-wgpu` only on machines with a real backend available.

## Dependency Policy

- Workspace crates should inherit common dependencies from root `workspace.dependencies`.
- Library error types should be typed and use `thiserror`.
- Runtime instrumentation should use `tracing`, through the crate's `trace` module in the
  runtime and engine (see [Portability](#portability)).
- Avoid adding dependencies to `core` and `transport` unless the owning contract truly requires them.
- Dependencies of the `no_std` crates must support `no_std` (see [Portability](#portability)).
- Keep backend variants behind stable feature names.
- Use `cargo tree -d --workspace` during release review and treat new duplicate dependency roots as review input.
- Toolchain and MSRV: `rust-toolchain.toml` pins Rust 1.99.0, and `rust-version` in
  `[workspace.package]` is 1.99, the same release: the workspace is built, linted and measured
  (MCU sizes, allocation budgets) only with the pinned toolchain, so that is the oldest one it
  supports. The CI workflows and `testing/docker/daedalus-examples.Dockerfile` pin the same
  version; raise all of them together. The upgraded dependencies alone need 1.90
  (`ordered-float`).
- Dependencies track their newest releases, majors included (`cargo upgrade --incompatible
  --recursive`, then `cargo update`; `styx` follows its `dev` branch through `Cargo.lock`). A
  newest major is kept back only when it cannot meet a requirement (for example it drops
  `no_std` or a supported target); record the reason here. Nothing is kept back at present.
  Duplicate roots that remain because a dependency has not moved yet: `libloading` 0.8 (`ash`,
  `wgpu-hal`) beside 0.9, `spin` 0.10 (Styx) beside 0.12, and `syn` 2 (most derive crates)
  beside 3.
- The `wasm-bindgen` CLI used by `scripts/ci.sh wasm` and CI must match the `wasm-bindgen`
  version in `Cargo.lock`; CI reads it from there.

## Public API Policy

- Prefer fallible graph and registry construction in production code: `try_connect`, `try_connect_ports`, `try_merge`, `try_on`, and related helpers.
- Prefer typed `PortHandle`s or explicit `(node, port)` tuples for graph wiring. Plain host-port strings such as `"input"` are fine at host boundaries; dotted strings such as `"node.output"` are shorthand and should stay in tests, small demos, or compile-time-fixed graphs.
- Panic-first helpers are acceptable in tests, small demos, and compile-time-fixed graph construction.
- Public ids, type keys, package descriptors, and telemetry fields should remain deterministic because planner/runtime goldens and FFI fixtures depend on stable serialization.
- Host-facing queues should be bounded deliberately; internal finite graph edges default to compatibility-oriented FIFO behavior unless a graph or runtime config selects otherwise.

## Observability

- Initialize a `tracing` subscriber in binaries and integration tests. The runtime's, engine's
  and planner's spans and events need the facade's `tracing` feature: on by default, so
  `default-features = false` builds (`embedded`, wasm) list it explicitly to keep them.
- Useful targets include `daedalus_runtime::executor`, `daedalus_runtime::executor::queue`, `daedalus_runtime::host_bridge`, `daedalus_runtime::stream`, `daedalus_runtime::config`, `daedalus_planner::passes`, `daedalus_gpu::wgpu`, `daedalus_gpu::dispatch`, `daedalus_gpu::readback`, and `daedalus_gpu::poll_driver`.
- Metrics levels are `Off`, `Basic`, `Timing`, `Detailed`, `Hardware`, `Profile`, and `Trace`.
- `Detailed` is the normal level for transport and allocation debugging.
- `Profile` adds per-node profile snapshots.
- `Trace` records lifecycle-level data movement details.
- `DAEDALUS_NODE_CPU_TIME=1` enables Linux per-node CPU timing.
- `DAEDALUS_NODE_PERF_COUNTERS=1` enables Linux perf counters.

## Runtime Defaults

- Stream workers use `DEFAULT_STREAM_IDLE_SLEEP` unless configured through `EngineConfig`, `RuntimeSection`, or `StreamWorkerConfig`.
- Host bridge event recording is off by default (`DEFAULT_HOST_BRIDGE_EVENT_RECORDING = false`) because it allocates one event per push and delivery. Enable it with `EngineConfig::with_host_event_recording(true)`, `HostBridgeConfig::with_event_recording(true)`, `HostBridgeHandle::set_event_recording(true)`, or `DAEDALUS_HOST_EVENT_RECORDING=1`; when enabled it retains `DEFAULT_HOST_BRIDGE_EVENT_LIMIT` events per bridge. Stats counters and `daedalus_runtime::host_bridge` tracing warnings for drops/replacements (with the `tracing` feature) are always on.
- Host ports with a replace-style capacity-one policy (the default `Bounded { capacity: 1, overflow: DropOldest }`, `LatestOnly`, `DropOldest`, `Coalesce`) store their value in a single slot that is overwritten in place.
- Internal edge queues preserve compatibility defaults; streaming, camera, daemon, and interactive workloads should set explicit bounded/latest-only policies.
- WGPU staging behavior is configured through `WgpuStagingPoolConfig` or `DAEDALUS_WGPU_STAGING_*` before backend construction.

## Minimal CPU-Only Profile

For constrained hosts (embedded boards, sidecar engines), depend on the facade with the
`embedded` preset:

```toml
daedalus = { package = "daedalus-rs", version = "3.0.0", default-features = false, features = ["embedded"] }
```

`embedded` is `engine` + `plugins` + `threads`: the facade's `engine` feature does not enable the
Rayon executor pool or metrics collection, and with default features off `tracing` is off too
(add the `tracing` feature to keep the runtime's spans and events). Add `dylib-plugins` only if
the host loads native plugin libraries. Leave every `gpu*` feature off; `EngineConfig`'s default
`GpuBackend::Cpu` and `planner.enable_gpu = false` need no GPU feature. This set links no
`wgpu`, `image`, `tokio`, `styx`, `rayon`, `crossbeam` or `tracing`; its heaviest runtime
dependency is `serde_json`.

Feature semantics without the extras:

- No `executor-pool`: `RuntimeMode::Parallel` and `RuntimeMode::Adaptive` still work; parallel
  runs fan out to a few persistent threads (one fewer than the workers, as the calling thread
  takes part) started on the first parallel run and parked between runs, instead of a Rayon
  pool. A frame spawns no threads either way.
- No `metrics`: the telemetry APIs (`MetricsLevel`, `ExecutionTelemetry`) still compile, but
  executors record no per-node or transport metrics. Add `metrics` when you read telemetry.

Applications that do not need to minimize dependencies should use `engine-full`
(`engine` + `executor-pool` + `metrics`).

### Dependency weight

A probe binary depends on the facade with `default-features = false` and the features below,
installs one plugin, compiles a one-node host graph and runs it 10,000 times (x86_64 Linux,
rustc 1.94.0 (not re-measured on 1.99.0), release, `lto = "thin"`, `codegen-units = 1`, `strip = true`, each preset built cold
in its own target directory). Crates counts the distinct packages of `cargo tree -e normal` for
x86_64 Linux, the probe included; peak RSS is the median `VmHWM` of three runs. Measured with
the machine at load 18-54, which affects build time only.

| Preset (facade features) | Crates | Stripped binary | Peak RSS | Threads |
| --- | --- | --- | --- | --- |
| `engine,plugins` (no `threads`) | 48 | 3.2 MiB | ~4.9 MiB | 1 |
| `embedded` | 48 | 3.2 MiB | ~5.0 MiB | 1 |
| `embedded,tracing` | 52 | 3.3 MiB | ~5.1 MiB | 1 |
| `engine-full,plugins` + defaults (`threads,tracing`) | 57 | 3.3 MiB | ~5.1 MiB | 1 |
| ... + `dylib-plugins` | 60 | 3.3 MiB | ~5.2 MiB | 1 |
| ... + `gpu-wgpu` | 94 | 7.0 MiB | ~6.2 MiB | 1 |
| ... + `dylib-plugins,gpu-dmabuf,gpu-async,gpu-gles,gpu-image,schema,proto` | 112 | 8.0 MiB | ~6.4 MiB | 1 |

What the optional features add on top of `engine-full,plugins`:

- `tracing`: `tracing`, `tracing-core`, `pin-project-lite`, `once_cell` (4 crates).
- `dylib-plugins`: `daedalus-ffi-host`, `daedalus-ffi-core` and `libloading`. The FFI crates'
  `integrity` feature (`sha2` and seven helper crates, package hashing) stays off.
- `gpu-wgpu`: wgpu with its Vulkan backend (`wgpu-core`/`-hal`/`-types`, `naga`, `ash`,
  `gpu-allocator`, `renderdoc-sys`, which wgpu always enables on native targets, ...) and
  `half`/`bytemuck`. `gpu-gles` adds `glow`, `khronos-egl`, `wayland-sys` and `dlib`;
  `gpu-image` adds `image`, `png`, `flate2`, `miniz_oxide`, `fdeflate`, `moxcms`, `pxfm`,
  `crc32fast`, `simd-adler32`, `adler2` and `byteorder-lite`.

Linear graphs stay on the serial execution path, so no preset starts worker threads for this
workload; the pool mainly costs crates and binary size. Code a binary does not call (the plugin
loader, GPU backends) is mostly removed by LTO, so crates track build time more than binary
size. Re-measure on the target board before budgeting.

## Portability

Daedalus targets hosted operating systems, but keeps two smaller targets buildable so they can
grow: microcontrollers (`no_std`) and WebAssembly. CI checks both (`scripts/ci.sh nostd wasm`,
see [testing.md](testing.md#no_std-and-wasm)). Nothing changes for `std` users: every crate
keeps `std` in its default features, and a native build compiles the same types as before.
For small microcontrollers there is also the [MCU profile](mcu.md): graphs planned on the host at
build time and run from generated code without a heap, in a few KiB of flash (`scripts/ci.sh
mcu` links the firmware and checks its size).

What builds where (CI-checked unless noted):

| Crates | Hosted (`std`) | `thumbv7em-none-eabihf` (CAS) | `thumbv6m-none-eabi`, `riscv32imc-unknown-none-elf` (no CAS) | `wasm32-unknown-unknown`, `wasm32-wasip1` |
| --- | --- | --- | --- | --- |
| core, transport, data, registry, planner | yes | `no_std` + alloc features | `no_std` + alloc features | yes (via the facade) |
| runtime, engine | yes | serial, `no_std`; `plugins`, `metrics`, `snapshots`, `lockfree-queues`, `config-env`, `tracing` | serial, `no_std`; same features except `tracing` | serial (`embedded` without `threads`) |
| `examples/nostd_smoke` | tests run natively | yes | yes | - |
| MCU profile: `daedalus-mcu`, `examples/mcu_blink` ([mcu.md](mcu.md)) | tests run natively | `no_std`, no `alloc`; firmware linked and size-checked | same | - |
| facade `daedalus-rs` | yes | - | - | `engine,plugins` (+ `tracing`) |
| gpu, nodes, ffi, daemon | yes | - | - | - |

`riscv32imc` is checked locally, not in CI; it needs the same `portable-atomic` backend as
`thumbv6m`.

### Tier 1: `no_std` + `alloc`

`daedalus-core`, `daedalus-transport`, `daedalus-data`, `daedalus-registry` and
`daedalus-planner` have a default `std` feature. With `default-features = false` they are
`#![no_std]` and need only `alloc` (checked on `thumbv7em-none-eabihf`, a Cortex-M4F, and on
`thumbv6m-none-eabi`, a Cortex-M0 without compare-and-swap; see below). That covers ids, payloads and lineage, the type and value
model with its JSON codec, registry declarations, and planning (`GraphDocument` parsing,
validation, type checking, scheduling): a device can receive, check and plan graphs. Their
optional features (`json`, `schema`, `proto`, `bundle`, `plugin`, `metrics`, ...) work without
`std`; `gpu` and core's `async-channels` imply it.

What `std` switches:

- Locks and lazy globals: `std::sync::{Mutex, OnceLock}` with `std`, `spin` without, through the
  crate-private `portable` module (`crates/portable.rs`, symlinked into each crate's `src/` like
  `crates/build_features.rs`). `data` and `planner` keep `parking_lot` locks only with `std`.
- Hash maps keyed by `TypeId` (`data`'s type registry, `transport`'s boundary vtables): `std`'s
  `HashMap` with `std`, `hashbrown` without. Planner passes use `BTreeMap`/`BTreeSet`.
- 64-bit counters: `AtomicU64`, from `portable-atomic` on targets without 64-bit atomics.
- Time: [`daedalus_core::platform`](../crates/core/src/platform.rs) exports `Instant`, which is
  `std::time::Instant` wherever the target has an OS clock and otherwise a portable instant whose
  platform reading (`Instant::now`) is zero, and `Clock`, the injectable clock (see "Tier 2"
  below). There is no process-wide clock to install.
- `std`-only: the planner's `DAEDALUS_TRACE_EMBEDDED_EXPAND` variable (its `tracing` feature,
  default on, implies `std`) and its two binaries.

Workspace wiring: shared dependencies that the tier is built from (`serde`, `serde_json`,
`thiserror`, `tracing`, `base64`, `crossbeam-queue`) and the five crates themselves are declared
in `[workspace.dependencies]` without default features, so `std` crates request
`features = ["std"]` (`daedalus-data`: `["std", "json"]`). `std` is a boundary feature in the
plugin build fingerprint because it swaps lock and map types; `daedalus-transport` is
fingerprinted now that it has a feature. `serde`'s `rc` feature is not on workspace-wide:
`daedalus-core` enables it where the target has compare-and-swap.

#### Targets without compare-and-swap

`thumbv6m-none-eabi` (Cortex-M0/M0+) and `riscv32imc-unknown-none-elf` have atomic loads and
stores but no compare-and-swap, so `alloc::sync` (`Arc`) does not exist there and `spin` and
`crossbeam-queue` cannot use `core` atomics. On those targets (`cfg(not(target_has_atomic =
"ptr"))`), and only there, the tier-1 crates take their atomics and `Arc` from the `portable`
module's other backend:

- `AtomicBool`/`AtomicUsize`/`AtomicU64` from `portable-atomic`, and `spin` with its
  `portable_atomic` feature, both over `portable-atomic`'s `critical-section` feature;
- `Arc` is `portable_atomic_util::Arc`, public as `daedalus_core::platform::Arc` (it is
  `alloc::sync::Arc` elsewhere): build values for Daedalus APIs that take an `Arc` with it. It
  cannot unsize-coerce on stable Rust, so internal `Arc<dyn Trait>` values are built through a
  `Box` there (`portable::arc_dyn!`, one extra allocation);
- `daedalus-core`'s bounded and unbounded channels use locked `VecDeque`s instead of
  `crossbeam-queue`, which has no queues there.

The final binary must provide a `critical-section` implementation, e.g. `cortex-m`'s
`critical-section-single-core` or `riscv`'s `critical-section-single-hart` feature, or a HAL's.
The dependencies are target-specific, so every other target compiles exactly what it did
before. The runtime and engine take the same backend there (their `portable` module and
`daedalus_runtime::sync`'s `spin` locks), so an application linking them for such a target
needs that `critical-section` implementation too. Also on those targets only:

- `tracing` is unavailable (`tracing-core` needs compare-and-swap); leave the runtime's and
  engine's `tracing` feature off (see "Tier 2");
- `lockfree-queues` keeps bounded edges on locked queues (`crossbeam-queue` has no
  `ArrayQueue` there), so the feature compiles but changes nothing;
- the runtime's internal `Arc<dyn Fn>` handlers and plugin codecs go through `arc_dyn!` (one
  extra allocation per registration, none per run).

### Tier 2: `no_std` serial runtime and engine

`daedalus-runtime` and `daedalus-engine` are `#![no_std]` + `alloc` without their `std` feature
(implied by the default `threads`): plan a graph, run it on the serial executor and drive it
through host bridges or a `HostGraph` on a microcontroller. CI checks both for
`thumbv7em-none-eabihf` with and without `plugins`, `metrics`, `snapshots`, `lockfree-queues`
and `config-env`, and [`examples/nostd_smoke`](../examples/nostd_smoke/src/lib.rs) (a
`#![no_std]` crate) runs `host.in -> inc -> host.out` through `Executor::run_in_place` and
through `Engine`/`HostGraph`, checked for that target and tested natively with every Daedalus
crate's `std` off. The same checks run for `thumbv6m-none-eabi` (no compare-and-swap, see
above), without `tracing`.

What the runtime's and engine's `std` switches (`std` is a boundary feature in the plugin
fingerprint; `dylib-plugins` turns it on):

- Locks: [`daedalus_runtime::sync`](../crates/runtime/src/sync.rs), `lock_api` types over
  `parking_lot` with `std` and over `spin` without (on `portable-atomic` without
  compare-and-swap, as above). The API is the same either way; `Condvar` exists only with `std`.
- Hash maps: [`daedalus_runtime::collections`](../crates/runtime/src/lib.rs) (`HashMap`,
  `HashSet`, the types in runtime signatures) are `std`'s with `std` and `hashbrown`'s without.
- The per-thread port buffer pool (`NodeIo` input/output lists) is a `thread_local!` with `std`
  and one `spin`-locked pool without (taken with `try_lock`, so a contended or interrupted
  caller allocates instead of waiting).
- `std`-only: Linux perf counters and thread CPU time (`RuntimeDebugConfig::node_perf_counters`/
  `node_cpu_time` read nothing elsewhere), `RuntimeDebugConfig::from_env` and
  `EngineConfig::from_env` (`config-env` without `std` keeps the serde config types only), and
  the host bridge's `Condvar`. `libc` is a Linux-only optional dependency.
- `std` forwards `std` to core, transport, data, registry, planner, `serde`, `serde_json`,
  `thiserror` and (when enabled) `tracing`; without it the whole tree is `alloc`-only.

The other switches:

- Tracing: the `tracing` feature (default on, independent of `std`, host-only in the
  fingerprint; the engine's enables the runtime's, the facade's also the planner's) keeps the `tracing` spans and events. Without it the
  crate's `trace` module (`crates/trace.rs`, symlinked into each crate's `src/` like
  `portable.rs`) expands `trace!`/`debug!`/`warn!`/`error!` to nothing, without evaluating their
  arguments, and `debug_span!` to an inert span, so `tracing` is not a dependency. It builds
  without `std` where the target has compare-and-swap (`thumbv7em`).
- Threads: the `threads` feature (default on `daedalus-runtime`, `daedalus-engine` and the facade,
  implies `std`, host-only in the fingerprint, kept by the `embedded` preset) compiles the worker
  pool and persistent workers, stream workers (`StreamGraph::spawn_continuous*`,
  `StreamGraphWorker`) and blocking waits (`InboundWaiter::wait`,
  `HostBridgeHandle::{wait_inbound, recv_payload_timeout}`, `GraphOutput::recv_timeout`,
  `HostGraph::{wait_for_input, tick_on_input, drive_blocking}`). Without it those do not exist,
  and `RuntimeMode::Parallel`/`Adaptive` resolve one worker and run serially (same results, same
  telemetry shape). Drive graphs with `HostGraph::tick`/`tick_if_ready`, the async
  `HostGraph::drive`/`.await` on `InboundWaiter` (a `core::task` future), host bridge
  `push*`/`try_pop*`/`drain*`, or `StreamGraph::poll`/`run_available`. Enabling `threads` on a
  wasm target without threads (`wasm32-unknown-unknown`, `wasm32-wasip1`) is a compile error.
- Clock: [`platform::Clock`](../crates/core/src/platform.rs) is what runtime and engine time
  reads: the platform clock by default (`Instant::now`, so native timing is unchanged: one
  predictable branch), or `Clock::new(|| elapsed_since_origin)` set per engine with
  `EngineConfig::with_clock` (or `Executor`/`OwnedExecutor`/`StreamGraph::with_clock`,
  `HostBridgeManager::set_clock`). Telemetry, adaptive segment costs, edge timings, stream
  execution and `HostGraph` step timings use it, and so does payload lineage the runtime
  creates: `NodeIo` pushes, host bridge `push*`, `GraphInput::feed_typed` and `HostGraph` direct
  lanes stamp `created_at` with it (`Payload::stamp`, a no-op on the platform clock, so no second
  reading natively). Host-bridge events are timestamped on the bridge clock and
  `FreshnessPolicy::MaxAge` ages payloads on it (`PayloadLineage::age`). Payloads built outside
  the runtime (`Payload::owned`, ...) read the platform clock; stamp them with the engine's clock
  (`handle.clock()`, `io.clock()`) before `feed_payload`/`push_payload` when it is a custom one.
  Engines set their clock on the bridges they create; a `HostBridgeManager` you pass to
  `Engine::execute_with_host` keeps its own.

### WebAssembly

`wasm32-unknown-unknown` has `std` but no threads and no clock (`std::time::Instant::now` and
thread spawning panic there). The `embedded` facade preset without `threads` (`engine,plugins`
with default features off) builds and runs on it: CI runs the
[`examples/wasm_smoke`](../examples/wasm_smoke/src/lib.rs) module, a fan-out graph in every
runtime mode timed by an injected counter `Clock`, and the
[`examples/wasm_bindgen_host`](../examples/wasm_bindgen_host/src/lib.rs) `wasm-bindgen` module in
Node. Behavior there:

- No `threads`: `Parallel` and `Adaptive` run serially and the thread-only APIs are absent (see
  above). Locks are `spin` locks.
- `platform::OS_CLOCK` is `false`: pass the host's clock to the engine,
  `EngineConfig::with_clock(Clock::new(|| Duration::from_secs_f64(performance_now_ms() / 1e3)))`;
  it also stamps payload lineage and drives `FreshnessPolicy::MaxAge` (see "Clock" above).
  Without a clock, durations are zero (telemetry reads zero, adaptive mode stays serial, and
  `MaxAge` never drops).
- The embedded dependency tree has no `getrandom` or other OS-bound crate. `dylib-plugins`,
  `gpu*`, `executor-pool` (Rayon) and the FFI crates are not supported on wasm.

Host glue: `examples/wasm_bindgen_host` is the pattern for a JavaScript host. It imports
`performance.now()` through `wasm-bindgen` (`#[wasm_bindgen(js_namespace = performance, js_name =
now)]`), passes it to the engine as `Clock::new`, and exports a
`Pipeline` class whose `push`/`tick`/`take` wrap `HostGraph`; `tick` returns the tick's graph
duration on that clock (the example enables `metrics`). Bind it with
`wasm-bindgen --target nodejs` (or `web`/`bundler` for browsers); the `wasm-bindgen` CLI must
match the crate version in `Cargo.lock`.

`wasm32-wasip1` has `std` and a clock (`OS_CLOCK` is `true`, `Instant` is `std`'s) but no
threads: the same preset builds there, and CI runs the `daedalus-wasi-smoke` command (the smoke
graph on the platform clock) under Node's WASI. A WASI runtime such as `wasmtime` runs it too.

### Rules for new code

- Tier-1 crates: no `std::` paths outside `#[cfg(feature = "std")]` items and tests. Use `core::`
  and `alloc::` (import `String`, `Vec`, `Box`, `ToString`, ... from `alloc`; `format!` and
  `vec!` come from `#[macro_use] extern crate alloc` without `std`). Exported macros name
  `alloc` types through `$crate::__private`, since the calling crate may lack `extern crate
  alloc`.
- No new process-global state. Where one is unavoidable (registries, interning), use
  `crate::portable::{OnceLock, Mutex}` and keep hot paths free of it.
- Tier-1 crates take `Arc`, `Weak` and atomics (`AtomicBool`, `AtomicUsize`, `AtomicU64`) from
  `crate::portable`, never `alloc::sync`/`core::sync::atomic` (`Ordering` is fine), and build an
  `Arc<dyn Trait>` with `portable::arc_dyn!` (not `Arc::new` plus coercion). Trait methods cannot
  take `self: Arc<Self>`.
- OS facilities (files, environment, processes, threads, sleeping, blocking waits, clocks) sit
  behind `std` in tier-1 crates, the runtime and the engine. In the runtime and engine, read time
  from the executor's `Clock` (`clock.now()`, `clock.elapsed(start)`; never
  `Instant::now()`/`Instant::elapsed()` outside thread-only code), stamp payloads the runtime
  builds with it (`Payload::stamp`), lock through `crate::sync`/`daedalus_runtime::sync` (never
  `parking_lot` directly), and put anything that spawns threads or blocks behind
  `#[cfg(feature = "threads")]`.
- Runtime and engine follow the tier-1 rules too: `use crate::prelude::*` for `String`, `Vec`,
  `Box`, `ToString` and the `HashMap`/`HashSet` of `daedalus_runtime::collections` (never
  `std::collections`), `Arc` and atomics from `crate::portable`.
- A new dependency of a tier-1 crate must support `no_std`: declare it in the workspace without
  default features and enable its `std` feature from the crate's `std` feature.
- Runtime and engine emit spans and events through `crate::trace::{trace, debug, warn, error,
  debug_span}!`, never `tracing::` directly. Parameters used only by an event get
  `#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]`.
- Run `scripts/ci.sh nostd wasm` after touching these crates or the executor.

### Roadmap

Tier 2 (a `no_std` serial runtime and engine) is done, with and without compare-and-swap. The
tracking list is in [TODO.md](../TODO.md) under "Portability (tier 2)". Not covered: `tracing` on
targets without compare-and-swap, and a linked, flashed firmware image of the tier-2 runtime (CI
type-checks the bare-metal targets and runs the smoke graph natively; the [MCU profile](mcu.md)
firmware is linked and measured, not flashed).

## Performance

`cargo bench -p daedalus-engine --features plugins --bench host_graph_drive` measures the host
bridge and a one-node `HostGraph` (`host.in -> inc -> host.out`, serial mode). Save a baseline
with `-- --save-baseline <name>` and compare with `-- --baseline <name>`. The
`hot_path_allocations` engine test pins the metrics-off round trip at 2 heap allocations (the
input and output payloads, one each); it was 31 before the hot-path pass and 4 before the
tick-cost pass below.

Hot-path pass (x86_64 Linux, shared 24-core machine at load ~6-8, criterion medians; before is
the `helios-integration` tree benched back to back with after, so treat differences under ~10%
as noise):

| Benchmark | Before | After |
| --- | --- | --- |
| bridge inbound push + take (small) | 607 ns | 499 ns |
| bridge outbound push + pop (small) | 632 ns | 550 ns |
| `push_tick_take` (`MetricsLevel::Basic`) | 3.43 µs | 2.47 µs |
| `push_tick_take_metrics_off` | 3.20 µs | 2.15 µs |
| latest-only 100-push burst + tick | 56.2 µs | 49.0 µs |
| bridge push + take, events off / on | 654 / 694 ns | 504 / 562 ns |

What the tick no longer does: look bridges up through `HostBridgeManager` (host nodes are
resolved in `with_host_bridges`), copy the schedule order, clone the `RuntimeNode`, allocate
node-id and port-name strings (`PortId`/`TypeKey` literals are `&'static str`, edge and const
ports are pre-built), collect host nodes/ports into vectors, snapshot the executor twice, or read
the clock when the metrics level does not use it. `Payload` construction dropped from four
allocations to two (single `Arc<dyn PayloadStorage>`, no empty residency map, no global
boundary-registry clone). The inbound bench now drains into a reused `Vec`
(`take_inbound_into`).

### Graph frame allocations

`cargo test -p daedalus-rs --features engine,plugins --test graph_frame_allocations` drives a
16-node detector-like graph (`crates/daedalus/tests/support/detector_graph.rs`: one frame input
fanned out, config-struct and const inputs (one config with a serde enum and a `String` field),
a five-input node, a metadata-only adapter edge,
connected and unconnected `Option<T>` inputs, two conditional producers, one of which never
emits so its consumer is skipped, fan-in, four host outputs) whose handlers allocate nothing,
and asserts the allocations per frame in serial (metrics off and basic), parallel and adaptive
modes. Set `DAEDALUS_ALLOC_TRACE=1` (with `-- --nocapture --test-threads 1`) to print them per
call site, grouped by the first Daedalus frame and its callers.
`cargo bench -p daedalus-rs --features engine-full,plugins --bench graph_frame` times the same
frame.

Allocations per frame at five points: `dev` at the optional-inputs merge (A), `dev` after the
macro work resolved output keys once per handler (B), with the executor/transport pass (C), with
the parallel scheduling pass (D), and with per-node state slots and decoded-constant caches (E,
whose harness adds a serde enum and a `String` config field; `track` takes its config as
`&TrackConfig`, since a by-value config clones its `String`). A and B carry the two harness
fixes described below.

| Category | A | B | C | D | E |
| --- | --- | --- | --- | --- | --- |
| Boundary contract formatting (`get_ref`/`try_into_owned`/`Payload::owned`) | 180 | 164 | 0 | 0 | 0 |
| Adapter lifecycle records, step names, path text | 54 | 54 | 0 | 0 | 0 |
| Output port names (`PortId::new` per push) | 28 | 0 | 0 | 0 | 0 |
| Const input payloads rebuilt per tick | 14 | 14 | 0 | 0 | 0 |
| Builtin const coercion boxing | 7 | 7 | 0 | 0 | 0 |
| `StateStore` take/set of node state | 6 | 6 | 0 | 0 | 0 |
| Failed moves boxing the payload (`try_into_owned` on a const) | 0 | 2 | 0 | 0 | 0 |
| Port lists spilling past four entries | 2 | 2 | 0 | 0 | 0 |
| Host input fan-out target list | 1 | 1 | 0 | 0 | 0 |
| Payloads created (node outputs, adapter results, branch, host frame) | 31 | 31 | 31 | 31 | 31 |
| Config decoding (E's serde enum and `String` fields, decoded per frame without the cache) | - | - | - | - | 0 |
| Generated handler code (`daedalus-macros`: per-push keys in A, state keys until E) | 55 | 9 | 9 | 9 | 0 |
| **Serial, metrics off** | **378** | **290** | **40** | **40** | **31** |
| Serial, basic metrics (per-node metrics: a `BTreeMap` until C, one vector in D) | 381 | 293 | 43 | 41 | 32 |
| Parallel/adaptive with `executor-pool` (C: a pool task per segment and a result channel) | 422 | 334 | 60 | 40 | 31 |
| Parallel/adaptive without `executor-pool` (C: a scoped OS thread per segment) | 458 | 370 | 141 | 40 | 31 |

In D a parallel frame fans out once to persistent workers (the Rayon pool, or without
`executor-pool` a few parked threads of the executor's own) that pull ready segments from one
locked queue, each reusing one executor snapshot, so dispatch allocates nothing; the test allows
one allocation per frame for amortized growth (Rayon's injector blocks, a node's first run on a
worker growing that thread's port buffers). The test binary is unoptimized, where the graph's
nodes are slow enough that adaptive mode runs it in parallel; optimized, it runs serially.

Each created payload is one allocation, the value's own `Arc`, which is the payload's storage
(`ArcValue<T>`, retyped in place); a handler returning an `Arc` it already holds allocates
nothing (until the tick-cost pass, two and one: a `TypedStorage` wrapper around the value's
`Arc`, which made the serial metrics-off frame 31 allocations; it is 14 now). Boundary contracts
are registry-scoped and checked when a graph is compiled, and only `Payload::boundary_owned`
builds contract-restricted storage.

Timings, B against C (x86_64 Linux, shared 24-core machine at load 20-30, criterion medians, back
to back):

| Benchmark | B | C |
| --- | --- | --- |
| `graph_frame/serial_metrics_off` | 31.7 µs | 17.5 µs |
| `graph_frame/serial_metrics_basic` | 38.8 µs | 21.0 µs |
| `graph_frame/parallel_metrics_off` (pool) | 203 µs | 129 µs |
| `graph_frame/adaptive_metrics_off` (pool) | 191 µs | 154 µs |
| `push_tick_take` (one node, basic metrics) | 1.90 µs | 1.57 µs |
| `push_tick_take_metrics_off` | 1.50 µs | 1.32 µs |

C against D, with (`engine-full,plugins`) and without (`engine,plugins`: no pool, no metrics)
`executor-pool`, same machine at load 17-30, so differences under ~15% are noise:

| `graph_frame/…` | C pool | D pool | C no pool | D no pool |
| --- | --- | --- | --- | --- |
| `serial_metrics_off` | 16.8 µs | 17.5 µs | 16.2 µs | 16.0 µs |
| `serial_metrics_basic` | 30.8 µs | 20.2 µs | 16.2 µs | 16.7 µs |
| `parallel_metrics_off` | 157 µs | 74 µs | 659 µs | 69 µs |
| `adaptive_metrics_off` | 136 µs | 20 µs | 554 µs | 16.6 µs |

The harness needed two fixes to run at all: owned scalar parameters fed by const inputs
(`NodeIo::take_owned` coercing `Value`s) and builtin branch adapters for keys shared by several
Rust types (a fanned-out `i64` output was branched by the `i32` adapter; since E every builtin
number has its own key).

### Frame-path overhead

`cargo run --release -p daedalus-frame-bench --example frame_chain` drives a host graph the way
a camera host does: a synthetic 640x480 GRAY8 frame in external memory (a dma-buf from
`/dev/dma_heap/system` here, `memfd` when there is no heap) pushed as a `daedalus:frame` payload,
a chain of N no-op stages that read the frame through `FrameView` and pass it on, and the frame
taken at the host output; `MetricsLevel::Off`, serial mode. `interface` feeds `daedalus:frame`
handles; `owner` feeds the frame type, so the provider's `View` adapter runs on the first edge.
`cargo bench -p daedalus-frame-bench --bench frame_chain` times the same frame with criterion
(see "Frame-Path Overhead" in [runtime-diagnostics.md](runtime-diagnostics.md) for the report
rows).

**Host numbers: x86_64 (AMD Ryzen 9 5900X, shared 24-core machine at load ~40), not the CM5.**
On-device numbers are a TODO. Push + tick + take per frame, p50 of 20000 frames
(`FRAME_CHAIN_TICKS=20000`), frame-overhead recording off:

| Stages | `interface` | `owner` |
| --- | --- | --- |
| 1 | 1.25 µs | 1.43 µs |
| 4 | 2.24 µs | 2.42 µs |
| 16 | 5.95 µs | 6.29 µs |
| fit | 0.96 µs + 0.31 µs per stage | 1.12 µs + 0.32 µs per stage |

Criterion medians on the same machine: `interface` 1.32 / 2.35 / 9.2 µs (the 16-stage run was
noisy, 8.5-10.1 µs), `interface+overhead` (recording on) 1.85 / 3.54 / 10.19 µs for 1 / 4 / 16
stages, so recording costs about 0.5 µs plus 0.2 µs per stage. Per-frame counters at steady
state: no copies, no GPU transfers and no runtime, node or host allocations for either feed
(asserted by `crates/frame-bench/tests/frame_chain_overhead.rs`, also for an owner feed fanned
out to 16 consumers and for typed `FrameView` + `&T` nodes). The `owner` feed's extra ~0.18 µs
is the `View` adapter path on the first edge (about 130 ns p50 with recording on); the adapter
retypes the payload in place (`Payload::provide_foreign`). It used to build an 88-byte
`ForeignHandle` payload per consumer edge per tick (`owner` p50 1.50 / 2.47 / 6.26 µs then,
measured alongside the numbers above, with the adapter at about 220 ns).

Breakdown of the 4-stage `interface` chain with recording on (p50, ns): push 110, tick 2650 =
inject 150 + inputs 480 + handlers 330 + node_io 830 + drain 190 + dispatch 670, take 120,
`graph_overhead` 2320, queue wait 820 (about 165 per edge). Per stage the runtime adds roughly
120 of input collection, 210 of node framing and 170 of dispatch around a handler that itself
takes about 80 (reading the frame through the vtable, timed). Against the Eidos stages
(about 0.96 ms p50 each on the CM5) that is well under 1%; the absolute numbers on the CM5's
Cortex-A76 cores will be higher.

### Detector-shaped frame (tick-cost pass)

`cargo run --release -p daedalus-frame-bench --example detector` drives a graph shaped like a
staged marker detector (`crates/frame-bench/src/detector.rs`, the parameter shapes of Eidos's
ArUco/AprilTag stages): five typed `#[node]` stages (mask prep, quads, decode, validate, refine)
with `Copy` config structs fed by constants (enum, integer, float and bool fields, 34 const
ports), state, the `ExecutionContext` and `Arc`'d struct outputs (validate returns two), the
owner frame fanned out to four stages (mask prep reads pixels through
`FrameView::plane_bytes`), three host outputs. It runs as the flat per-stage graph and as one
group node (an `EMBEDDED_GRAPH_KEY` node declaration the planner expands into the same stages).
`crates/frame-bench/tests/detector_overhead.rs` asserts, for both, no copies and zero runtime,
node and host allocations per frame, and that the group expands to the flat graph's edges.
`run_frame_bench` reports user-space instructions per frame (`perf_event_open`, push + tick +
take including the five handlers).

**x86_64 numbers (AMD Ryzen 9 5900X, shared 24-core machine at load 25-35, pinned to one core,
20000 frames, median of three interleaved runs per build), not the CM5.** Before is `dev` at
the `daedalus:frame` v2 merge (ed7ddde) with this bench. Instructions per frame with recording
off; stage rows are frame-overhead p50 / p99 in ns with recording on:

| | flat before | flat after | group before | group after |
| --- | --- | --- | --- | --- |
| instructions per frame | 66 033 | 33 117 | 67 455 | 33 167 |
| node allocations per frame | 6 (384 B) | 0 | 6 (384 B) | 0 |
| push + tick + take, recording off | 8 190 / 18 291 | 4 730 / 9 550 | 8 340 / 19 280 | 4 650 / 9 581 |
| inject | 460 / 790 | 350 / 490 | 510 / 910 | 350 / 500 |
| inputs | 1 690 / 2 550 | 890 / 1 120 | 1 700 / 2 600 | 890 / 1 180 |
| handlers (generated code included) | 3 510 / 6 310 | 2 120 / 3 430 | 3 530 / 6 460 | 2 090 / 3 630 |
| node_io | 1 640 / 2 510 | 1 160 / 1 580 | 1 710 / 2 630 | 1 170 / 1 710 |
| drain | 520 / 890 | 410 / 630 | 540 / 890 | 410 / 620 |
| dispatch | 880 / 1 620 | 550 / 920 | 970 / 1 800 | 550 / 930 |
| `graph_overhead` (tick - handlers) | 5 310 / 10 320 | 3 490 / 4 880 | 5 550 / 11 290 | 3 510 / 5 310 |

What the tick no longer does, in the order the pass removed it:

- **Payload wrapper allocations.** Wrapping a handler's `Arc<T>` output allocated a 64-byte
  `TypedStorage`: the six allocations per frame the node scope reported. A payload's storage is
  now the value's own allocation.
- **Hash lookups per node and edge.** The direct-edge set (a `HashSet` probed per edge) and the
  host-bridge test (a metadata `BTreeMap` lookup per node, slower in group nodes, whose metadata
  has more keys) are per-index masks; handler dispatch and host-port maps use an unseeded Fx
  hash instead of SipHash.
- **Queues for edges that never hold more than they are given.** Edges whose target port has
  one producer use direct slots for every slot-compatible policy: the default buffer-all (the
  slot keeps every payload in order, one inline), latest-only, coalescing and a bounded queue of
  one dropping the oldest, fanned-out ports and adapter edges included (adapters run when the
  consumer collects).
- **Per-call clones.** Each node's `ExecutionContext` and `NodeIo` environment are built once
  (rebuilt when the state store, capabilities, GPU, coercers, type index or clock change); a
  call borrows the context and clones two `Arc`s for its io instead of about a dozen.
- **Per-call node-state hashing.** Generated handlers keep state and decoded configs/constants
  as one tuple in a per-node `NodeStateSlot` the context resolves once: one slot lock to take
  and one to store, instead of a SipHash of the node id and a write lock per part.
- **Const copies.** A node's const inputs are one shared list read by each call's `NodeIo`
  (34 `PortId` and payload clones per frame before), and `ConfigCache` skips its per-port check
  while a call shares the list the config was decoded from.
- **Per-tick snapshots.** `OwnedExecutor::run_in_place` borrows its own core instead of cloning
  ~25 handles and building fresh telemetry; `ExecutionTelemetry` is several hundred bytes
  smaller (plain vectors for warnings and errors).

**Group node against the per-stage template.** On the CM5 the group measured 35 µs against the
template's 27 µs. Runtime overhead explained little of it: the expanded group runs the same five
nodes and eleven edges (asserted) and cost 2% more instructions before this pass (the per-node
host-bridge metadata lookup over the group's larger metadata maps, now a mask), the same after.
The rest is measurement order: the Eidos bench ticks the group right after the bare stages
(about 1 ms of image processing that evicts the runtime's code and data from L1/L2) and the
template right after the group, warm. Replaying that loop on x86 (a 8 MiB memory sweep, then
graph A, then graph B, per frame) gave, before this pass, 6.4 µs for whichever graph ran first
against 5.5-5.8 µs for the second (5.2 / 5.0 µs without the sweep); after it, 4.0-4.2 µs first
against 3.8-4.0 µs second (3.5 / 3.5 µs without). Fewer instructions and less data touched per
tick shrink the cold-cache penalty with them; on the CM5's smaller caches the first-run graph
still pays more, so compare graphs in alternating order or one per run.

**CM5 projection (an estimate, not a measurement).** Scaling the CM5 rows by the x86 ratios
(`graph_overhead` p50 x0.66, p99 x0.47; instructions x0.50), the per-stage template's 27 µs
`graph_overhead` p50 would be about 14-18 µs and its 36-41 µs p99 about 17-20 µs, and the
group, run cold-first as in the Eidos bench, about 3-5 µs above that rather than 8; with no
node allocations (the 384 B per frame are gone), the CM5 should gain at least as much as x86
does. Measure on the device to confirm.

### Node fusion

Chains of single-consumer CPU nodes run as fused units: inside a unit, a producer's output goes
straight into the input list of the consumer that runs next, without the edge's direct slot (no
slot put and take, no fan-out routing, no queue-wait or depth bookkeeping), and the unit is one
segment for the parallel and adaptive schedulers. What every node sees is unchanged.

An edge fuses when all of these hold (`crates/runtime/src/plan/fusion.rs`,
`RuntimeEdgeExplanation::fusion_block` names the first that fails):

- both ends are `ComputeAffinity::CpuOnly`, neither is a host-bridge node, and neither sets node
  metadata `daedalus.node.fusion` (`NODE_FUSION_META_KEY`) to `false`;
- its source port feeds only this edge (no fan-out) and its target port has no other producer
  (no fan-in);
- it is a direct-slot edge (a slot-compatible policy that the graph's backpressure strategy does
  not turn back into a queue) with no adapter steps;
- the consumer fires on any input (`fire = "all"` consumers keep what arrived queued across
  ticks, so their inputs stay in slots);
- the consumer is the next non-host-bridge node of the schedule order after the producer.

Stateful nodes, const and config inputs, optional and required inputs, conditional outputs and
repeated pushes are all fine. A unit is a maximal run of schedule-order nodes joined by fused
edges; when its nodes are single-node segments (every CPU node is one), they become one segment.

At run time a fused output is carried only when the consumer really runs next in the order being
executed (the schedule, or a fused segment on a parallel worker), the edge and the consumer are
active under the demand masks, the mask still makes the edge a direct slot, and the slot is empty
(a payload left from an earlier tick, e.g. while the consumer was inactive, must be delivered
first); otherwise the payload takes the slot as before. The carry keeps the slot's semantics:
buffer-all edges keep every push in order, the others keep the newest and record the same
pressure event. Errors and skips behave as before: a failed producer publishes nothing, so the
consumer sees no value (and is not ready if that input is required); a consumer that is not
ready drops what it collected, as it drops what it popped. Per-node metrics, handler timing and
telemetry counters are recorded per node as before; fused edges report `fused_handoffs` instead
of queue metrics, and `Profile`/`Trace` metrics (per-edge lifecycle events) run unfused.

Opt-outs: `EngineConfig::with_node_fusion(false)` (`DAEDALUS_NODE_FUSION=0` through
`EngineConfig::from_env` and the frame-bench examples), `Executor`/`OwnedExecutor::
with_node_fusion(false)`, or the node metadata flag. `crates/runtime/tests/fusion_equivalence.rs`
runs 60 random DAGs (chains, fan-out, fan-in, conditional and doubled outputs, optional and
required inputs, `fire = "all"`, latest-only edges, state, failing nodes, opted-out nodes) for six
ticks in serial (fail-fast on and off), parallel and adaptive mode at `Basic` and `Detailed`
metrics, fused and unfused, and requires identical node calls (every input in order), run
results, errors, per-node calls and transport counts and edge pressure events; it also covers a
slot payload held across ticks while the consumer was masked out.

**x86_64 numbers (AMD Ryzen 9 5900X, shared 24-core machine at load ~20, pinned to one core,
20000 frames), not the CM5.** Fusion on against `DAEDALUS_NODE_FUSION=0` in the same build (the
unfused path runs the instructions `dev` does: 33 142 against 33 130 per detector frame). Instructions per frame with
recording off; stage rows are frame-overhead p50 / p99 in ns with recording on:

| detector mirror | flat, unfused | flat, fused | group, unfused | group, fused |
| --- | --- | --- | --- | --- |
| fused edges | 0 | 3 | 0 | 3 |
| instructions per frame | 33 142 | 32 342 | 33 220 | 32 376 |
| push + tick + take, recording off | 4 660 / 7 700 | 4 430 / 7 470 | 4 720 / 7 910 | 4 450 / 7 700 |
| inputs | 830 / 1 160 | 670 / 980 | 840 / 1 160 | 680 / 1 000 |
| node_io | 1 070 / 1 640 | 950 / 1 510 | 1 090 / 1 660 | 940 / 1 550 |
| dispatch | 540 / 960 | 540 / 950 | 540 / 950 | 550 / 970 |
| `graph_overhead` | 3 350 / 5 040 | 3 040 / 4 780 | 3 390 / 5 000 | 3 060 / 4 830 |

| `frame_chain` (`interface` feed) | unfused | fused |
| --- | --- | --- |
| instructions per frame, 1 / 4 / 16 stages | 8 115 / 15 075 / 43 019 | 8 115 / 14 160 / 38 176 |
| frame p50, 1 / 4 / 16 stages | 850 / 1 560 / 4 280 ns | 850 / 1 420 / 3 570 ns |
| frame p50 fit | 633 ns + 228 ns per stage | 682 ns + 181 ns per stage |
| `graph_overhead` p50 fit | 472 ns + 327 ns per stage | 550 ns + 242 ns per stage |

Each fused edge saves about 310 instructions (a slot put and take, the fan-out scan, the queue
bookkeeping around them); the detector mirror fuses three of its eleven edges (mask prep -> quads
-> decode -> validate; the frame edges cross the host bridge, `validate.detections` fans out), so
the frame gains 2.4% in instructions and about 9% in `graph_overhead`, while a 16-stage chain
fuses 15 and gains 11% (`graph_overhead` per stage -26%). What remains per node is the framing
fusion does not touch: input-list and `NodeIo` setup, the handler call through the registry, and
the readiness and metrics checks.

**CM5 measurement (Raspberry Pi CM5, kernel 7.2.9 with 4 KiB pages, `bcc9f33`, static aarch64-musl,
`FRAME_BENCH_CPU=2`, PhotonVision stopped, 2000 frames, fusion off/on alternated and repeated).**
The tick-cost pass and fusion together beat the projections above: the detector mirror's
`graph_overhead` p50 is 7.2 µs unfused (27-35 µs before the tick-cost pass), and flat and group
now cost the same. For comparison, Eidos measures about 16-17 µs `graph_overhead` p50 on the
CM5 for its real AprilTag graph (group about equal to flat), with no copies and about zero
allocations per tick.

| CM5 detector mirror | flat, unfused | flat, fused | group, unfused | group, fused |
| --- | --- | --- | --- | --- |
| instructions per frame | 32 439 | 32 046 | 32 480 | 32 087 |
| push + tick + take p50, recording off (repeat) | 8 593 (8 648) | 8 463 (8 611) | 8 611 (8 667) | 8 463 (8 555) |
| inputs p50 | 1 800 | 1 520 | 1 800 | 1 540 |
| node_io p50 | 2 260 | 2 170 | 2 280 | 2 190 |
| dispatch p50 | 1 280 | 1 260 | 1 280 | 1 270 |
| `graph_overhead` p50 / p99 | 7 220 / 7 410 | 6 830 / 7 040 | 7 240 / 7 520 | 6 850 / 7 020 |

| CM5 `frame_chain` (`interface` feed, 640x480 GRAY8 on dma-heap) | unfused | fused |
| --- | --- | --- |
| instructions per frame, 1 / 4 / 16 stages | 7 432 / 13 606 / 38 302 | 7 432 / 13 109 / 35 717 |
| frame p50, 1 / 4 / 16 stages | 1 871 / 3 222 / 8 241 ns | 1 907 / 3 130 / 7 407 ns |
| frame p50 fit | 1 485 ns + 423 ns per stage | 1 602 ns + 364 ns per stage |
| `graph_overhead` p50 fit | 1 194 ns + 655 ns per stage | 1 342 ns + 498 ns per stage |

Fusion saves about 5.5% of the detector's `graph_overhead` and 24% per fused chain stage;
nothing copies or allocates in either mode.

### Shared preprocessing (execution domains)

`cargo run --release -p daedalus-frame-bench --example shared_detectors` runs N detector graphs
on one camera (`SHARED_DETECTORS`, default 2, up to 4 dictionaries) three ways in an
`ExecutionDomain`: `separate` (N full detectors, each running mask prep and quads), `shared`
(a `preprocess` graph linked to N decode/validate/refine tails) and `structural`
(`load_shared` on the N full graphs, which finds the same split from the `shareable` stages).
`crates/frame-bench/tests/shared_preprocessing.rs` asserts that preprocessing runs once per
frame, that both shared layouts give the separate detectors' outputs, and that a frame copies
nothing and allocates nothing in the runtime, nodes or host (the forwarding included).

**x86_64 numbers (AMD Ryzen 9 5900X, shared 24-core machine at load ~60-67, pinned, 20000
frames, recording off), not the CM5.** Push + domain tick + take per frame:

| Detectors | | separate | shared | structural |
| --- | --- | --- | --- | --- |
| 2 | instructions | 67 978 | 68 866 | 68 956 |
| 2 | p50 / p99 (ns) | 9 560 / 15 790 | 8 691 / 14 310 | 8 750 / 14 570 |
| 4 | instructions | 135 729 | 123 250 | 123 358 |
| 4 | p50 / p99 (ns) | 27 631 / 36 840 | 17 950 / 28 861 | 15 670 / 29 290 |

The bench stages are nearly free, so these rows are runtime cost: sharing adds one graph tick
(~1 µs) and the forwarding (taking `quads` and feeding N `Arc` clones, ~0.2 µs per link) and
saves two node runs per extra detector. With real stages the saving is the preprocessing time
itself: the domain counts it (`stats()`: 1 avoided graph run and 2 avoided node runs per frame
for 2 detectors, 3 and 6 for 4, with `saved_time` at the measured upstream tick time), so on
the CM5, where Eidos's mask and quad stages take about 0.32 ms and 0.39 ms p50, every detector
after the first should save about 0.7 ms per frame (an estimate; the bench stages measure only
runtime cost).

**CM5 measurement (same run as the fusion rows above, fusion on, recording off).** Push + domain
tick + take p50: 2 detectors 17 666 separate, 17 611 shared, 17 685 structural ns; 4 detectors
35 630 separate, 30 852 shared (-13%), 30 944 structural ns. Over 2000 frames with 4 detectors
the domain avoided 6000 graph runs and 12 000 node runs.

### Choosing a runtime mode

A parallel frame still costs about 3-4 µs per segment more than a serial one (waking workers,
the queue lock, cross-thread payload handoff): for the 16 cheap nodes above that is 70 µs against
17 µs serially. Parallel pays when independent branches each do well over that much work.

- `Serial`: graphs of cheap nodes, linear graphs, and anything latency-sensitive whose nodes take
  microseconds. No worker threads are started.
- `Parallel`: you know independent branches are heavy (milliseconds of CPU, blocking I/O, GPU
  waits) on every frame.
- `Adaptive`: the default choice when unsure. `run_adaptive_in_place` (compiled engines, host
  graphs) times segments, one `Instant` read per node on every fourth serial frame and per
  segment on parallel ones, and predicts serial as the summed segment time `T` and parallel as
  `max(critical path, T / workers) + segments × dispatch`. It switches to parallel when that
  saves at least 25% of `T`, back to serial when the saving drops under 5%, and stays at least 8
  frames in a mode. `dispatch` starts at `DEFAULT_DISPATCH_OVERHEAD` (4 µs, set with
  `EngineConfig::with_adaptive_dispatch_overhead` or `with_adaptive_dispatch_overhead` on an
  executor) and follows what parallel frames measure. Unmeasured graphs start serial unless a node
  is hinted heavy: GPU compute affinity, or node metadata `NODE_COST_META_KEY`
  (`"daedalus.node.cost"`) set to `"heavy"`, which makes the first frame parallel. Cheap graphs
  run at serial speed (the detector frame above: 16.6 µs against 16.0 µs). A one-shot
  `Executor::run_adaptive` has nothing measured and goes parallel whenever the segment graph has
  independent work.

## Troubleshooting

| Symptom | First checks |
| --- | --- |
| Missing adapter | Inspect planner diagnostics and `RuntimePlan::explain()`. Confirm source and target `TypeKey` values and registered adapter declarations. |
| Type mismatch | Log producer `payload.type_key()` and compare it with the consumer adaptation target. |
| Queue pressure | Enable `RUST_LOG=daedalus_runtime::executor::queue=trace` and inspect the edge policy and pressure reason. |
| GPU unavailable | Reproduce with `gpu-mock`, then retry `gpu-wgpu` with `RUST_LOG=daedalus_gpu::wgpu=trace`. |
| Missing host output | Inspect `host_events()` and output queue policy. |
| FFI worker failure | Check `BackendConfig`, protocol version negotiation, worker stderr, and normalized `InvokeResponse` validation errors. |
