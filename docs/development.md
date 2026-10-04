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
- `examples`: runnable examples and plugin fixtures.
- `scripts` and `testing`: local validation and CI support.

## Validation Loop

Run the default loop in [testing.md](testing.md#default-surface) before sending broad changes.
Use `gpu-mock` for deterministic GPU-path tests and `gpu-wgpu` only on machines with a real backend available.

## Dependency Policy

- Workspace crates should inherit common dependencies from root `workspace.dependencies`.
- Library error types should be typed and use `thiserror`.
- Runtime instrumentation should use `tracing`.
- Avoid adding dependencies to `core` and `transport` unless the owning contract truly requires them.
- Dependencies of the `no_std` crates must support `no_std` (see [Portability](#portability)).
- Keep backend variants behind stable feature names.
- Use `cargo tree -d --workspace` during release review and treat new duplicate dependency roots as review input.

## Public API Policy

- Prefer fallible graph and registry construction in production code: `try_connect`, `try_connect_ports`, `try_merge`, `try_on`, and related helpers.
- Prefer typed `PortHandle`s or explicit `(node, port)` tuples for graph wiring. Plain host-port strings such as `"input"` are fine at host boundaries; dotted strings such as `"node.output"` are shorthand and should stay in tests, small demos, or compile-time-fixed graphs.
- Panic-first helpers are acceptable in tests, small demos, and compile-time-fixed graph construction.
- Public ids, type keys, package descriptors, and telemetry fields should remain deterministic because planner/runtime goldens and FFI fixtures depend on stable serialization.
- Host-facing queues should be bounded deliberately; internal finite graph edges default to compatibility-oriented FIFO behavior unless a graph or runtime config selects otherwise.

## Observability

- Initialize a `tracing` subscriber in binaries and integration tests.
- Useful targets include `daedalus_runtime::executor`, `daedalus_runtime::executor::queue`, `daedalus_runtime::host_bridge`, `daedalus_runtime::stream`, `daedalus_runtime::config`, `daedalus_planner::passes`, `daedalus_gpu::wgpu`, `daedalus_gpu::dispatch`, `daedalus_gpu::readback`, and `daedalus_gpu::poll_driver`.
- Metrics levels are `Off`, `Basic`, `Timing`, `Detailed`, `Hardware`, `Profile`, and `Trace`.
- `Detailed` is the normal level for transport and allocation debugging.
- `Profile` adds per-node profile snapshots.
- `Trace` records lifecycle-level data movement details.
- `DAEDALUS_NODE_CPU_TIME=1` enables Linux per-node CPU timing.
- `DAEDALUS_NODE_PERF_COUNTERS=1` enables Linux perf counters.

## Runtime Defaults

- Stream workers use `DEFAULT_STREAM_IDLE_SLEEP` unless configured through `EngineConfig`, `RuntimeSection`, or `StreamWorkerConfig`.
- Host bridge event recording is off by default (`DEFAULT_HOST_BRIDGE_EVENT_RECORDING = false`) because it allocates one event per push and delivery. Enable it with `EngineConfig::with_host_event_recording(true)`, `HostBridgeConfig::with_event_recording(true)`, `HostBridgeHandle::set_event_recording(true)`, or `DAEDALUS_HOST_EVENT_RECORDING=1`; when enabled it retains `DEFAULT_HOST_BRIDGE_EVENT_LIMIT` events per bridge. Stats counters and `daedalus_runtime::host_bridge` tracing warnings for drops/replacements are always on.
- Host ports with a replace-style capacity-one policy (the default `Bounded { capacity: 1, overflow: DropOldest }`, `LatestOnly`, `DropOldest`, `Coalesce`) store their value in a single slot that is overwritten in place.
- Internal edge queues preserve compatibility defaults; streaming, camera, daemon, and interactive workloads should set explicit bounded/latest-only policies.
- WGPU staging behavior is configured through `WgpuStagingPoolConfig` or `DAEDALUS_WGPU_STAGING_*` before backend construction.

## Minimal CPU-Only Profile

For constrained hosts (embedded boards, sidecar engines), depend on the facade with the
`embedded` preset:

```toml
daedalus = { package = "daedalus-rs", version = "2.0.0", default-features = false, features = ["embedded"] }
```

`embedded` is `engine` + `plugins`: the facade's `engine` feature no longer enables the Rayon
executor pool or metrics collection. Add `dylib-plugins` only if the host loads native plugin
libraries. Leave every `gpu*` feature off; `EngineConfig`'s default `GpuBackend::Cpu` and
`planner.enable_gpu = false` need no GPU feature. This set links no `wgpu`, `image`, `tokio`,
`styx`, `rayon`, or `crossbeam`; its heaviest runtime dependencies are `serde_json` and
`tracing`.

Feature semantics without the extras:

- No `executor-pool`: `RuntimeMode::Parallel` and `RuntimeMode::Adaptive` still work; parallel
  runs fan out to a few persistent threads (one fewer than the workers, as the calling thread
  takes part) started on the first parallel run and parked between runs, instead of a Rayon
  pool. A frame spawns no threads either way.
- No `metrics`: the telemetry APIs (`MetricsLevel`, `ExecutionTelemetry`) still compile, but
  executors record no per-node or transport metrics. Add `metrics` when you read telemetry.

Applications that do not need to minimize dependencies should use `engine-full`
(`engine` + `executor-pool` + `metrics`).

Reference measurement (x86_64 Linux, rustc 1.97.1, `lto = "thin"`, `codegen-units = 1`,
`strip = true`): a binary that installs one plugin, compiles a one-node host graph, and runs it
10,000 times.

| Features | Crates (normal deps) | Stripped binary | Peak RSS | Threads |
| --- | --- | --- | --- | --- |
| `embedded` | 66 | 2.6 MiB | ~4.4 MiB | 1 |
| `engine-full,plugins` | 71 | 2.7 MiB | ~4.5 MiB | 1 |

Linear graphs stay on the serial execution path, so neither profile starts worker threads for
this workload; the pool mainly costs crates and binary size. Re-measure on the target board
before budgeting.

## Portability

Daedalus targets hosted operating systems, but keeps two smaller targets buildable so they can
grow: microcontrollers (`no_std`) and WebAssembly. CI checks both (`scripts/ci.sh nostd wasm`,
see [testing.md](testing.md#no_std-and-wasm)). Nothing changes for `std` users: every crate
keeps `std` in its default features, and a native build compiles the same types as before.

### Tier 1: `no_std` + `alloc`

`daedalus-core`, `daedalus-transport`, `daedalus-data`, `daedalus-registry` and
`daedalus-planner` have a default `std` feature. With `default-features = false` they are
`#![no_std]` and need only `alloc` and pointer-sized atomic compare-and-swap (checked on
`thumbv7em-none-eabihf`, a Cortex-M4F). That covers ids, payloads and lineage, the type and value
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
- 64-bit counters: `AtomicU64`, or a spin-locked `u64` on targets without 64-bit atomics.
- Time: [`daedalus_core::platform`](../crates/core/src/platform.rs) exports `Instant`, which is
  `std::time::Instant` wherever the target has an OS clock and otherwise a portable instant read
  from the clock installed with `platform::set_clock` (zero until one is installed).
- `std`-only: the planner's `DAEDALUS_TRACE_EMBEDDED_EXPAND` variable and its two binaries.

Workspace wiring: shared dependencies that the tier is built from (`serde`, `serde_json`,
`thiserror`, `tracing`, `base64`, `crossbeam-queue`) and the five crates themselves are declared
in `[workspace.dependencies]` without default features, so `std` crates request
`features = ["std"]` (`daedalus-data`: `["std", "json"]`). `std` is a boundary feature in the
plugin build fingerprint because it swaps lock and map types; `daedalus-transport` is
fingerprinted now that it has a feature.

### WebAssembly

`wasm32-unknown-unknown` has `std` but no threads and no clock (`std::time::Instant::now` and
thread spawning panic there). The `embedded` facade preset builds and runs on it: CI runs the
[`examples/wasm_smoke`](../examples/wasm_smoke/src/lib.rs) module, a fan-out graph in every
runtime mode, in Node. Behavior there:

- `platform::THREADS` is `false` (also on `wasm32-wasip1` and any wasm target without atomics):
  `Parallel` and `Adaptive` resolve one worker and run serially; blocking host-bridge waits
  (`InboundWaiter::wait`, `HostBridgeHandle::recv_payload_timeout`) return at once when nothing
  is queued, and an indefinite wait (`wait(None)`, so `HostGraph::drive_blocking`) panics;
  `StreamGraph::spawn_continuous*` panics. Drive graphs with `HostGraph::tick`/`tick_if_ready`,
  the async `HostGraph::drive`, or `StreamGraph::poll`/`run_available`.
- `platform::OS_CLOCK` is `false`: runtime and engine timing reads `platform::Instant`. Without a
  clock, durations are zero (telemetry reads zero, adaptive mode stays serial, and
  `FreshnessPolicy::MaxAge` never drops). Install one first, e.g.
  `platform::set_clock(|| Duration::from_secs_f64(performance_now_ms() / 1e3))`.
- The embedded dependency tree has no `getrandom` or other OS-bound crate. `dylib-plugins`,
  `gpu*`, `executor-pool` (Rayon) and the FFI crates are not supported on wasm.

### Rules for new code

- Tier-1 crates: no `std::` paths outside `#[cfg(feature = "std")]` items and tests. Use `core::`
  and `alloc::` (import `String`, `Vec`, `Box`, `ToString`, ... from `alloc`; `format!` and
  `vec!` come from `#[macro_use] extern crate alloc` without `std`). Exported macros name
  `alloc` types through `$crate::__private`, since the calling crate may lack `extern crate
  alloc`.
- No new process-global state. Where one is unavoidable (registries, interning), use
  `crate::portable::{OnceLock, Mutex}` and keep hot paths free of it.
- OS facilities (files, environment, processes, threads, sleeping, blocking waits, clocks) sit
  behind `std` in tier-1 crates. In the runtime and engine, read time through
  `daedalus_core::platform::Instant` (never `std::time::Instant::now()` outside thread-only code)
  and check `platform::THREADS` before spawning threads or blocking.
- A new dependency of a tier-1 crate must support `no_std`: declare it in the workspace without
  default features and enable its `std` feature from the crate's `std` feature.
- Run `scripts/ci.sh nostd wasm` after touching these crates or the executor.

### Roadmap: tier 2

Tier 2 is a `no_std` serial executor: running planned graphs on a microcontroller. Open items
are tracked in [TODO.md](../TODO.md) under "Portability (tier 2)":

- Lock backend: `daedalus-runtime` and `daedalus-engine` use `parking_lot` directly; move them
  to a `lock_api`-based alias (`parking_lot` with `std`, a `spin` or `critical-section` raw lock
  without).
- Clock: replace the global `platform::set_clock` with a `Clock` value carried by
  `EngineConfig`/the executor, so hosts with several clocks (and tests) can inject one.
- Host bridge without blocking: the bridge's `Condvar` waits become `std`-only; `no_std` hosts
  push, poll, and await the waker-based `InboundWaiter` future.
- Compile-time gating: a `threads` feature (default on) for the worker pool, stream workers and
  blocking waits, instead of the runtime `platform::THREADS` checks.
- `alloc`-only runtime pieces: `serde_json` const decoding, `tracing` without `std`, telemetry
  without `std::time`, `libc` only on Linux.
- Targets without compare-and-swap (`thumbv6m-none-eabi`): `spin` with `portable-atomic`, or a
  `critical-section` lock.

## Performance

`cargo bench -p daedalus-engine --features plugins --bench host_graph_drive` measures the host
bridge and a one-node `HostGraph` (`host.in -> inc -> host.out`, serial mode). Save a baseline
with `-- --save-baseline <name>` and compare with `-- --baseline <name>`. The
`hot_path_allocations` engine test pins the metrics-off round trip at 4 heap allocations (the
input and output payloads); it was 31 before the hot-path pass.

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

Each created payload is two allocations (value `Arc` and storage), one when a handler returns an
`Arc` it already holds. `Payload::owned` always builds typed storage: boundary contracts are
registry-scoped and checked when a graph is compiled, and only `Payload::boundary_owned` builds
contract-restricted storage.

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
