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

- No `executor-pool`: `RuntimeMode::Parallel` and `RuntimeMode::Adaptive` still work; ready
  segments run on scoped threads per run instead of a persistent Rayon pool, and
  `pool_size` only caps concurrency. Add `executor-pool` (or use `engine-full`) for hosts that
  run parallel graphs at high frequency.
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
fanned out, config-struct and const inputs, a five-input node, a metadata-only adapter edge,
connected and unconnected `Option<T>` inputs, two conditional producers, one of which never
emits so its consumer is skipped, fan-in, four host outputs) whose handlers allocate nothing,
and asserts the allocations per frame in serial (metrics off and basic), parallel and adaptive
modes. Set `DAEDALUS_ALLOC_TRACE=1` (with `-- --nocapture --test-threads 1`) to print them per
call site, grouped by the first Daedalus frame and its callers.
`cargo bench -p daedalus-rs --features engine-full,plugins --bench graph_frame` times the same
frame.

Allocations per frame, before (local `dev` at the optional-inputs merge, plus the two harness
fixes below) and after:

| Category | Before | After |
| --- | --- | --- |
| Boundary contract formatting (`get_ref`/`try_into_owned`/`Payload::owned`) | 180 | 0 |
| Adapter lifecycle records, step names, path text | 54 | 0 |
| Output port names (`PortId::new` per push) | 28 | 0 |
| Const input payloads rebuilt per tick | 14 | 0 |
| Builtin const coercion boxing | 7 | 0 |
| `StateStore` take/set of node state | 6 | 0 |
| Port lists spilling past four entries | 2 | 0 |
| Host input fan-out target list | 1 | 0 |
| Payloads created (node outputs, adapter results, branch, host frame) | 31 | 31 |
| Generated handler code (per-push type keys, state keys; `daedalus-macros`) | 55 | 55 |
| **Serial, metrics off** | **378** | **86** |
| Serial, basic metrics (per-node metrics map) | 381 | 89 |
| Parallel/adaptive with `executor-pool` (one pool task per segment, result channel) | 422 | 106 |
| Parallel/adaptive without `executor-pool` (a scoped thread per segment) | 458 | 187 |

Timings (x86_64 Linux, shared 24-core machine at load 20-45, criterion medians, back to back):

| Benchmark | Before | After |
| --- | --- | --- |
| `graph_frame/serial_metrics_off` | 33.6 µs | 20.5 µs |
| `graph_frame/serial_metrics_basic` | 40.6 µs | 24.6 µs |
| `graph_frame/parallel_metrics_off` (pool) | 240 µs | 160 µs |
| `graph_frame/adaptive_metrics_off` (pool) | 235 µs | 158 µs |
| `push_tick_take` (one node, basic metrics) | 1.73 µs | 1.57 µs |
| `push_tick_take_metrics_off` | 1.53 µs | 1.34 µs |

The harness needed two fixes to run at all: owned scalar parameters fed by const inputs
(`NodeIo::take_owned` now coerces `Value`s) and builtin branch adapters for keys shared by
several Rust types (a fanned-out `i64` output was branched by the `i32` adapter). For graphs of
cheap nodes, serial is several times faster than parallel or adaptive (adaptive picks parallel
whenever the segment graph fans out); parallel pays off only when segments do real work.

## Troubleshooting

| Symptom | First checks |
| --- | --- |
| Missing adapter | Inspect planner diagnostics and `RuntimePlan::explain()`. Confirm source and target `TypeKey` values and registered adapter declarations. |
| Type mismatch | Log producer `payload.type_key()` and compare it with the consumer adaptation target. |
| Queue pressure | Enable `RUST_LOG=daedalus_runtime::executor::queue=trace` and inspect the edge policy and pressure reason. |
| GPU unavailable | Reproduce with `gpu-mock`, then retry `gpu-wgpu` with `RUST_LOG=daedalus_gpu::wgpu=trace`. |
| Missing host output | Inspect `host_events()` and output queue policy. |
| FFI worker failure | Check `BackendConfig`, protocol version negotiation, worker stderr, and normalized `InvokeResponse` validation errors. |
