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

## Troubleshooting

| Symptom | First checks |
| --- | --- |
| Missing adapter | Inspect planner diagnostics and `RuntimePlan::explain()`. Confirm source and target `TypeKey` values and registered adapter declarations. |
| Type mismatch | Log producer `payload.type_key()` and compare it with the consumer adaptation target. |
| Queue pressure | Enable `RUST_LOG=daedalus_runtime::executor::queue=trace` and inspect the edge policy and pressure reason. |
| GPU unavailable | Reproduce with `gpu-mock`, then retry `gpu-wgpu` with `RUST_LOG=daedalus_gpu::wgpu=trace`. |
| Missing host output | Inspect `host_events()` and output queue policy. |
| FFI worker failure | Check `BackendConfig`, protocol version negotiation, worker stderr, and normalized `InvokeResponse` validation errors. |
