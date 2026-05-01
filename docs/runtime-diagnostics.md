# Runtime Diagnostics

Use this flow when preparing a release or debugging runtime behavior in a host application.

## Build And Lint

Run the full workspace checks before cutting a release:

```sh
cargo check --workspace --locked --all-targets
cargo clippy --workspace --locked --all-targets -- -D warnings
```

For dependency drift, run:

```sh
cargo tree -d --workspace --locked
```

The expected duplicate set is mostly from dev tooling and the GPU ecosystem. Treat new production duplicate versions as release blockers unless there is a documented reason.

Current documented duplicate roots:

- `bindgen`, `proc-macro-crate`, `toml_edit`, `toml_datetime`, `winnow`, `regex`, `regex-automata`, `rustc-hash`, and `either`: build/dev/tooling drift through camera bindings, macro support, `trybuild`, `criterion`, and related generator tooling.
- `bit-set`, `bit-vec`, `hashbrown`, `foldhash`, `getrandom`, `rand`, `rand_core`, `rustix`, `linux-raw-sys`, and `libc`: mixed GPU, property-test, benchmark, tempfile, and platform stacks.
- `bitflags`, `smallvec`, and `thiserror`: direct workspace versions plus transitive camera/GPU ecosystem versions.
- `wayland-sys` and related Wayland/X11 support: transitive display/camera backend dependencies through `styx`, `minifb`, and `wgpu`.

These are acceptable for the current release as transitive dependencies. New direct workspace dependencies should still be added through `workspace.dependencies`, and new duplicate production roots should be documented here or eliminated.

## Runtime Telemetry

Use `ExecutionTelemetry` from executor runs as the first-level runtime snapshot. The high-signal fields are:

- `nodes_executed`, `warnings`, and `errors` for run health.
- `backpressure_events` and edge pressure metrics for queue pressure.
- node metrics and resource lifecycle metrics for expensive handlers or retained state.
- `ffi` telemetry for package, backend, worker, payload, and adapter behavior.

Prefer `telemetry.report().to_table()` when humans need a compact view, and structured serialization when comparing runs or attaching diagnostics to host logs.

Enable telemetry through owned runtime or engine configuration:

```rust
use daedalus::engine::{EngineConfig, MetricsLevel};

let config = EngineConfig::default().with_metrics_level(MetricsLevel::Detailed);
```

Use the lowest level that answers the question:

- `Off`: disables runtime metrics collection for overhead checks.
- `Basic`: records run health, call counts, warnings, errors, and basic queue pressure.
- `Timing`: keeps timing-oriented metrics without the full detailed transport/resource surface.
- `Detailed`: adds node handler timing, edge waits, transport bytes, queue depth, and resource metrics.
- `Hardware`: enables hardware-oriented samples when supported by the host configuration.
- `Profile`: adds profile snapshots and richer per-node/per-edge histograms.
- `Trace`: records lifecycle-level data movement and detailed trace events.

The `metrics_levels`, `runtime_metrics`, `transport_metrics`, `ownership_metrics`,
`lifecycle_trace`, and `stream_diagnostics` examples show the expected release-facing output shapes.

## Tracing Targets

Enable tracing in host applications with `tracing_subscriber` and a runtime filter. Start broad for release debugging:

```sh
RUST_LOG=daedalus_engine=debug,daedalus_runtime=info
```

Narrow the filter when investigating a specific subsystem:

- Executor scheduling, queue pressure, and transport movement:
  `RUST_LOG=daedalus_runtime::executor=debug,daedalus_runtime::executor::queue=trace,daedalus_runtime::transport=debug`
- Streaming host IO and retained execution:
  `RUST_LOG=daedalus_runtime::stream=debug,daedalus_runtime::host_bridge=trace`
- Runtime configuration and global state warnings:
  `RUST_LOG=daedalus_runtime::config=debug,daedalus_runtime::state=debug,daedalus_runtime::handler_registry=trace`
- Engine cache behavior:
  `RUST_LOG=daedalus_engine::cache=debug`
- GPU backend selection, dispatch, polling, and readback:
  `RUST_LOG=daedalus_gpu::wgpu=debug,daedalus_gpu::dispatch=debug,daedalus_gpu::poll_driver=debug,daedalus_gpu::readback=trace`
- Planner passes and bundled demo nodes:
  `RUST_LOG=daedalus_planner::passes=debug,daedalus_nodes::demo=info`

FFI worker and payload details are primarily exposed through `ExecutionTelemetry::ffi` so embedders can forward structured diagnostics to host logs without parsing stderr. Persistent worker stderr is still retained and surfaced in runner errors when startup or message decoding fails.

## Host Bridge Diagnostics

For streaming host IO, inspect:

- `StreamGraph::diagnostics()` for state, worker state, pending inbound/outbound counts, current execution elapsed time, last error, and last telemetry summary.
- `StreamGraph::host_stats()` for accepted, replaced, dropped, delivered, and closed counters.
- `StreamGraph::host_config()` for active host bridge pressure/freshness policies.
- `StreamGraph::host_events()` for retained feed/drop/deliver events.

Keep `HostBridgeConfig::event_limit` bounded in long-running hosts. Use `event_recording = false` when event snapshots are not needed.

## Stream Workers

Use `StreamGraphWorker::stop_timeout` in release-facing hosts. Dropping a worker requests shutdown and emits a warning if the thread is still running, but it does not kill a blocked handler.

If shutdown is delayed, inspect:

- `StreamGraphWorker::diagnostics()`
- `StreamGraph::diagnostics()`
- host bridge pending counts and last execution elapsed time

Long-running node handlers should be bounded and cooperative so worker shutdown can complete predictably.

## FFI Workers

Persistent workers expose diagnostics through FFI telemetry:

- backend starts, reuses, invokes, failures, not-ready counts, shutdowns, pruning, and byte counts.
- worker handshakes, request/response bytes, encode/decode duration, malformed responses, stderr events, typed errors, and raw IO events.
- payload handle creation, resolution, release, access mode, residency, layout, and ownership-mode counters.

Persistent worker stderr is drained continuously with capped retention to prevent pipe backpressure from blocking worker stdout. If a worker exits before producing a valid message, the retained stderr text is included in the runner error.

`RunnerLimits` is explicit about unsupported persistent-worker semantics. Non-default queue depth, request timeout, and restart policy settings are rejected at construction until cancellable worker IO and automatic restart are implemented.

## FFI Validation

Run the FFI crates and worker lifecycle tests when touching the FFI contract or host runner:

```sh
cargo test -p daedalus-ffi-core --locked
cargo test -p daedalus-ffi-host persistent_worker_ --locked
cargo test -p daedalus-ffi-python --locked
cargo test -p daedalus-ffi-node --locked
cargo test -p daedalus-ffi-java --locked
cargo test -p daedalus-ffi-cpp --locked
```

Language-specific tests may skip when the corresponding interpreter/toolchain is absent.
