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

## Frame-Path Overhead

For camera-style hosts (a frame arrives, the graph ticks, results are taken) the question is how
much of each frame goes to the nodes and how much to everything around them. Frame-overhead
recording answers it per tick, at any metrics level (including `Off`), without allocating per
tick:

```rust
use daedalus::engine::{EngineConfig, MetricsLevel};

// Every host graph compiled with this config records its last 1024 ticks
// (`DAEDALUS_FRAME_OVERHEAD=1024` does the same; `=1` keeps the default 512).
let config = EngineConfig::default()
    .with_metrics_level(MetricsLevel::Off)
    .with_frame_overhead(1024);
let mut host = engine.compile_registry(&registry, graph)?;
// ... or on a compiled graph: host.enable_frame_overhead(1024);

// warm up, then start the window over
host.reset_frame_overhead();
// ... push / tick / take frames ...
println!("{}", host.frame_overhead().unwrap()); // table; `.to_json()` for logs
let last = host.last_frame_tick();               // one tick's `FrameTickSample`
```

`frame_overhead()` returns a `FrameOverheadReport`: p50/p99/max/mean (exact, over the window,
plus a log2 `Histogram` per row) of each stage in nanoseconds, the per-tick counters, and per-edge
queue and adapter time. The stages of one tick:

| Row | What it measures |
| --- | --- |
| `push` | Host-bridge feeds (`push*`, bound inputs, a camera thread's `feed_payload`) since the previous tick, attributed to the tick they feed. |
| `tick` | Wall time of the executor run behind `HostGraph::tick*` (direct routes included), output drain included. |
| `  inject` | Host inputs fanned out to graph edges. |
| `  inputs` | Per-node input collection: queue and direct-slot pops (adapters excluded). |
| `  adapters_zero_copy` / `adapters_copying` | Adapter paths, split by the path's kinds: identity, reinterpret, view, shared/cow view, metadata-only and in-place are zero-copy; copy-on-write, branch, materialize, device transfers, (de)serialization and custom adapters count as copying. |
| `  handlers` | Node handler calls. |
| `  node_io` | Framing around each handler: `NodeIo` setup, flush, output publishing and fan-out. |
| `  drain` | Graph outputs handed to the host bridge. |
| `  dispatch` | The rest of the tick: run setup, scheduling, readiness checks. |
| `take` | Host takes after the tick: `take*`, `drain*`, bound outputs, `inspect_payload`/`inspect_outputs`. |
| `graph_overhead` | `tick - handlers`: everything the runtime adds to the handlers' own work. |
| `queue_wait` | Enqueue to dequeue summed over edges; latency that overlaps the stages above. |

In a serial tick the nested rows add up to `tick`; parallel node runs overlap, so `dispatch`
saturates at zero there. Per-tick counters: `nodes`, `zero_copy_adapts`, `copies` (copying adapter
runs) and `copied_bytes` (their estimated output size), `shared_clones` (fan-out payload clones,
an `Arc` increment each), `gpu_uploads`/`gpu_downloads` (device-transfer steps run), and with the
allocation probe `runtime_allocs`, `node_allocs`, `host_allocs` and their bytes. Edge rows list
every edge that queued or adapted a payload in the window: adapter class, wait and adapter p50/p99,
adapter runs per tick.

Reading it:

- **`graph_overhead` against `handlers`** is the headline. For heavy nodes (a 1 ms detector) a
  few microseconds of overhead is noise; for a chain of cheap nodes it dominates.
- **Per-node cost** shows in `inputs`, `node_io` and `dispatch` growing with the node count. The
  `frame_chain` example fits `fixed + per_node × N` over 1, 4 and 16 no-op stages.
- **A copy on the frame path** shows as `copies`/`copied_bytes` above zero and an edge with
  adapter class `copying`; `explain_plan()` names it ahead of time (`copies_frame` below).
- **Allocations at steady state** (`runtime_allocs` above zero after warm-up) are runtime
  bookkeeping or adapter outputs: an owner-type frame fed to `FrameView` inputs allocates one
  `ForeignHandle` payload per consumer edge per frame (the provider's `View` adapter), while a
  host that feeds `daedalus:frame` payloads directly allocates nothing.

Recording costs two clock reads per node and per edge plus a few relaxed atomic adds (about
0.5 µs per tick plus 0.2 µs per stage on the x86_64 host in
[development.md](development.md#frame-path-overhead));
`HostGraph::disable_frame_overhead` turns it off. Without it, each recording site is one `None`
check, the bridge's feed/take paths one relaxed load, and `HostGraph::tick*` one check: the
executor times the run into the probe and the graph moves it into the window when the next tick
starts (`frame_overhead()` and `last_frame_tick()` include the latest tick). Push and take times
read the platform clock; everything else reads the executor's clock.

### Allocation probe

Feature `alloc-probe` (`daedalus` or `daedalus-engine`) adds `daedalus::alloc_probe`: a counting
global allocator and per-thread scopes. Install it in the binary that should be measured:

```rust
#[global_allocator]
static ALLOC: daedalus::alloc_probe::CountingAllocator =
    daedalus::alloc_probe::CountingAllocator::system();
```

The executor marks its threads `Runtime` while a tick runs and `Node` around each handler call;
host-bridge feeds and takes are `Host`; anything else is `Other`. `alloc_probe::counts()` reads
the process-wide counters, and the frame-overhead report fills `runtime_allocs`/`node_allocs`
(during the tick) and `host_allocs` (between the tick and the next: its takes and the next
feeds) once the allocator is installed.
Without the feature the scope switches compile to nothing; with it but without the allocator
installed they cost one relaxed load per handler call. The counters are process-wide, so measure
one graph at a time.

### Copying and residency-crossing edges

`HostGraph::explain_plan()` flags each edge: `crosses_residency` when its adapter path moves the
payload between CPU, GPU and external memory (a device transfer, or a step whose residency differs
from the previous step or the target port), and `copies_frame` when the edge carries a frame-like
payload (the `daedalus:frame` interface or a key naming a frame or image) and its path copies it
or crosses residency. `RuntimePlanExplanation::copying_edges` / `crossing_edges` list them, and
its `Display` prints one line per node and edge plus a summary:

```text
runtime plan: 3 nodes, 3 edges, backpressure=None
  node 0: io.host_bridge (CpuOnly) label=host
  node 1: daedalus.frame_bench:stage (CpuOnly) label=stage_0
  node 2: daedalus.frame_bench:stage (CpuOnly) label=stage_1
  edge 0: io.host_bridge.frame -> daedalus.frame_bench:stage.frame [queue] adapters=daedalus.foreign:daedalus.frame_bench:synthetic_frame->daedalus:frame:view
  edge 1: daedalus.frame_bench:stage.frame -> daedalus.frame_bench:stage.frame [direct_slot]
  edge 2: daedalus.frame_bench:stage.frame -> io.host_bridge.out [direct_slot]
copies_frame: none
crosses_residency: none
```

### Frame bench harness

`crates/frame-bench` (`daedalus-frame-bench`, not published; use it as a path or git
dev-dependency) packages the measurement: `SyntheticFrameSource` hands out 640x480 frames in a
dma-buf from `/dev/dma_heap` (else a `memfd` mapping) through `daedalus:frame`, either as
interface payloads or as its own type with a provider; `compile_frame_chain(n, feed, config)`
builds `host -> n no-op FrameView stages -> host`; `run_frame_bench` warms up, drives push, tick
and take per frame and returns the wall time per frame, the overhead report and allocations per
frame. Its crate docs show how to run your own nodes (e.g. a detector taking `FrameView<'_>`)
through the same harness, and
`cargo run --release -p daedalus-frame-bench --example frame_chain` prints the reference numbers.

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
- `StreamGraph::host_stats()` for accepted, replaced, dropped, delivered, and closed counters, and `HostBridgeHandle::input_port_stats(port)`/`output_port_stats(port)` for the same counters (plus pending) on one port.
- `StreamGraph::host_config()` for active host bridge pressure/freshness policies.
- `StreamGraph::host_events()` for retained feed/drop/deliver events (empty unless event recording is enabled).

Host bridge event recording is off by default. Enable it (`HostBridgeConfig::with_event_recording(true)`, `EngineConfig::with_host_event_recording(true)`, or `DAEDALUS_HOST_EVENT_RECORDING=1`) while debugging dropped or missing payloads, and keep `HostBridgeConfig::event_limit` bounded in long-running hosts. Stats and pressure warnings on the `daedalus_runtime::host_bridge` tracing target do not depend on it.

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
