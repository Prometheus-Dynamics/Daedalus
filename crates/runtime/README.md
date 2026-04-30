# daedalus-runtime

Executes planned graphs, routes runtime values between nodes, and enforces policies (backpressure, sync groups, capabilities). Integrates plugins and GPU hand-off.

## Responsibilities
- **Execution**: runs `RuntimePlan` segments serially or in parallel, with telemetry and error propagation.
- **Handlers**: `HandlerRegistry` maps node IDs to handler fns; capability handlers enable type-directed operations.
- **Backpressure/sync**: configurable edge policies (fifo/bounded/latest/zip) and sync groups for aligning correlated runtime values.
- **Plugins**: install bundles of nodes/handlers; host bridge support for external processes.
- **GPU**: optional integration with `daedalus-gpu` (feature-gated); routes GPU-resident runtime values and shader contexts.

## Key modules
- `executor`: serial and parallel executors, queues, telemetry, error types.
- `plan`: runtime plan representation (built by planner).
- `handler_registry`: mapping of node handlers and capabilities.
- `io`: payload routing, correlation IDs, sync policies, backpressure application.
- `plugins`: plugin installation helpers.
- `host_bridge`: channel-style bridge for host↔graph payload exchange.

## Features
- `executor-pool`: use the retained Rayon-backed worker pool for parallel execution. Engine users
  get this by default through `daedalus-engine`; direct runtime users can disable it for the
  scoped-thread fallback.
- `plugins`: enable plugin install path (default for engine use).
- `gpu`: enable GPU-aware execution and payload conversion.

## Parallel Execution

`Executor::run_parallel` runs independent ready segments concurrently. With `executor-pool` enabled
it uses the retained worker pool; with the feature disabled it uses the scoped-thread executor as a
minimal fallback. Linear segment graphs are fused onto the serial path because there is no parallel
work to schedule.

Pool size is resolved from `Executor::with_pool_size`, then `DAEDALUS_RUNTIME_POOL_SIZE`, then
`std::thread::available_parallelism()`, with a fallback of `4`. The value is clamped to the segment
count, so a graph with fewer segments than available workers only schedules useful work.

## Usage
1) Build/obtain a `RuntimePlan` from the planner.  
2) Build a `HandlerRegistry` with node handlers and capabilities.  
3) Construct `RuntimeConfig` (mode, pool size, backpressure).  
4) Run `Executor::run(plan, registry, config)` or use the `engine` facade for convenience.

## Queue Defaults

Internal graph edges default to FIFO `BufferAll` with `BackpressureStrategy::None`. This preserves
all produced values and keeps existing graph semantics stable, but it means producer/consumer
imbalance can grow memory until the graph author chooses a bounded policy. Release-facing bounded
behavior is opt-in through `SchedulerConfig::default_policy`, per-edge builder policies such as
`RuntimeEdgePolicy::bounded(capacity)`, or runtime backpressure strategies. Host bridge input and
output queues use separate bounded defaults so external producers do not inherit the internal
unbounded edge policy accidentally.

`BackpressureStrategy::BoundedQueues` is intentionally nonblocking. When a bounded internal edge is
full, the runtime records a pressure event and rejects the incoming payload while retaining the
already queued payload. Use `ErrorOnOverflow` when a full bounded edge should surface as an execution
error instead of a dropped incoming payload.

## Streaming workers
`StreamGraph::spawn_continuous` runs graph ticks on a background thread and wakes on host-bridge
input. Node handlers used with continuous streaming should be bounded and cooperative: a handler
that blocks indefinitely can delay `StreamGraphWorker::stop` and the worker `Drop` path because the
runtime waits for the in-flight handler to return before joining the worker thread. Use
`StreamGraphWorker::stop_timeout` when shutdown code needs to report or recover from a delayed
worker stop. After a timeout, `StreamGraphWorker::diagnostics()` reports whether stop has been
requested, whether the worker thread has finished, whether shutdown is still pending, and how long
the stop request has been outstanding.

Continuous streaming also detects graph ticks that leave host inbound queues unchanged. When a tick
does not reduce pending inbound payloads, the worker waits before retrying and records a warning in
the last telemetry snapshot. This prevents a misconfigured demand slice or inactive host input from
turning into a busy retry loop.

For the 1.0 release, continuous streaming is intentionally thread/condvar based rather than tied to
a Tokio runtime. This keeps the runtime usable from synchronous hosts, FFI hosts, and async
applications without requiring one executor model. Async applications should treat the worker as a
dedicated blocking graph runner, feed it through host bridge handles, and use `stop_timeout` during
shutdown so delayed node handlers are observable through `StreamWorkerDiagnostics`.

## Testing
- `cargo test -p daedalus-runtime --features "plugins"` covers routing/backpressure/sync and golden plans.
- GPU-specific tests are gated behind `gpu` + `gpu-wgpu`.
