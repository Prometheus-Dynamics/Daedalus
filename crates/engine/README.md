# daedalus-engine

Convenience facade that wires registry → planner → runtime so applications can execute graphs with minimal setup.

## Responsibilities
- Bundle configuration (`EngineConfig`) for planner/runtime/GPU settings.
- Provide a simple `Engine::run(registry, graph, handlers)` entrypoint.
- Surface telemetry and results in a single return value.
- Provide an explicit cache-backed `prepare -> build -> execute` path for repeated runs.

## When to use
- If you want an opinionated end-to-end path without manually invoking planner/runtime.
- For examples and small apps; larger systems can compose planner/runtime directly.

## Features
- `executor-pool`: use the retained worker-pool parallel executor. This is enabled by default for
  the engine facade and is the release-facing parallel path.
- `gpu`: include GPU planning/runtime support.
- `plugins`: enable plugin installation path.

## Runtime Modes

`RuntimeMode::Serial` runs the compiled segment order on the caller thread. `RuntimeMode::Parallel`
runs independent ready segments concurrently. `RuntimeMode::Adaptive` currently uses the same
parallel path for non-linear segment graphs and falls back to the fused serial path when the
compiled graph is linear.

With the default `executor-pool` feature, parallel and adaptive execution use the retained
worker-pool executor. When `executor-pool` is disabled, the runtime uses the scoped-thread executor
as a small dependency fallback.

Worker count is resolved in this order:

1. `EngineConfig::with_pool_size` / `runtime.pool_size`.
2. `DAEDALUS_RUNTIME_POOL_SIZE`.
3. `std::thread::available_parallelism()`.
4. A fallback of `4`.

The resolved worker count is clamped to at least one worker and at most the number of runtime
segments, so small graphs do not spawn idle workers.

## Usage
- Build a `Registry` and `HandlerRegistry` (often via `#[node]` macros).
- Construct a graph (see the workspace `examples` package and its quickstart binaries).
- Configure `EngineConfig` (mode, backpressure, GPU backend).
- Call `Engine::run` and inspect telemetry/results.

## In-Process Reuse

The intended ownership model is host-owned and in-process: create an `Engine`, keep it alive, and reuse it across runs instead of rebuilding it per graph.

Cold per-call setup:

```rust
use daedalus_engine::{Engine, EngineConfig, EngineError, RunResult};
use daedalus_planner::Graph;
use daedalus_runtime::HandlerRegistry;

fn run_once(graph: Graph, handlers: &HandlerRegistry) -> Result<RunResult, EngineError> {
    let engine = Engine::new(EngineConfig::default())?;
    let registry = build_registry();
    engine.run(&registry, graph, handlers)
}
```

Warm in-process reuse:

```rust
use daedalus_engine::{Engine, EngineConfig, EngineError, RunResult};
use daedalus_planner::Graph;

let engine = Engine::new(EngineConfig::default())?;
let registry = build_registry();
let handlers = build_handlers();

let first = engine.run(&registry, first_graph, &handlers)?;
let second = engine.run(&registry, second_graph, &handlers)?;
```

For explicit cache-aware preparation:

```rust
use daedalus_engine::{CacheStatus, Engine, EngineConfig};
use daedalus_planner::Graph;

let engine = Engine::new(EngineConfig::default()).unwrap();
let graph = Graph::default();

let prepared = engine.prepare_plan(graph).unwrap();
assert!(matches!(prepared.cache_status(), CacheStatus::Hit | CacheStatus::Miss));

let runtime = prepared.build().unwrap();
assert!(matches!(runtime.cache_status(), CacheStatus::Hit | CacheStatus::Miss));
```

If you want to inspect or control reuse more directly, keep the same `Engine` instance and use the explicit lifecycle:

```rust
let prepared = engine.prepare_plan(&registry, graph)?;
let built = prepared.build()?;
let result = engine.execute(built.runtime_plan(), &handlers)?;
```

Use `Engine::cache_metrics()` to inspect planner/runtime-plan cache behavior and `Engine::clear_caches()` when the host wants to reset warm state.

## HostGraph Flows

`Engine::compile_registry` returns a `HostGraph` for applications that keep a graph alive and feed host ports over time. Prefer these flows before dropping to the lower-level push/tick/drain pieces:

```rust
let mut graph = engine.compile_registry(&registry, graph)?;

let outputs: Vec<String> = graph.run_once(("in", 7_i64), "out")?;
```

For repeated typed I/O, bind handles once:

```rust
let input = graph.bind_input::<i64>("in");
let output = graph.bind_output::<String>("out");

input.push(7);
graph.tick_until_idle()?;
let value = output.try_take().expect("typed output");
```

For hot single-input/single-output routes, cache a direct lane when available:

```rust
let lane = graph.bind_lane::<i64>("in", "out").expect("direct route");
let value: Option<String> = graph.run_lane_owned(&lane, 7)?;
```

Use `push`, `tick`, `tick_selected`, and `drain_*` directly when a graph has multiple host inputs, demand-selected outputs, or diagnostic needs.

### Cache Lifecycle

Planner cache entries are reused only when the graph shape, planner config, and registry-derived planning inputs match. If you change the graph, change relevant planner settings, or change the registry contents that planning depends on, expect a planner-cache miss.

Runtime-plan cache entries are reused only when the execution plan and scheduler/runtime-plan inputs still match. If the planner output changes, or if runtime-plan construction inputs change, expect a runtime-plan-cache miss even if the same `Engine` instance is reused.

Use `Engine::cache_metrics()` when you want to confirm whether the current host ownership pattern is actually producing cache hits. Use `Engine::clear_caches()` when the host intentionally wants to drop warm state, such as:

- after loading a materially different graph or registry set and wanting a clean measurement pass
- after a configuration transition where reuse would be misleading
- before benchmark phases that need to separate cold and warm behavior

If you are just running more graphs through the same long-lived engine, do not clear caches by default. Reuse is the whole point of the warm path.

## Optional Daemon

If you want a long-lived process around the engine, use `daedalus-daemon`.

That crate owns:

- `stdio` and optional TCP transport
- named graph and registry state
- cache and plan/build inspection
- CLI-oriented request/response handling

It is parked as optional tooling. `daedalus-engine` stays focused on the Rust library surface rather than process hosting.
