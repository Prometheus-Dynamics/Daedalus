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
- `plugins`: enable plugin install path (default for engine use).
- `gpu`: enable GPU-aware execution and payload conversion.

## Usage
1) Build/obtain a `RuntimePlan` from the planner.  
2) Build a `HandlerRegistry` with node handlers and capabilities.  
3) Construct `RuntimeConfig` (mode, pool size, backpressure).  
4) Run `Executor::run(plan, registry, config)` or use the `engine` facade for convenience.

## Testing
- `cargo test -p daedalus-runtime --features "plugins"` covers routing/backpressure/sync and golden plans.
- GPU-specific tests are gated behind `gpu` + `gpu-wgpu`.
