# daedalus-runtime

Executor and host integration layer for planner-produced runtime plans.

## Owns

- `RuntimePlan`, runtime nodes, runtime edges, segments, and demand slices,
- serial, parallel, retained-pool, and selected execution paths,
- handler registry integration,
- host bridge input/output queues,
- streaming graph workers,
- runtime state and managed resources,
- transport execution and direct payload lanes,
- telemetry, metrics, profiling, lifecycle records, and FFI telemetry aggregation.

## Feature Shape

- `plugins`: plugin installation and registry integration.
- `gpu`: GPU-aware execution support.
- `gpu-mock`: deterministic GPU test backend wiring.
- `executor-pool`: retained worker pool for parallel execution.
- `lockfree-queues`: optional lock-free bounded edge queues.
- `metrics`: runtime metrics hooks.

Internal finite graph edges keep compatibility-oriented FIFO behavior unless the graph or runtime config selects bounded/latest policies. Host-facing streaming paths should choose explicit queue policies.

## Production API Shape

Prefer fallible construction APIs in release-facing hosts:

- `Executor::try_new` and `OwnedExecutor::try_new` for runtime-plan execution.
- `try_with_active_nodes`, `try_with_active_nodes_mask`, `try_with_active_edges_mask`, and `try_with_active_direct_edges_mask` for demand slices and masks.
- `GraphBuilder::try_connect_ports`, `try_connect_to_nested`, `try_connect_from_nested`, and other `try_*` graph wiring helpers.
- `PortHandle` or explicit `(node, port)` tuple endpoints for node wiring. Dotted string endpoints are convenience shorthand and are best kept out of release-facing host code.

The non-`try` variants remain useful for examples, tests, and compile-time-fixed graph construction, but they intentionally panic on invalid wiring or invalid masks.
