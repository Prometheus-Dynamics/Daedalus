# daedalus-core

Shared primitives for the workspace.

## Owns

- typed ids for nodes, edges, ports, channels, runs, and ticks,
- deterministic logical clocks and message metadata,
- synchronous channel traits and built-in queue implementations,
- compute affinity, sync policy, and backpressure names,
- core errors and optional metrics hooks.

## Does Not Own

- graph planning,
- runtime execution,
- payload type modeling,
- registry state,
- GPU backends,
- FFI contracts.

Keep this crate dependency-light. Higher layers rely on it as stable plumbing.
