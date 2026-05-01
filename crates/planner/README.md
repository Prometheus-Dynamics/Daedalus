# daedalus-planner

Graph validation and runtime-plan input generation.

## Owns

- graph, node, edge, and port references,
- diagnostics and golden-friendly error payloads,
- registry hydration,
- type validation,
- adapter path resolution,
- embedded group lowering,
- GPU segment annotation,
- scheduling metadata,
- plan explanation and patch helpers.

The planner is deterministic by design. Runtime execution state, handler dispatch, queues, and host bridge behavior live in `daedalus-runtime`.

## Scoped Lowerings

Use an owned `PlannerLoweringRegistry` when lowerings must be isolated by host, plugin set, tenant, or test:

```rust
use daedalus_planner::{PlannerConfig, PlannerLoweringRegistry};

let config = PlannerConfig {
    lowerings: PlannerLoweringRegistry::new(),
    ..PlannerConfig::default()
};
```

`register_planner_lowering` and `PlannerLoweringRegistry::global()` are convenience paths for simple binaries and examples. Release-facing planning should prefer `PlannerConfig { lowerings, .. }` so custom lowerings do not leak across independent runs.
