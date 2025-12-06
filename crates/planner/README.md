# daedalus-planner

Plans logical graphs into executable runtime plans. Validates node connections, enforces compute affinities, sync policies, and emits segments suitable for the runtime.

## Responsibilities
- Validate graphs against the registry (node IDs, ports, metadata).
- Compute segment boundaries, sync groups, and ordering constraints.
- Enforce compute affinity (CPU/GPU required/preferred) and emit GPU requirements.
- Produce diagnostic information (missing nodes/ports, cycles, capability gaps).

## Key modules
- `graph`: graph representation used during planning.
- `passes`: validation and shaping passes that produce a `RuntimePlan`.
- `diagnostics`: structured errors/warnings for consumers.
- `helpers`: helpers for building/inspecting graphs in tests.

## Features
- `gpu`: include GPU capabilities in plans.
- `schema` / `proto`: optional export formats for plans/graphs.

## Usage
- Build or load a `Graph` (via registry helpers or manual construction).
- Call `planner::plan(&registry, graph, config)` to get a `RuntimePlan` and diagnostics.
- Feed the plan into `daedalus-runtime` or the `engine` facade.

## Testing
- Golden tests under `crates/planner/tests` cover success/failure cases and GPU capability handling.
