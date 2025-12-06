# daedalus-core

Foundational types shared across the workspace: compute affinity, sync policy, IDs, metrics, errors, and channel utilities.

## Highlights
- Enums for compute affinity (CPU/GPU preferences), sync policies, and metrics.
- Typed IDs for nodes/edges to keep registry/planner/runtime aligned.
- Channel helpers for bounded/unbounded/broadcast/newest delivery semantics.
- Error types shared between planner/runtime/engine.

## Usage
- Imported transitively through the facade crates; rarely used directly.
- Channel implementations are used inside the runtime executors and host bridges.
