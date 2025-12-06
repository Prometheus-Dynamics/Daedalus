# daedalus-nodes

Demonstration and bundle nodes built with the `#[node]` macro. Used as fixtures for planner/runtime tests and as examples for downstream authors.

## What’s inside
- Example nodes covering CPU and GPU paths, branching, metadata handling, and sync policies.
- UI tests (`tests/ui`) that validate macro diagnostics for common authoring mistakes.
- Helpers for bundling and registering node sets.

## Usage
- Treat these nodes as references when authoring your own `#[node]` functions.
- Tests in other crates depend on these descriptors/handlers; keep changes backward-compatible within the workspace.
- Feature-gated bundles in `Cargo.toml` control which node sets are exposed.
