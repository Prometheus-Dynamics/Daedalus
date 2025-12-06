# daedalus-engine

Convenience facade that wires registry → planner → runtime so applications can execute graphs with minimal setup.

## Responsibilities
- Bundle configuration (`EngineConfig`) for planner/runtime/GPU settings.
- Provide a simple `Engine::run(registry, graph, handlers)` entrypoint.
- Surface telemetry and results in a single return value.

## When to use
- If you want an opinionated end-to-end path without manually invoking planner/runtime.
- For examples and small apps; larger systems can compose planner/runtime directly.

## Features
- `gpu`: include GPU planning/runtime support.
- `plugins`: enable plugin installation path.

## Usage
- Build a `Registry` and `HandlerRegistry` (often via `#[node]` macros).
- Construct a graph (see `crates/daedalus/examples`).
- Configure `EngineConfig` (mode, backpressure, GPU backend).
- Call `Engine::run` and inspect telemetry/results.
