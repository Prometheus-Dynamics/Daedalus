# Daedalus, `next`

This branch is a clean-slate rebuild of Daedalus. `dev` is the current code and the frozen reference.

The foundation (group A) and the planner (group B) are merged: the core interfaces, the `#[node]`
and `#[transfer]` macros, the registry, a simulated backend, the backend conformance suite, and a
planner that turns a graph into a proven plan. The runner and host (group C) are in progress.
Nothing runs a graph end to end yet.

- [`AGENTS.md`](AGENTS.md): how to pick up work.
- [`docs/design.md`](docs/design.md): the design.
- [`docs/architecture.md`](docs/architecture.md): the full walkthrough and plan
  ([with diagrams](docs/architecture.html)).
- [`TODO.md`](TODO.md): every planned PR.

MIT OR Apache-2.0.
