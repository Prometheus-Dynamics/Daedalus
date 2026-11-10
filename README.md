# Daedalus, `next`

This branch is a clean-slate rebuild of Daedalus. `dev` is the current code and the frozen reference.

Graphs run end to end: the core, the planner, the runner and host, the CPU backend with its
chip forms, the GPU backend, plugins and workers in other languages, and the service
(`daedalusd` and `daedalus-ctl`) are merged. `python3 scripts/pr-status.py` shows what is left.

- [`docs/README.md`](docs/README.md): the documentation, by subject.
- [`examples/`](examples): `00_quickstart` is the shortest program that runs a graph.
- [`AGENTS.md`](AGENTS.md): how to pick up work.
- [`docs/design.md`](docs/design.md): the design.
- [`docs/architecture.md`](docs/architecture.md): the full walkthrough and plan
  ([with diagrams](docs/architecture.html)).
- [`TODO.md`](TODO.md): every planned PR.

MIT OR Apache-2.0.
