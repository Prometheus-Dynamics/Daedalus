# daedalus-daemon

Optional long-lived daemon and CLI for warm Daedalus engine workflows.

This crate is intentionally separate from `daedalus-engine` so the engine crate stays library-first.

Current scope:

- hold a warm `Engine` instance in a long-lived process
- store graphs and `CapabilityRegistry` values by session/name
- plan and build against warm state
- expose cache, latest-summary, and trace inspection

Current non-goals:

- pretending to own arbitrary runtime data movement
- handler-backed graph execution without an explicit handler transport model

Usage examples:

```bash
cargo run -p daedalus-daemon -- stdio
cargo run -p daedalus-daemon --features tcp -- serve 127.0.0.1:4100
cargo run -p daedalus-daemon --features tcp -- inspect-state --addr 127.0.0.1:4100
```

The `tcp` feature is optional. The default build keeps the daemon on the simpler `stdio` process model.
