# daedalus-daemon

Optional long-lived process around `daedalus-engine`.

## Scope

- keep a warm engine process alive,
- store named graph and registry state,
- prepare and build plans against warm state,
- inspect cache state, summaries, and traces,
- expose stdio by default and TCP when the `tcp` feature is enabled.

The daemon is process tooling. It does not replace the Rust library-first engine API.
