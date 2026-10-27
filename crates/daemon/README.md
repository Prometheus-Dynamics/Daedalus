# daedalus-daemon

Optional long-lived process around `daedalus-engine`.

## Scope

- keep a warm engine process alive,
- store named graph and registry state,
- prepare and build plans against warm state,
- inspect cache state, summaries, and traces,
- expose stdio by default and TCP when the `tcp` feature is enabled.

## Protocol version

`ping` answers `pong` with `protocol_version` (`PROTOCOL_VERSION` in `src/service.rs`). The
number moves only on an incompatible change to the request envelope or a response shape, so a
client can check it before it sends anything else. Adding an optional response field does not
move it.

The daemon is process tooling. It does not replace the Rust library-first engine API.
