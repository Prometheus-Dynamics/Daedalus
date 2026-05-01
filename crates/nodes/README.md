# daedalus-nodes

Built-in and demo node bundles used by examples, tests, and plugin fixtures.

## Owns

- convenience re-export of the `node` macro,
- `declare_plugin!` helper for macro-generated plugin structs,
- feature-gated starter, utility, and demo bundles,
- fixtures that exercise registry/runtime/plugin integration.

Production applications can use these as references, but should keep domain-specific node libraries in their own crates.
