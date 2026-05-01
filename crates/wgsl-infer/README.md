# daedalus-wgsl-infer

Small WGSL metadata inference helper.

## Owns

- `@workgroup_size(...)` extraction,
- `@binding(...)` resource access inference,
- a lightweight inferred spec model for macro and GPU tooling.

This crate is intentionally narrow. It is a preflight helper, not a complete WGSL parser or shader compiler.
