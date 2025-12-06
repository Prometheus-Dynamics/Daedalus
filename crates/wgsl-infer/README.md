# daedalus-wgsl-infer

Small helpers for inferring WGSL bindings and workgroup sizes from shader source.

## What it does
- Parses `@binding` declarations to infer resource access kinds.
- Detects `@workgroup_size(...)` annotations for dispatch configuration.

## Typical usage
- Used by the Daedalus GPU macro tooling to validate shader bindings.
- Can be reused to preflight WGSL snippets before compilation.
