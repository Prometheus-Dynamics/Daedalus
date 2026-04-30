# Daedalus FFI

Language-specific FFI surfaces are split by subdirectory.

- `lang/rust/` — Rust-authored plugins built as `cdylib` and loaded via `PluginLibrary`.
- `lang/python/` — Python-side manifest emitter (`daedalus_py`) plus sample plugin.
  - Manifest loader example: `crates/ffi/examples/load_python_manifest.rs` (uses the language-dispatching loader)
- `lang/node/` — Node.js-side manifest emitter (`daedalus_node`) plus sample plugin.
  - Manifest loader example: `crates/ffi/examples/load_python_manifest.rs` (uses the language-dispatching loader)
- `lang/java/` — Java-side manifest emitter + subprocess bridge.
- `lang/c_cpp/` — C/C++ manifest emitter + shared-library bridge.

## Rewrite Contract

The current crate is still manifest-first, but the rewrite now has an explicit contract surface in
`crates/ffi/src/contracts.rs`.

### Layers

- `package_discovery`
  Finds plugin artifacts and package metadata.
- `schema`
  Describes plugin identity, node shape, and port types.
- `host_core`
  Owns install/request/response/state handling on the Rust side.
- `backend_runtime`
  Executes a backend-specific entrypoint.
- `transport`
  Carries typed values between the host core and backend runtime.

Each layer has one job. Runtime process details and transport behavior are no longer part of the
core schema boundary.

### Runtime Models

The rewrite only treats these runtime models as first-class:

- `in_process_abi`
- `persistent_worker`

Spawn-per-call subprocess execution is not a target model for the rewrite.

### Typed Wire Contract

The rewrite target for host/backend payloads is the `WireValue` contract exported by the crate:

- `unit`
- `bool`
- `int`
- `float`
- `string`
- `bytes`
- `image`
- `list`
- `record`
- `enum`

Request/response traffic is expressed as:

- `InvokeRequest`
- `InvokeResponse`
- `InvokeEvent`

The important boundary change is that schema and backend config are separate:

- `PluginSchema` / `NodeSchema`
- `BackendConfig`

That split is deliberate. Plugin/node shape belongs to the schema layer; process/runtime details
belong to the backend runtime layer.
