# daedalus-macros

Proc macros for node authoring, plugin wiring, transport declarations, typed data, and GPU metadata.
Use them through the `daedalus` facade (`daedalus::macros::node`, `daedalus::{plugin, type_key, adapt, device}`),
which resolves the generated paths to the facade's re-exports.

## Surface

- `node` and `node_handler`: node descriptor + handler generation, and handler-only generation.
- `plugin`: turns a unit struct into a `Plugin` that installs the listed `types(...)`,
  `values(...)`, `nodes(...)`, `adapters(...)`, `devices(...)` and `parts(...)`, with node handle
  accessors and a manifest.
- `type_key`: implements `DaedalusTypeExpr` with a stable opaque key (a string literal or a
  string constant).
- `adapt`: turns a `T`/`&T`/`&mut T`/`Arc<T>` function into a transport adapter and generates
  `register_<fn>_adapter`.
- `device`: pairs an upload function with a `download = ...` function as a typed device
  transport and generates `register_<fn>_device`.
- `NodeConfig`: structured config inputs (`#[port(...)]`, `#[validate(fn = ...)]`).
- `GpuBindings` and `GpuStateful`: WGSL/GPU metadata derives.
- `BranchPayload`, `DaedalusTypeExpr`, and `DaedalusToValue`: data helper derives. Like the
  other macros they resolve through the facade, so a `daedalus-rs` dependency is enough.

See the item docs in `src/lib.rs` for arguments and examples; `crates/daedalus/tests/ui/transport`
has compile-checked usage.

Macro output should remain deterministic because registry snapshots, UI tests, and generated fixtures depend on stable names and diagnostics.
