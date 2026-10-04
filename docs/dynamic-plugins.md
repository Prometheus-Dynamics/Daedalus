# Native Dynamic Plugins

Rust plugins can ship as `cdylib` shared libraries and be loaded by a host at startup. Plugins in
other languages (Python, Node, Java, C/C++) use the package and worker model in
[`crates/ffi`](../crates/ffi/README.md) instead.

## Plugin Crate

```toml
[lib]
crate-type = ["rlib", "cdylib"]

[dependencies]
daedalus = { package = "daedalus-rs", version = "2.0.0", features = ["dylib-plugins"] }
```

```rust
use daedalus::{macros::{node, plugin}, runtime::NodeError};

#[node(id = "add", inputs("a", "b"), outputs("out"))]
fn add(a: i64, b: i64) -> Result<i64, NodeError> {
    Ok(a + b)
}

#[plugin(id = "demo.math", nodes(add))]
pub struct MathPlugin;

daedalus::export_plugin!(MathPlugin);
```

`export_plugin!` emits two unmangled symbols (`daedalus_plugin_abi_version`,
`daedalus_plugin_descriptor`), so use it once per final shared library. The plugin's manifest
version defaults to the crate version, which graph documents can require
(`PluginRequirement::new("demo.math").with_version(">=1.0.0")`).

## Host

Enable the `dylib-plugins` feature:

```rust
use daedalus::{PluginLibrary, PluginRegistry, discover_plugin_libraries};
use daedalus::runtime::plugins::RegistryPluginExt;

let mut registry = PluginRegistry::new();
// Types the host shares with plugins first (e.g. the frame library's own Daedalus plugin), so
// `install_into` can check the plugins use the same Rust types for those keys.
registry.install_plugin(&styx_core::daedalus_integration::StyxPlugin::new())?;
let mut libraries = Vec::new();
for path in discover_plugin_libraries(["/usr/lib/app/plugins", "/var/lib/app/plugins"])? {
    // Safety: plugins are trusted Daedalus plugins.
    let library = unsafe { PluginLibrary::load(&path) }?;
    let schema = library.schema(); // readable even if the plugin cannot be installed
    tracing::info!(path = %library.path().display(), plugin = %schema.plugin.name,
        nodes = schema.nodes.len(), "found plugin");
    library.install_into(&mut registry)?; // Incompatible / BoundaryTypeConflict on a mismatch
    libraries.push(library);
}
```

`discover_plugin_libraries` returns `.so` / `.dylib` / `.dll` files sorted by file name, skips
missing directories, and keeps the first occurrence of a file name across directories.

## ABI And Compatibility Rules

The `daedalus::dylib` module docs ([source](../crates/daedalus/src/dylib/mod.rs), rendered on
[docs.rs](https://docs.rs/daedalus-rs/latest/daedalus/dylib/index.html)) are the reference for the
two ABI layers, the build fingerprint, feature classification, and known limitations. In short:

- `load` only needs a matching `PLUGIN_ABI_VERSION`; the C-ABI descriptor and `PluginLibrary::schema`
  work across toolchains, Daedalus versions, and feature sets.
- `install_into` passes Rust types across the boundary, so it requires the same Daedalus version,
  `rustc --version`, and build fingerprint (`daedalus::build_fingerprint()`), and otherwise fails
  with `PluginLibraryError::Incompatible` naming the differing segments.
- Before calling into the plugin, `install_into` also compares the plugin's boundary types (every
  type key its nodes and adapters consume or produce and every type it registers, with the Rust
  type behind it: `TypeId` hash, size, align, name; `PluginLibrary::boundary_types()`) against
  the host registry's (`PluginRegistry::boundary_types()`). A key the host maps to another Rust
  type fails with `PluginLibraryError::BoundaryTypeConflict`, listing every such key as a
  `BoundaryTypeConflict` (the same type static installs fail with,
  `PluginError::BoundaryTypeConflict`); keys the host does not know are accepted. After a
  successful install the plugin's table is recorded in the host registry, so later plugins, typed
  pushes and payloads fed to the host bridge are checked against it. `PLUGIN_ABI_VERSION` 6 added
  this table to the descriptor.
- `install_into` also compares the foreign interfaces the plugin uses (key, version, vtable
  layout hash; `PluginLibrary::foreign_interfaces()`) with the host registry's
  (`PluginRegistry::foreign_interfaces()`) and fails with
  `PluginLibraryError::ForeignInterfaceMismatch` when one differs. `PLUGIN_ABI_VERSION` 7 added
  this table.
- **Rust-ABI plugins that share third-party Rust types with the host must come from the same
  cargo build as the host**: one workspace and
  lockfile, one toolchain, and one `cargo build` invocation for the host and its plugins (e.g.
  `cargo build -p app -p app-plugin-a -p app-plugin-b`), so Cargo resolves Daedalus *and every
  other shared dependency* once, with one feature set. Plugins that share no third-party Rust
  types with the host (they read them through foreign interfaces) can be built separately; see
  [Separately Built Plugins](#separately-built-plugins). Libraries are never unloaded, plugins must
  register everything through the `PluginRegistry` they are given, and neither side may install
  a custom `#[global_allocator]`.

## Types Owned By Other Crates

A plugin that uses a type from another crate (a camera library's `FrameLease`, say) depends on
the crate that owns the type, with that crate's optional `daedalus` feature, so port keys come
from the type itself; it never registers the type under a key of its own. When the owner has no
integration, the plugin declares the key with `#[plugin(foreign_types(Type = "key"))]` or a port
`type_key`. List the owner's plugin in `deps(...)` so the requirement is explicit. See "Library-Owned
Integration Features" in [`node-authoring.md`](node-authoring.md#library-owned-integration-features).

The fingerprint only covers Daedalus crates, so a plugin built in a separate cargo invocation that
resolved the owner crate with different features passes it, yet its `FrameLease` is a different
Rust type: same key, same type name, different `TypeId`. Before the boundary type check every
frame then failed at runtime with `payload type mismatch: expected styx:framelease, found
styx:framelease`. Now:

- if the host registered the type (it installed the owner's plugin, or called
  `registry.register_boundary_type::<FrameLease>("styx:framelease")`), `install_into` refuses the
  plugin up front:
  ``plugin `helios_cv` uses type keys for different Rust types than the host (`styx:framelease`:
  host `styx_core::frame::FrameLease` (type id ..., size 48, align 8), plugin ...); Rust-ABI
  plugins must come from the same cargo build as the host, ...``;
- a payload the host feeds under a registered key but built with another Rust type (say, by a
  separately built frame source) is refused at the host bridge, before any node runs:
  `FeedOutcome::Rejected` with ``payload for `styx:framelease` holds `...` but this graph
  expects `styx_core::frame::FrameLease` (built separately?)``;
- a downcast that still fails names both Rust types: ``payload type mismatch: same TypeKey
  `styx:framelease`, different Rust type (expected `...`, found `...`); the producer and consumer
  were likely built separately``.

Even a dependency built with an extra feature (for example the plugin crate's own `dylib`
feature, when the host also links that crate) yields different types; the facade's
`dylib_plugin` test shows the refusal.

## Separately Built Plugins

Plugins shipped as separate artifacts (built in their own cargo invocation, possibly long after
the host) can be installed when:

- **Daedalus matches**: same Daedalus version, `rustc --version` and build fingerprint (boundary
  features and the layouts of the types installation and node calls touch). Build plugins
  against the host's Daedalus revision and boundary feature set.
- **Shared third-party types cross through foreign interfaces**, never as Rust types. A plugin
  node takes `FrameView<'_>` (the `daedalus:frame` interface, see
  [`foreign-frame-interface.md`](foreign-frame-interface.md)) or `ForeignRef<'_, I>` instead of
  `&styx_core::FrameLease`; the host installs the owner's plugin, which registers the provider.
  The plugin's ports then carry the interface key and no Rust boundary type, so the boundary
  type check has nothing to refuse, while the foreign interface check refuses a plugin built
  against another version of the interface. Frames are not copied: the plugin reads the host's
  buffer through the owner's accessor functions. See "Foreign Interfaces" in
  [`node-authoring.md`](node-authoring.md#foreign-interfaces).
- **Other boundary types are `std` types or the plugin's own.** Values the plugin and host
  exchange otherwise (scalars, strings, `Vec`s) are read through the value's own `Any`, so they
  work even when the plugin's copy of Daedalus has other `TypeId`s (any dependency of Daedalus
  resolved with different features changes those, without changing the fingerprint).

`examples/plugins/foreign_consumer` is such a plugin: built in a separate `cargo build` with a
deliberately different copy of the example crate, it installs into a host that owns `Counter`
and reads host counters in place through `example:counter_view` (the facade's `dylib_plugin`
test).

What still requires one cargo build: plugins whose nodes take or return a shared third-party
type directly, value serializers or capabilities keyed by such types, and anything that relies
on process globals (see the module docs). Toolchain or Daedalus-version independence needs the
stable handler path below.

`smallvec` is built with its `union` feature everywhere: it changes `SmallVec` layouts, which
`NodeIo` exposes to plugin handlers, and a host linking wgpu (whose HAL enables it) would
otherwise lay `NodeIo` out differently from a plugin without wgpu. `NodeIo` is part of the
fingerprint, so such a difference is a typed `Incompatible` error rather than a crash.

## Design Note: Stable Handler Path

Today a mismatched plugin is introspectable but not runnable. Lifting the same-build requirement
for plugins whose ports carry wire-representable values needs a handler path that never passes
Rust types:

1. **Descriptor entry point.** Add `invoke(node: StrView, request: *const u8, len: usize,
   sink: StrSink) -> bool` exchanging `daedalus_ffi_core::{InvokeRequest, InvokeResponse}`
   (`WireValue` payloads) as bytes, plus instance ids for stateful nodes. Bump
   `PLUGIN_ABI_VERSION`.
2. **Typed wire codecs (the blocker).** Macro-generated handlers read inputs with
   `NodeIo::take_owned::<T>`, which needs the exact Rust type in the `Payload`; a
   `Payload::owned(key, Value)` only coerces through `get_typed`. Neither side can build a typed
   payload from a type key today: `value_serializers` (typed → `Value`) are keyed by `TypeId`
   and const coercers (`Value` → typed) by Rust type name and return `Box<dyn Any>`. The registry
   needs a codec table keyed by transport `TypeKey` (`Value` ↔ `Payload` of `T`), registered by
   the node macros for boundary-contract types.
3. **Plugin side.** `invoke` decodes inputs with the plugin's codecs, runs the handler from its
   private registry through `NodeIo::from_inputs`, and encodes outputs.
4. **Host side.** Install the schema's declarations (`daedalus_ffi_host::node_decls_from_schema`)
   and register host handlers that encode inputs, call `invoke`, and decode outputs with the
   host's codecs.
5. **Negotiation.** `install_into` keeps the Rust-ABI fast path when `rust_abi()` is `Ok`, uses
   the stable path when every schema port has a codec on both sides, and otherwise reports
   `Incompatible` naming the offending ports.

Costs: one encode/decode per call and no zero-copy for GPU or large buffers. Foreign handles
already are such a borrowed view: `ForeignHandle` is `#[repr(C)]` and only calls the owner's
`extern "C"` functions, so the stable path can pass frames as handles instead of bytes.

## Coming From The Pre-Release FFI

| Before | Now |
| --- | --- |
| facade feature `ffi` | `dylib-plugins` for native Rust plugins (both plugin and host); `daedalus-ffi-*` crates for other languages |
| `daedalus::FfiPluginError` | `daedalus::PluginLibraryError` |
| four symbols (`abi_version`, `info`, `register_boundary_contracts`, `register`) | `daedalus_plugin_abi_version` + `daedalus_plugin_descriptor` (`PluginDescriptor`) |
| `PluginErrorSink` | `StrSink` |
| `check_plugin_info`, `*VersionMismatch`/`BuildFingerprintMismatch` load errors | `check_rust_abi` → `RustAbiMismatch`; `install_into` fails with `Incompatible` |
| `library.abi_version()` | removed (a loaded library always has `PLUGIN_ABI_VERSION`) |
| `HOST_ONLY_FEATURES` | `[package.metadata.daedalus]` in each crate's `Cargo.toml` |
| facade feature `gpu` | `gpu` (alias of `gpu-engine`); CPU-only hosts need only `engine-full,plugins` (or `embedded`) |
