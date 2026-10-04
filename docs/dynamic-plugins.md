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
`daedalus_plugin_descriptor`), so use it once per final shared library. A plugin crate that other
crates (hosts, or plugins with `deps`) link must not invoke it in its own library: the symbols
would end up in every dependent `cdylib` next to that plugin's own and fail to link (a cargo
feature gate does not help, since features unify, e.g. under `--all-features`). Export it from a
small leaf crate instead, `crate-type = ["cdylib"]` with one `export_plugin!` line, as
`examples/plugins/example_project_dylib` does for `examples/plugins/example_project`. The plugin's manifest
version defaults to the crate version, which graph documents can require
(`PluginRequirement::new("demo.math").with_version(">=1.0.0")`). `plugin_descriptor!` builds the
same descriptor without exporting anything, for a crate that exports the two symbols itself (to
adjust the descriptor, as `examples/plugins/stable_abi` does).

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
    // Rust ABI for plugins from this build, the stable path for other toolchains/patch releases.
    let path = library.install_into(&mut registry)?;
    tracing::info!(plugin = %schema.plugin.name, ?path, "installed plugin");
    libraries.push(library);
}
```

`discover_plugin_libraries` returns `.so` / `.dylib` / `.dll` files sorted by file name, skips
missing directories, and keeps the first occurrence of a file name across directories.

## Install Paths

`install_into` installs a plugin one of two ways and returns which (`InstallPath`);
`PluginLibrary::install_mode` tells beforehand, and `install_into_as` forces one (e.g. the stable
path for a same-build plugin, to test it):

| | `InstallPath::RustAbi` | `InstallPath::Stable` |
| --- | --- | --- |
| Taken when | `rust_abi()` is `Ok` | `rust_abi()` differs and `stable_abi_version()` equals the host's `STABLE_ABI_VERSION` |
| Requires | same Daedalus version, rustc and build fingerprint; boundary types identical to the host's | same `PLUGIN_ABI_VERSION` and `STABLE_ABI_VERSION`; foreign interfaces identical |
| Installs | everything the plugin registers (nodes, adapters, serializers, capabilities, types) | the schema's nodes, each with a host handler calling the plugin |
| Values | Rust payloads, no conversion | builtins, `Value`s, `ToValue`/`Deserialize` types, foreign handles |
| Per call | nothing | about 1 µs (below) |

Both check dependencies and foreign interfaces first; only the Rust ABI path compares boundary
types (no Rust type crosses the stable path). When neither path is available, `install_into` fails
with `PluginLibraryError::StableAbiMismatch`, naming the Rust ABI mismatch too.

### The stable handler path

The descriptor (`PLUGIN_ABI_VERSION` 8) carries `StableHandlers { version, invoke, release }`,
versioned on its own by `STABLE_ABI_VERSION`, so the value encoding can evolve without breaking
loading and introspection.

- **Plugin side.** On its first `invoke` the plugin builds a private registry (linked
  dependencies, then itself) with node port codecs recorded: for every port with a concrete Rust
  type the node macros register how it converts from a `Value` (builtin conversions, then
  `DaedalusTypeExpr::from_value`, then serde) and to one (`ToValue`). Codecs are keyed by node
  port, not type key, since structural keys (`typeexpr:List(Int)`) name no single Rust type. A
  call names a node by its index in the schema's `nodes`, decodes the inputs into payloads of the
  handler's types, runs the handler, and encodes what it pushed (builtins directly, then `Value`s,
  the port codec, the registry's value serializers). Node state stays in the plugin, one state
  store per host node instance; the host releases it when the node's state is dropped. Errors
  and panics come back as a status code (handler error, invalid input, backpressure, panic) and a
  message; panics are caught in the plugin, so the host gets a `NodeError` and keeps running.
- **Values** cross as `StableValue`s, a `#[repr(C)]` mirror of ffi-core's `WireValue` model
  (plus `Tuple`/`Map`, so `Value`s cross unchanged, and `u64` beyond `i64::MAX`): scalars inline
  with no allocation; strings, bytes and nested values borrowed for the call, copied once into
  the receiver's owned Rust value, never serialized; foreign handles by pointer, so a `FrameView`
  input reads the host's frame in place (the plugin clones the handle, which retains through the
  owner's own functions).
- **Host side.** The host registers the schema's node declarations
  (`daedalus_ffi_host::node_decl_from_schema`, so fire modes, conditional outputs and other node
  metadata carry over) with handlers that encode the inputs, call `invoke` and decode the outputs:
  builtin port types into their Rust types, everything else as a `Value` payload under the port's
  key (downstream nodes coerce it as they do graph constants). Host inputs must be builtins,
  `Value`s, types with a registered value serializer, or foreign handles.
- **Limits.** Only nodes install: a plugin's adapters, serializers, capabilities, named type
  schemas, device transports and boundary contracts stay in the plugin. Typed values the host
  lacks the Rust type for arrive as `Value`s. Fan-in inputs work when the inner type converts.

`examples/plugins/stable_abi` is a plugin whose descriptor claims another rustc; the facade's
`dylib_stable` test installs it through the stable path and compares every node with the static
install.

**Performance** (release, x86-64 desktop, `cargo test -p daedalus-rs --features
engine,dylib-plugins --release --test dylib_stable -- --ignored --nocapture`): a scalar node
handler costs about 0.4 µs statically and 1.2 µs through the stable path (+0.8 µs: the input and
output payloads are rebuilt on the other side, node state is looked up on both sides, plus the
encode/decode); in a host graph tick, +1.0 µs per stable node with scalars, +1.2 µs with a 4 KiB
`Vec<u8>` (one copy in, none out), and about +1.9 µs per node for a derived struct, which crosses
as a `Value` and is rebuilt through serde. Frames cross as handles at the scalar cost.

## Plugin Dependencies

A plugin whose nodes use types another plugin registers (a frame library's integration plugin
that owns `styx:framelease` and provides `daedalus:frame`, say) declares the dependency and, when
it can, links that plugin into its library:

```rust
#[plugin(id = "eidos", deps("styx.frames"), nodes(to_gray))]
pub struct EidosPlugin;

daedalus::export_plugin!(EidosPlugin, deps [styx_core::daedalus_integration::StyxPlugin]);
```

- **Declared** (`#[plugin(deps(...))]`, plus every linked plugin): the schema lists the
  dependencies (`PluginSchema::dependencies`), and `install_into` fails with
  `PluginLibraryError::MissingDependencies` naming every one the host registry has not
  installed, before calling into the plugin. Install the dependency in the host first, from the
  host's build: it is never installed from the plugin's library.
- **Linked** (`export_plugin!(.., deps [Plugin, ...])`, each `Plugin + Default`): the
  descriptor's introspection entry points (`schema`, `boundary_types`, `foreign_interfaces`)
  install the linked plugins before the plugin into their private registry, so the keys they own
  or map (`foreign_types`) resolve and their adapters, providers and types are there. Their
  boundary types and foreign interfaces are part of the exported tables, so the host checks the
  plugin's copy of them too. Linking matters for types that declare no key themselves (a
  dependency's `foreign_types` mapping): node macros resolve those through the registry they
  install into, and in the introspection registry only a linked dependency adds the mapping. At
  `install_into` the host registry, where the dependency is installed first, provides it. Types
  that own their key (`#[type_key]`, `DaedalusTypeExpr`) resolve without it.
- **Not linked**: introspection runs in a lenient mode. A port type from another crate without a
  key is recorded instead of failing (`PluginRegistry::record_external_types`), listed in the
  schema as `plugin.metadata.external_types` (`owner`, `port`, `rust_type`), and keeps its
  `rust:` key; the schema loads, but installing fails with `UnkeyedForeignType`. Link the
  dependency (or give the port a `type_key`).

`examples/plugins/dependent` depends on and links the example plugin (whose `Lease` type has no
key of its own); the facade's `dylib_plugin`, `dylib_linked_deps` and `dylib_external_types`
tests cover the three cases.

## ABI And Compatibility Rules

The `daedalus::dylib` module docs ([source](../crates/daedalus/src/dylib/mod.rs), rendered on
[docs.rs](https://docs.rs/daedalus-rs/latest/daedalus/dylib/index.html)) are the reference for the
two ABI layers, the build fingerprint, feature classification, and known limitations. In short:

- `load` only needs a matching `PLUGIN_ABI_VERSION`; the C-ABI descriptor and `PluginLibrary::schema`
  work across toolchains, Daedalus versions, and feature sets.
- The Rust-ABI install path passes Rust types across the boundary, so it requires the same
  Daedalus version, `rustc --version`, and build fingerprint (`daedalus::build_fingerprint()`);
  `rust_abi()` names the differing segments, and forcing the path fails with
  `PluginLibraryError::Incompatible`. Mismatched plugins install through the stable path instead
  (see [Install Paths](#install-paths)), which needs only matching `PLUGIN_ABI_VERSION` and
  `STABLE_ABI_VERSION`.
- Before calling into the plugin, the Rust-ABI path also compares the plugin's boundary types (every
  type key its nodes and adapters consume or produce and every type it registers, with the Rust
  type behind it: `TypeId` hash, size, align, name; `PluginLibrary::boundary_types()`) against
  the host registry's (`PluginRegistry::boundary_types()`). A key the host maps to another Rust
  type fails with `PluginLibraryError::BoundaryTypeConflict`, listing every such key as a
  `BoundaryTypeConflict` (the same type static installs fail with,
  `PluginError::BoundaryTypeConflict`); keys the host does not know are accepted. After a
  successful install the plugin's table is recorded in the host registry, so later plugins, typed
  pushes and payloads fed to the host bridge are checked against it. `PLUGIN_ABI_VERSION` 6 added
  this table to the descriptor.
- Both paths compare the foreign interfaces the plugin uses (key, version, vtable
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
`type_key`. List the owner's plugin in `deps(...)` so the requirement is explicit, and link it
(see [Plugin Dependencies](#plugin-dependencies)). See "Library-Owned
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

Even a dependency built with an extra feature (for example the example crate's no-op
`separate-build` feature, which `examples/plugins/example_project_dylib` enables while the host
links the example crate without it) yields different types; the facade's `dylib_plugin` test
shows the refusal.

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
on process globals (see the module docs). Plugins whose nodes only exchange builtins, `Value`s,
`ToValue`/`Deserialize` types and foreign interface handles need none of this: the
[stable path](#install-paths) runs them across toolchains and Daedalus patch releases.

`smallvec` is built with its `union` feature everywhere: it changes `SmallVec` layouts, which
`NodeIo` exposes to plugin handlers, and a host linking wgpu (whose HAL enables it) would
otherwise lay `NodeIo` out differently from a plugin without wgpu. `NodeIo` is part of the
fingerprint, so such a difference is a typed `Incompatible` error rather than a crash.

## Coming From The Pre-Release FFI

| Before | Now |
| --- | --- |
| facade feature `ffi` | `dylib-plugins` for native Rust plugins (both plugin and host); `daedalus-ffi-*` crates for other languages |
| `daedalus::FfiPluginError` | `daedalus::PluginLibraryError` |
| four symbols (`abi_version`, `info`, `register_boundary_contracts`, `register`) | `daedalus_plugin_abi_version` + `daedalus_plugin_descriptor` (`PluginDescriptor`) |
| `PluginErrorSink` | `StrSink` |
| `check_plugin_info`, `*VersionMismatch`/`BuildFingerprintMismatch` load errors | `check_rust_abi` → `RustAbiMismatch`; mismatched plugins install through the stable path (`InstallPath::Stable`), `StableAbiMismatch` when that differs too |
| `library.abi_version()` | removed (a loaded library always has `PLUGIN_ABI_VERSION`) |
| `HOST_ONLY_FEATURES` | `[package.metadata.daedalus]` in each crate's `Cargo.toml` |
| facade feature `gpu` | `gpu` (alias of `gpu-engine`); CPU-only hosts need only `engine-full,plugins` (or `embedded`) |
