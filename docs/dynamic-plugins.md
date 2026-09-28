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

let mut registry = PluginRegistry::new();
let mut libraries = Vec::new();
for path in discover_plugin_libraries(["/usr/lib/app/plugins", "/var/lib/app/plugins"])? {
    // Safety: plugins are trusted Daedalus plugins.
    let library = unsafe { PluginLibrary::load(&path) }?;
    let schema = library.schema(); // readable even if the plugin cannot be installed
    tracing::info!(path = %library.path().display(), plugin = %schema.plugin.name,
        nodes = schema.nodes.len(), "found plugin");
    library.install_into(&mut registry)?; // PluginLibraryError::Incompatible on a mismatch
    libraries.push(library);
}
```

`discover_plugin_libraries` returns `.so` / `.dylib` / `.dll` files sorted by file name, skips
missing directories, and keeps the first occurrence of a file name across directories.

## ABI And Compatibility Rules

A plugin exposes two layers:

- **Stable descriptor (C ABI).** `daedalus_plugin_abi_version` is frozen as
  `extern "C" fn() -> u32`. When it equals `PLUGIN_ABI_VERSION`, `load` calls
  `daedalus_plugin_descriptor`, a `#[repr(C)]` `PluginDescriptor` made only of C types:
  `PluginInfo` (plugin name/version, Daedalus version, rustc version, build fingerprint as
  `StrView`s) and `extern "C"` entry points. Its `schema` entry point installs the plugin into a
  private registry inside the plugin and returns the manifest and node declarations as a
  `daedalus_ffi_core::PluginSchema` (JSON), the same language-neutral schema FFI packages use.
  None of this depends on the plugin's toolchain, Daedalus version or feature set, so `load`
  accepts any plugin with a matching ABI version and `PluginLibrary::schema` describes it.
  A wrong ABI version, missing symbol, invalid info string or invalid schema fails `load` with a
  typed `PluginLibraryError`.
- **Rust-ABI install.** Handlers are Rust closures over `Payload` (`Arc<dyn Any>`), so
  `install_into` passes a `*mut PluginRegistry` to the plugin. That is only sound when both sides
  agree on every layout, so `check_rust_abi` (surfaced as `PluginLibrary::rust_abi`) requires
  the Daedalus version, `rustc --version` and build fingerprint to match the host, and
  `install_into` returns `PluginLibraryError::Incompatible { plugin, mismatch }` without calling
  into the plugin otherwise.

The fingerprint (`daedalus::build_fingerprint()`) is a readable `key=value;...` string: target
and pointer width; the enabled boundary Cargo features of the facade, `daedalus-core`, `-data`,
`-registry`, `-planner` and `-runtime` (`features.<crate>=...`) plus a stable FNV-1a
`features.hash`; and `layout.<type>=size/align` for `PluginRegistry`, `HandlerRegistry`,
`Payload`, `TypeKey`, `BoundaryTypeContract`, `TypeExpr`, `NodeDecl`, `AdapterDecl`,
`PluginManifest` and `StrView`. A mismatch error lists the differing segments, e.g.
``features.runtime: host `gpu,plugins`, plugin `plugins` ``.

Every feature of those crates is classified next to its definition, in the crate's `Cargo.toml`:

```toml
[package.metadata.daedalus]
boundary-features = ["gpu", "gpu-mock", "plugins"]
host-only-features = ["executor-pool", "lockfree-queues", "metrics", "snapshots"]
```

Only enabled boundary features enter the fingerprint, so a plugin built with `dylib-plugins`
installs into a host built with `engine-full,dylib-plugins`. Each crate's build script exports its
enabled features (`ENABLED_FEATURES`, from `CARGO_CFG_FEATURE`) and the crate embeds its manifest
(`CARGO_MANIFEST`); a facade test fails when a declared feature is unclassified or classified
twice.

In practice: build the host and its plugins from one workspace and lockfile with one toolchain
and one boundary Daedalus feature set (`plugins`, `gpu-types`/`gpu-runtime`/backends, `schema`,
`proto`).

Libraries are never unloaded; only load-at-startup is supported. A plugin has its own copy of
Daedalus globals, so it must register everything through the `PluginRegistry` it is given, and
neither side may install a custom `#[global_allocator]`. See the `daedalus::dylib` module docs
for the full list of limitations.

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

Costs: one encode/decode per call and no zero-copy for GPU or large buffers (a later
`WireValue::Handle`-style borrowed view could address the latter).

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
