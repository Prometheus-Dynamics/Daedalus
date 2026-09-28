# Native Dynamic Plugins

Rust plugins can ship as `cdylib` shared libraries and be loaded by a host at startup. Plugins in
other languages (Python, Node, Java, C/C++) use the package and worker model in
[`crates/ffi`](../crates/ffi/README.md) instead.

## Plugin Crate

```toml
[lib]
crate-type = ["rlib", "cdylib"]

[dependencies]
daedalus = { package = "daedalus-rs", version = "2.0.0", features = ["plugins"] }
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

`export_plugin!` emits unmangled symbols, so use it once per final shared library. The plugin's
manifest version defaults to the crate version, which graph documents can require
(`PluginRequirement::new("demo.math").with_version(">=1.0.0")`).

## Host

Enable the `dylib-plugins` feature:

```rust
use daedalus::{PluginLibrary, PluginRegistry, discover_plugin_libraries};

let mut registry = PluginRegistry::new();
let mut libraries = Vec::new();
for path in discover_plugin_libraries(["/usr/lib/app/plugins", "/var/lib/app/plugins"])? {
    // Safety: plugins are trusted and built with the same toolchain and Daedalus build.
    let library = unsafe { PluginLibrary::load(&path) }?;
    library.install_into(&mut registry)?;
    let info = library.info();
    tracing::info!(
        path = %library.path().display(),
        name = info.plugin_name.as_str(),
        version = info.plugin_version.as_str(),
        "loaded plugin"
    );
    libraries.push(library);
}
```

`discover_plugin_libraries` returns `.so` / `.dylib` / `.dll` files sorted by file name, skips
missing directories, and keeps the first occurrence of a file name across directories.

## Compatibility Rules

`PluginLibrary::load` rejects a plugin with a typed `PluginLibraryError` before any Rust type
crosses the boundary unless the plugin ABI version, Daedalus version, `rustc --version`, and
build fingerprint (target, layout-affecting features, `PluginRegistry` layout) match the host.
In practice: build the host and its plugins from one workspace and lockfile with one toolchain
and one Daedalus feature set.

Libraries are never unloaded; only load-at-startup is supported. A plugin has its own copy of
Daedalus globals, so it must register everything through the `PluginRegistry` it is given, and
neither side may install a custom `#[global_allocator]`. See the `daedalus::dylib` module docs
for the full list of limitations.

## Coming From The Pre-Release FFI

| Before | Now |
| --- | --- |
| facade feature `ffi` | `dylib-plugins` for native Rust plugins; `daedalus-ffi-*` crates for other languages |
| `daedalus::FfiPluginError` | `daedalus::PluginLibraryError` |
| `library.info() -> Option<PluginInfo>` | `library.info() -> PluginInfo` |
| `library.abi_version() -> Option<u32>` | `library.abi_version() -> u32` |
| facade feature `gpu` | `gpu` (alias of `gpu-engine`); CPU-only hosts need only `engine,plugins` |
