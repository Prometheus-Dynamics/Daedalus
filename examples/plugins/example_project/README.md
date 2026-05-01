# Daedalus Rust Example Project (Plugin Crate)

This is a minimal Rust plugin crate that demonstrates the “native” (macro-based) way to author nodes.

It’s intended to be copy/pasted into a new repo (or used as a template) when you want the closest ergonomics to the Rust system.

## What you get

- `src/lib.rs`: a small set of nodes showing:
  - stateless typed nodes
  - config-backed inputs (`#[derive(NodeConfig)]`)
  - stateful nodes (`state(MyState)`)
  - metadata-only port source (`port(... source=...)`)
  - capability-dispatch node + capability registration in `install`

## Build

From repo root:

```bash
cargo build -p daedalus-plugins-example-project
```

## Use in an app

In your host app (Rust), install the plugin and build graphs from the registered nodes:

```rust
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus_plugins_example_project::ExampleProjectPlugin;

let mut plugins = PluginRegistry::new();
plugins.install_plugin(&ExampleProjectPlugin::new()).unwrap();
```

