# Plugins

Optional node bundles published as separate crates. They demonstrate how external packs can integrate with the registry/planner/runtime via the plugin system.

## Included
- `plugins/math`: basic arithmetic and utility nodes.
- `plugins/images`: image-processing nodes.

## Usage
- Depend on the plugin crate and enable the `plugins` feature in the runtime/engine.
- Register the plugin bundle into your `Registry` (or use the engine facade which can install plugins automatically).
- Planner/runtime will then accept the nodes exposed by the plugins.
