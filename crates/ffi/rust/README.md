# Daedalus FFI Rust SDK

Rust SDK for out-of-process Daedalus plugins. A plugin is written the same way as an in-process
Rust plugin (`#[node]`, `declare_plugin!`), and `Worker` serves it as a persistent worker over the
worker protocol.

## Authoring

```rust
use daedalus::{PluginRegistry, declare_plugin, macros::node, runtime::NodeError};

#[node(id = "add", inputs("a", "b"), outputs("out"))]
fn add(a: i64, b: i64) -> Result<i64, NodeError> {
    Ok(a + b)
}

declare_plugin!(MathPlugin, "demo.math", [add]);

fn main() -> std::io::Result<()> {
    daedalus_ffi_rust::Worker::from_plugin(&MathPlugin::new())
        .map_err(std::io::Error::other)?
        .serve()
}
```

The plugin crate declares a `plugins` feature (the `declare_plugin!` expansion checks it) and
depends on the `daedalus` facade with `features = ["plugins"]`.

## What The Worker Does

- Says hello with the protocol version, `supported_nodes` and the `rust` backend; reads the host's
  ack; answers each `invoke` with a `response` (outputs, events) or a typed `error`.
- Builds the schema from the same registry export the dynamic-plugin path uses, so ports, type
  keys and feature flags match the in-process view.
- Runs node handlers through the runtime's handler registry, with the plugin's const coercers and
  value serializers.
- Keeps `state(T)` nodes' state in the worker process across invokes.
- Reports a node error as `node_error`, a panic as `node_panic` (the worker keeps serving), a
  request for an unknown node or bad arguments as `unknown_node` or `bad_request`.
- Exits cleanly when stdin ends.

Diagnostics go to stderr. Stdout carries protocol frames only.

## Events

`emit_event(level, message, metadata)` and `event_info`, `event_warn` and the other level helpers
record an event for the node that is running. Events return in the response, in emission order.

## Packaging

`rust_worker_package(schema, executable, args)` builds the `PluginPackage` the host installs: one
persistent-worker backend per node, `executable` as the worker binary.

## Not Yet

- Image payloads are not converted.
- Struct, enum, optional, list, map, and tuple wire values are not mapped to node ports.
- Custom type keys and boundary contracts are not mapped at the worker boundary.
- `WireValue` state is not imported or exported; stateful nodes keep their state in the worker.

## Conformance

`tests/conformance.rs` runs the canonical Rust fixtures from `daedalus-ffi-core` through a real
persistent worker: the host starts the test binary as the worker and checks outputs and events of
five fixtures against the expected responses. The rest are reported as not served.
