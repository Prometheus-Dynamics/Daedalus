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

## Wire Mapping

| Wire value | Rust port type |
| --- | --- |
| `Int`, `UInt`, `Float`, `Bool`, `String`, `Unit` | `i64`, `u64`, `f64`, `bool`, `String`, `()` and the other scalars |
| `Bytes` | `Vec<u8>` (a `Value::Bytes`, coerced like any value) |
| `List`, `Record`, `Enum` | `Vec<T>`, `BTreeMap<String, T>`, tuples, `Option<T>` and `#[derive(DaedalusTypeExpr, DaedalusToValue)]` structs and enums; decode with `register_const_coercer` and encode with `register_to_value_serializer` |
| `Image` | `daedalus_ffi_core::ImagePayload` |
| `Handle` | `daedalus_ffi_core::WirePayloadHandle`: the worker reads its metadata (capabilities, access, residency) and passes it on; it does not resolve the payload |

Custom type keys go on the port (`port(name = "...", type_key = "...")`) and on the type
(`#[daedalus(type_key = "...")]`). Boundary contracts are registered with
`register_boundary_contract`, with the layout hash `LayoutHash::for_schema` over the port's type
expression, as the macros derive it.

## State

A `state(T)` node keeps its state in the worker across invokes. A node whose state type implements
`ToValue` declares the wire form with `Worker::with_state(node_id, decode)`, and a node with config
ports or `&T` inputs with `Worker::with_state_slots::<S>(node_id, decode)`. `S` is the node's stored
state tuple, `(T, cache...)`: `T` first, then one `ConfigCache<C>` per config port and one
`DecodedInputs` per decoded `&T` input, in the order the macros store them. The host's state is
seeded before the call, the cache slots are defaulted (they are rebuilt from the request), and the
state is exported in the response. A declared `S` that does not match the node's stored tuple
silently resets the state, so declare exactly the slots the macros generate.

## Limits

- Shader nodes run their CPU form in the worker; no GPU dispatch and no package artifact is loaded.

## Conformance

`tests/conformance.rs` runs all 18 canonical fixtures from `daedalus-ffi-core` through a persistent
worker: the host starts the test binary as the worker, and each fixture's node is invoked twice,
checking outputs, events and state against the fixture's expected response. The image, shader and
package-artifact fixtures declare another backend in core; the run re-targets their schema to the
Rust backend, which is what a Rust-served node declares.
