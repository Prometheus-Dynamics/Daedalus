# Node Authoring And Payload Residency

This is the authoritative guide for writing Daedalus 2.x nodes and for reasoning about what
actually moves between them at runtime. Downstream projects should link here instead of
restating runtime internals.

## Two Layers Of Typing

Daedalus separates two things that are easy to conflate:

1. **Graph/UI schema**: `daedalus::data::model::TypeExpr`. It describes what a port looks like to
   an editor, to JSON graph documents, and to planner compatibility checks.
2. **Runtime carrier**: `daedalus::transport::Payload`. Every edge carries a `Payload`, which
   holds an `Arc`-backed Rust value (or bytes) plus transport metadata:
   - `type_key()` – the stable `TypeKey` identifying the carried type,
   - `residency()` – `Cpu`, `Gpu`, `CpuAndGpu`, or `External` (memory owned outside Daedalus,
     for example a dmabuf-backed camera frame),
   - `layout()` and `bytes_estimate()` – optional layout/size hints for telemetry and adapters,
   - lineage and release metadata used by lifecycle telemetry.

A `TypeExpr` is never the carrier. It is the portable description of it.

## Declaring Types

- `#[type_key("domain:name")]` pins a stable transport key on a Rust type. Keys are part of the
  public contract: plans, telemetry, FFI fixtures, and graph documents depend on them.
- `#[derive(DaedalusTypeExpr)]` gives a struct/enum a stable, structured `TypeExpr` schema. Use
  `#[daedalus(type_key = "...")]` to pin its key; otherwise it defaults to
  `rust:<module_path>::<TypeName>`.
- `#[derive(ToValue)]` makes a type exportable as `daedalus::data::model::Value`, which is what
  hosts see when they inspect payloads.
- Types that cross graph boundaries (anything wired outside a local subgraph, edited in a UI, or
  stored in a graph document) should have a structured `TypeExpr`. `TypeExpr::opaque(..)` is for
  genuinely non-portable data; when you must use it, also register a value serializer or an
  adapter to an inspectable type so hosts can still debug it.

Register types, nodes, and adapters through a plugin (`#[plugin(...)]` or `declare_plugin!`) so
everything lands in the `PluginRegistry` the engine compiles from.

The `id` of `#[node]`, `#[adapt]` and `#[plugin]` is a string literal or any expression that
evaluates to a `&'static str` constant, so a library can prefix its ids in one place:

```rust
macro_rules! eidos_id {
    ($name:ident) => { concat!("eidos.", stringify!($name)) };
}

#[node(id = eidos_id!(blur), inputs("frame"), outputs("frame"))] // or id = concat!(..), id = BLUR_ID
fn blur(frame: &Frame) -> Result<Frame, NodeError> { /* ... */ }
```

### How Port Keys Resolve

`#[node]` and `#[adapt]` give every port (and adapter end) a `TypeKey`, in this order:

1. An explicit key: `inputs(port(name = "frame", type_key = "styx:framelease"))` (same for
   `outputs(port(...))`), `#[adapt(from = "...", to = "...")]`, or a full schema with
   `port(name = "...", ty = <TypeExpr>)`.
2. The key the type owns (`#[type_key]`, `#[derive(DaedalusTypeExpr)]`). It is resolved at compile
   time through the type's trait impl, so it never depends on which plugin installed first.
3. The installing registry's typing registry (`PluginRegistry::type_registry`):
   `#[plugin(foreign_types(Type = "key"))]` (see below), which registers the mapping before the
   plugin's nodes install, or `register_foreign_type` on that registry. Mappings made in another
   registry never apply, and nothing is read from process-global state.
4. Builtins (integers, floats, `bool`, `String`, `Vec<u8>`, `()`) and structural containers
   (`Vec<T>`, `Option<T>`, tuples). `&T`, `Arc<T>`, `Cpu<T>` and `Gpu<T>` use `T`'s key.
5. Otherwise the fallback `rust:<type path>`. Whether a mapping exists depends on what the
   registry installed first, so for a type defined in **another crate** the fallback is an error
   at install:
   `PluginError::UnkeyedForeignType` names the node (or adapter), the port and the Rust type,
   and lists the fixes. Types from the node's own crate and from `std` keep the fallback.

Handlers push outputs under the same key the port declares. The keys are resolved once, when the
plugin installs (`node_decl_in`, `boundary_contracts_in` and `handler_registry_in` take the
registry's `TypeRegistry`; `node_decl()` and `handler_registry()` resolve through
`TypeRegistry::empty()`, so they see owned keys and builtins only), and handlers keep their output
keys instead of rebuilding them on every push. Each port's Rust type is recorded per
key (`PluginRegistry::boundary_types()`); one key used for two different Rust types fails with
`PluginError::BoundaryTypeConflict` (two plugins, a plugin and the host, or a dynamic plugin built
separately).

Key registration is strict, so the outcome never depends on install order. Registering the same
type under the same key again (two plugins listing `types(Frame)`) is a no-op; the placeholder a
port leaves when it uses a key before the owner installs, and a built-in declaration, are replaced.
Anything else fails: a second key for one type (`PluginError::TypeKeyedTwice`), another Rust type
for a key (`BoundaryTypeConflict`), or another schema or export policy for a key
(`PluginError::TypeDeclarationConflict`).

#### Generic Pushes

Code that names a Rust type but no key, `HostGraph::push::<T>` / `bind_input::<T>` /
`bind_lane::<T>` / `run_once((port, value))`, `HostBridgeHandle::push`, `NodeIo::push_to` and
`GraphBuilder::input_typed::<T>` / `output_typed::<T>`, resolves `T` through the registry the graph
came from, never through process-global state. `PluginRegistry::type_index()` freezes it into a
`TypeIndex`: `registry.graph_builder()` captures one, compiling through a registry
(`compile_registry`, `compile_host_graph_plugin_registry`, `compile_plugin_registry`) hands one to
the host bridge and to every handler's `NodeIo`. A type resolves to:

1. its builtin structural key: integers, floats, `bool`, `String`, `Vec<u8>`, `()`, and
   `Option`/`Vec` of them, in every index;
2. the key it owns in that registry (`#[type_key]`/`DaedalusTypeExpr` types the registry
   installed, `foreign_types`/`register_foreign_type`, `registry.type_registry`);
3. else the one key the registry's ports and adapters use it under (a port `type_key`, or
   `register_boundary_type::<T>(key)` in the host).

So `push::<Frame>` works when the frame's owner plugin installed after the consumer, but not in a
graph whose registry never saw `Frame`, even if another registry in the process did. A type
without a key fails instead of getting an order-dependent `rust:` key: `push` returns
`FeedOutcome::Rejected(TypeKeyError::Unkeyed { .. })`, the binding and builder calls return the
error, and `push_to` returns a `NodeError`, all naming the fixes (install the owner plugin, map it
with `foreign_types`, give a port a `type_key`, or pass the key with `push_as`). A type used under
several keys and owning none is `TypeKeyError::Ambiguous`. Graphs and builders made without a
registry resolve builtins only; pass explicit keys there.

The host bridge also checks fed payloads (`push_payload`, `bind_payload_input`, `push_as`,
`feed_payload`, and the direct-lane entry points): a payload whose key the registry records for
another Rust type is refused with `FeedOutcome::Rejected(TypeKeyError::RustTypeMismatch)`,
``payload for `k` holds `A` but this graph expects `B` (built separately?)``, instead of failing
in a node later. Payloads under unknown keys, bytes and foreign handles pass; the check compares
one `u64` per feed and is skipped when the registry records no Rust types.

## Handler Inputs: Ask For The Type You Want

Write handlers against the type the algorithm needs. The macro generates the fetch code; the
planner and runtime make the value arrive in that form when a path exists.

| Parameter form | Meaning | Node `access` |
| --- | --- | --- |
| `&T` | shared read, no copy | `read` (default) |
| `Arc<T>` | shared handle to the same allocation | `read` |
| `T` | owned value; moved when the payload is unique, otherwise planned branch/copy | `move` |
| `&mut T` | in-place mutation when unique, planned copy-on-write otherwise | `modify` |
| `Cow<'_, T>` | borrow when possible, clone on write | `modify` |
| `Option<T>` / `Option<&T>` / `Option<Arc<T>>` | optional input, `None` when no value arrived (see [Optional Inputs](#optional-inputs-and-readiness)) | `read` |
| `FanIn<T>` | all values arriving on an indexed fan-in port | any |
| `Cpu<T>` / `Gpu<T>` | explicitly request a device residency | any |
| `FrameView<'_>` / `ForeignRef<'_, I>` | a host-owned value through a foreign interface, no copy (see [Foreign Interfaces](#foreign-interfaces)) | `read` |

Use `access = "move"` or `access = "modify"` only when the node truly consumes or mutates its
input. Read access lets fanout share one allocation.

### Optional Inputs And Readiness

Every tick, the runtime runs each node once with whatever arrived on its input edges that tick
(plus const inputs). The rule:

- A node runs only when **each connected required input has a value**. Otherwise it is skipped
  for that tick: no error, no outputs, and the values that did arrive on its other ports are
  dropped (as before, every tick consumes what arrived). Downstream nodes then see nothing either.
- **Optional inputs never block.** A parameter `Option<T>` (or `Option<&T>`, which borrows,
  or `Option<Arc<T>>`) is `None` when the port is unconnected or nothing arrived this tick, and
  `Some` otherwise.
- A required input that is **not connected** (and has no const value) still fails the node with
  `NodeError::InvalidInput("missing <port>")`: that is a wiring error, not a timing one.
- Nodes that take `NodeIo` read their inputs themselves, so their declared ports are optional.

The port of an `Option<T>` parameter has `T`'s key and schema, so producers of `T` (and typed
host inputs of `T`) connect to it directly, without an adapter; `PortDecl::optional` marks it
(and `WirePort::optional` in exported schemas, and the planner does not warn about it being
unconnected). `Option<T>` clones the value; use `Option<&T>` to borrow it.

A node returning `Result<Option<T>, _>` (or a tuple element `Option<T>`) has a **conditional
output** of type `T`: `Some` pushes, `None` pushes nothing.

```rust
#[node(id = "detect", inputs("frame"), outputs("corners"))]
fn detect(frame: &Frame) -> Result<Option<Corners>, NodeError> { /* None: nothing found */ }

#[node(id = "refine", inputs("frame", "corners"), outputs("corners"))]
fn refine(frame: &Frame, corners: &Corners) -> Result<Corners, NodeError> { /* ... */ }

// Runs every frame: with refined corners when detect and refine produced them, else `None`.
#[node(id = "pose", inputs("frame", "refined"), outputs("pose"))]
fn pose(frame: &Frame, refined: Option<&Corners>) -> Result<Pose, NodeError> { /* ... */ }
```

The planner records each node's required inputs in its metadata
(`NODE_REQUIRED_INPUTS_META_KEY`); nodes without a registry declaration are never gated.

### Constants, Defaults And Config Enums

Port defaults (`port(name = "mode", default = "wrap")`, `#[port(default = ...)]` on a
`#[derive(NodeConfig)]` field) and graph constants (`const_input`, graph documents) arrive as a
`Value`. A handler parameter or config field of another type gets it converted when the node
runs (`T`, `&T`, `&mut T`, `Option<T>`, and config fields):

- builtins (integers, floats, `bool`, `String`, `Vec<u8>`) convert directly;
- enums deriving `DaedalusTypeExpr` whose variants are all unit variants take a variant name
  (`Value::String` or `Value::Enum`, case-insensitive) or an index (`Value::Int`), with no serde
  dependency (`DaedalusTypeExpr::from_value`);
- other types implementing `serde::Deserialize` deserialize from the value (structs from
  `Value::Struct`/`Map`, enums externally tagged: `Value::Enum { name, value }`).

The node macros register these conversions for every input and config field type when the plugin
installs, so no `register_enum` or `register_const_coercer` call is needed. A conversion
registered explicitly with `PluginRegistry::register_const_coercer` (or `register_enum`) takes
precedence, whichever installs first. Values that arrive typed (an upstream node producing the
enum) are used as they are.

```rust
#[derive(Clone, Copy, Debug, daedalus::DaedalusTypeExpr)]
#[daedalus(type_key = "eidos:border")]
enum Border { Reflect, Constant, Wrap }

#[derive(Clone, Debug, NodeConfig)]
struct BlurConfig {
    #[port(default = "reflect")]
    border: Border,
    #[port(default = 3, min = 1)]
    radius: i32,
}

#[node(id = "blur", inputs("frame", config = BlurConfig), outputs("frame"))]
fn blur(frame: &Frame, cfg: BlurConfig) -> Result<Frame, NodeError> { /* ... */ }
```

## Adapters Replace Conversion Nodes

When a producer's type or residency differs from what a consumer asks for, the planner resolves
an **adapter path** and inserts it on the edge. Do not write conversion-only nodes
(`to_cpu_*`, `to_gray`, `frame_to_image`, ...); declare the conversion once:

```rust
#[adapt(id = "example.count_to_label", kind = daedalus::transport::AdapterKind::Materialize)]
fn count_to_label(value: &Count) -> Result<CountLabel, TransportError> {
    Ok(CountLabel(format!("count={}", value.0)))
}
```

`#[adapt]` accepts `id`, `kind`, `access`, `cost`, `residency`, `layout`, `requires_gpu`, and
`feature`. The adapter `kind` tells the planner what the step costs semantically:

- `Identity`, `View`, `SharedView`, `CowView`, `MetadataOnly` – no data copy.
- `Materialize`, `Cow`, `Branch` – produce a new value.
- `Reinterpret`, `MutateInPlace`, `DeviceTransfer`, `DeviceUpload`, `DeviceDownload`,
  `Serialize`, `Deserialize`, `Custom` – conversions.

The planner picks the cheapest path by declared cost and records it in the plan. Use
`RuntimePlan::explain()` / `HostGraph::explain_plan()` to see the chosen `adapter_path` and the
handoff reason for each edge.

## CPU/GPU Residency

- `#[device(id = ..., cpu = "k", device = "k@gpu", download = f)]` on an upload function declares
  a device pair: an upload adapter (CPU → device) and a download adapter (device → CPU).
- A node asking for `Gpu<T>` receives device-resident data; a node asking for `T` or `Cpu<T>`
  receives CPU data. The planner inserts `DeviceUpload`/`DeviceDownload` adapters as needed.
- `#[node(fallback = "cpu.node.id")]` lets a GPU node fall back to a CPU implementation when no
  GPU backend is available.
- A payload can carry **cached residents**: `Payload::with_cached_resident` /
  `insert_cached_resident` attach another residency of the same value (for example the CPU copy
  of a GPU frame). Adapters and consumers reuse a cached resident instead of transferring again,
  and clones of the payload share the cache, so fanout to several CPU consumers does not
  multiply downloads.
- The avoidable cost is a round trip caused by mismatched affinities (CPU → GPU → CPU). Group GPU
  stages together and check the plan explanation for `DeviceUpload`/`DeviceDownload` steps.

`Residency::External` marks memory owned outside Daedalus (camera buffers, dmabuf). The payload
still travels by `Arc`; the runtime never copies it unless an adapter path requires a CPU or GPU
materialization.

## Integrating An External Frame Source

Daedalus does not depend on any camera or media library. When the library that defines the frame
type ships an optional `daedalus` integration feature (see
[Library-Owned Integration Features](#library-owned-integration-features)), enable it and skip
steps 1–3. Otherwise the integration with a frame source (a camera stack, a decoder, a
compositor) belongs in a small glue module in the application that uses both, or in a standalone
bridge crate if several applications share it.

The glue is small and always has the same shape:

1. **Pick a stable key.** One `pub const FRAME_TYPE_KEY: &str = "vendor:frame";` used for every
   payload and as the frame type's key (`#[type_key(FRAME_TYPE_KEY)]`). Treat it as a public
   contract.
2. **Describe the frame.** Define a plain descriptor struct (width, height, pixel format,
   timestamp, planes with stride/length, residency) with
   `#[derive(DaedalusTypeExpr, DaedalusToValue)]`. That struct is the graph/UI schema, so editors
   and graph documents see a structured type instead of `opaque`. The derives only need the
   `daedalus-rs` dependency.
3. **Register once.** In the plugin (not per frame): `#[plugin(types(Frame), values(FrameMeta),
   adapters(...), install = ...)]` registers the frame type, the descriptor (nested descriptor
   types such as a plane struct are registered automatically) and the adapters, such as a
   `MetadataOnly` adapter from the frame to its descriptor or a `View` adapter to a CPU image view
   for already-CPU frames. The install hook adds a value serializer that turns a frame into its
   descriptor (`register_value_serializer::<Frame, _>(|frame| frame.meta().to_value())`; it only
   borrows the frame), so host payload inspection shows structured data.
4. **Wrap without copying.** Build payloads with
   `Payload::shared_with(FRAME_TYPE_KEY, Arc::new(frame), residency, layout, bytes)`, mapping
   the source's buffer kind to `Residency`: host memory → `Cpu`, externally owned buffers such as
   dmabuf → `External`, GPU textures → `Gpu`.
5. **Feed the host bridge** through a typed input, `graph_builder.input_typed::<Frame>("frame")?`
   (resolved through the registry, see [Generic Pushes](#generic-pushes)), with `push_payload` on
   a latest-only input so stale frames are replaced rather than queued.

Nodes then take the frame type (or a view type reachable through adapters) directly, and the
planner handles the rest. Host ports are generic unless declared: an undeclared host input takes
its type from the node ports it feeds, so it cannot feed a frame port and a descriptor port at
once. A declared one (`input_typed::<T>` / `input_as(name, TypeExpr)`, and `output_typed` /
`output_as` for outputs) keeps its type, and the planner adapts each edge separately.

[`examples/04_async/external_frame_source.rs`](../examples/04_async/external_frame_source.rs)
is a copyable template of all five steps with a synthetic source instead of a camera:
`cargo run -p daedalus-examples --bin external_frame_source`.

For dmabuf frames that GPU nodes consume, the glue can skip the CPU entirely: with the
`gpu-dmabuf` feature, `GpuContextHandle::import_dmabuf` turns the buffer (fd, offset, stride, DRM
fourcc/modifier) into a GPU image that aliases the producer's memory, holding a keepalive (the
producer's buffer lease) until the GPU is done. Check `supports_dmabuf_import()` first and fall
back to a CPU upload otherwise. See "Importing external frames (dmabuf)" in
[`crates/gpu/README.md`](../crates/gpu/README.md).

## Library-Owned Integration Features

When several crates exchange one type through Daedalus (a camera library's `FrameLease`, consumed
by an image-processing library's nodes and by an application's plugins), **the crate that
defines the type owns its type key and its Daedalus registration**, behind an optional `daedalus`
feature. Daedalus never depends on that crate; the crate optionally depends on Daedalus.

The owning crate (here `styx-core`):

```toml
[features]
daedalus = ["dep:daedalus"]

[dependencies]
daedalus = { package = "daedalus-rs", version = "2", optional = true, features = ["plugins"] }
```

```rust
// src/frame.rs: the key is part of the type, so every consumer resolves it the same way.
#[cfg_attr(feature = "daedalus", daedalus::type_key("styx:framelease"))]
pub struct FrameLease { /* ... */ }

#[cfg_attr(feature = "daedalus", derive(daedalus::DaedalusTypeExpr, daedalus::DaedalusToValue))]
#[cfg_attr(feature = "daedalus", daedalus(type_key = "styx:frame_meta"))]
pub struct FrameMeta { pub width: u32, pub height: u32, /* format, planes, ... */ }

// src/lib.rs
#[cfg(feature = "daedalus")]
pub mod daedalus_integration;

// src/daedalus_integration.rs: everything else Daedalus needs to know about the type, once.
use daedalus::data::to_value::ToValue;
use daedalus::runtime::plugins::{PluginInstallContext, PluginResult};

pub const FRAME_LEASE_KEY: &str = "styx:framelease";

#[daedalus::plugin(
    id = "styx",
    types(crate::FrameLease),
    values(crate::FrameMeta),
    adapters(lease_to_meta),
    // `FrameLease: FrameSource` (below): separately built plugins read leases as `FrameView`.
    foreign_providers(crate::FrameLease => daedalus::transport::FrameInterface),
    install = install
)]
pub struct StyxPlugin;

#[daedalus::adapt(id = "styx.lease_to_meta", kind = daedalus::transport::AdapterKind::MetadataOnly)]
fn lease_to_meta(
    lease: &crate::FrameLease,
) -> Result<crate::FrameMeta, daedalus::transport::TransportError> {
    Ok(lease.meta())
}

fn install(registry: &mut PluginInstallContext<'_>) -> PluginResult<()> {
    registry.register_value_serializer::<crate::FrameLease, _>(|lease| lease.meta().to_value());
    Ok(())
}

// The `daedalus:frame` v1 accessors (docs/foreign-frame-interface.md).
impl daedalus::transport::FrameSource for crate::FrameLease { /* width, height, planes, ... */ }
```

Every other crate enables that feature instead of registering the type again:

```toml
# eidos (ships its ops as nodes), or an application's plugin crate
styx-core = { version = "...", features = ["daedalus"] }
```

```rust
#[node(id = "to_gray", inputs("frame"), outputs("gray"))]
fn to_gray(frame: &styx_core::FrameLease) -> Result<Gray8, NodeError> { /* ... */ }

// `deps` makes the requirement explicit: freezing a registry without the styx plugin fails with a
// missing-dependency error. The install order of the two plugins does not matter.
#[plugin(id = "eidos", deps("styx"), nodes(to_gray))]
pub struct EidosPlugin;
```

The port key of `to_gray`'s `frame` is `styx:framelease` because `FrameLease` owns it, even when
`EidosPlugin` installs before `StyxPlugin`. A dynamic plugin also links the dependency,
`export_plugin!(EidosPlugin, deps [StyxPlugin])` (see
[Plugin Dependencies](dynamic-plugins.md#plugin-dependencies)). The host installs `StyxPlugin` and wraps frames with
`Payload::shared_with(styx_core::daedalus_integration::FRAME_LEASE_KEY, Arc::new(lease), ...)`.

Rules:

- **Never mint a second key for someone else's type.** Two keys for one type split the graph into
  incompatible halves; two types under one key fail with `BoundaryTypeConflict` (or worse, at
  runtime). Enable the owner's `daedalus` feature.
- **When the owner has no integration yet**, declare a key where you use the type:
  `#[plugin(foreign_types(styx_core::FrameLease = "styx:framelease"))]` registers the mapping
  into the plugin's registry before its nodes install, and
  `inputs(port(name = "frame", type_key = "styx:framelease"))` sets it for a single port. Pick the
  key the owner would use and move to the owner's feature once it exists. A type from another
  crate with no key at all fails install (`UnkeyedForeignType`) instead of silently getting an
  order-dependent `rust:` key.
- **Register host-side types too.** A host that wraps a type in payloads should install the
  owner's plugin (or call `registry.register_boundary_type::<T>(key)`), so the registry records
  which Rust type the key carries. Dynamic plugins and fed payloads are checked against it, and
  `push::<T>` resolves the key (see [Generic Pushes](#generic-pushes) and
  [`docs/dynamic-plugins.md`](dynamic-plugins.md#types-owned-by-other-crates)).
- **Provide `daedalus:frame` for frame types.** A frame owner implements `FrameSource` and
  registers the provider (`foreign_providers(...)` above), so nodes that only need pixels and
  metadata take `FrameView<'_>` and work with any frame library and in plugins built separately
  from it. Nodes that need the library's own API keep taking `&FrameLease`.

## Foreign Interfaces

A Rust type is only the same type in two binaries when Cargo built its crate identically for
both. A plugin built in its own cargo invocation can resolve a shared crate (say `styx-core`)
with other features, so its `FrameLease` differs from the host's under the same key; install
refuses such a plugin (`BoundaryTypeConflict`). A **foreign interface** lets it consume the
host's values anyway, zero-copy, without sharing the Rust type:

- **Interface**: a `#[repr(C)]` vtable of `extern "C"` accessors with a key, a version and a
  layout hash of its declaration, declared with `daedalus::transport::foreign_interface!`.
  Daedalus ships `daedalus:frame` v1 ([spec](foreign-frame-interface.md)); libraries can declare
  their own.
- **Provider**: the owner implements `ProvideForeign<I>` for its type (for frames, the safe
  `FrameSource` trait) and registers it once with `#[plugin(foreign_providers(Owner =>
  Interface))]` or `registry.register_foreign_provider::<Owner, Interface>()`. That registers a
  `View` adapter (`daedalus.foreign:<owner key>-><interface key>`, cost of a view).
- **Consumer**: a node takes `FrameView<'_>` (= `ForeignRef<'_, FrameInterface>`) or
  `ForeignRef<'_, I>`. The port's key is the interface key, its access is `read`, and it records
  no Rust boundary type. The planner inserts the provider's adapter on the edge, which wraps the
  producer's `Arc` in a `ForeignHandle` (data pointer, vtable, interface identity and a
  reference-counted keepalive; one `Arc` increment, no copy) carried by a payload under the
  interface key. The node checks the handle against its own copy of the interface (key, version
  and layout hash) and reads through the vtable.

```rust
use daedalus::transport::{FrameView, ForeignRef};

#[node(id = "frame_size", inputs("frame"), outputs("pixels"))]
fn frame_size(frame: FrameView<'_>) -> Result<u64, NodeError> {
    Ok(u64::from(frame.width()) * u64::from(frame.height()))
}

// A library-specific interface (declared by the owner with `foreign_interface!`).
#[node(id = "read_counter", inputs("counter"), outputs("value"))]
fn read_counter(counter: ForeignRef<'_, CounterInterface>) -> Result<i32, NodeError> {
    Ok(counter.value()) // an extension trait over the vtable, written by the interface's owner
}
```

Rules and limits:

- The macros recognize the parameter by name: spell it `FrameView<'_>` or `ForeignRef<'_, I>`
  (a by-value parameter, not `Option`/`&`). Other aliases are not recognized.
- The host input or producer must carry the owner type (declare host inputs with
  `input_as(name, TypeExpr::opaque(owner_key))` or `input_typed::<Owner>`), and the registry needs
  the owner's provider; otherwise planning reports a missing converter.
- One version per interface key per registry (`PluginError::ForeignInterfaceConflict`); dynamic
  plugins export the interfaces they use and `install_into` refuses mismatches
  (`PluginLibraryError::ForeignInterfaceMismatch`).
- Interfaces are input-only: a node that produces frames returns its own type, which consumers
  read through the interface again.
- `foreign_interface!` hashes the field names and types as written (whitespace ignored), size and
  alignment. Keep declarations stable and bump the version for any change.
- Accessors run the owner's code through `extern "C"` functions: they must not panic (that
  aborts), and the owner keeps shared values immutable.

[`crates/daedalus/tests/foreign_interfaces.rs`](../crates/daedalus/tests/foreign_interfaces.rs)
shows a host frame type with a provider feeding `FrameView` and `ForeignRef` nodes, and
[`examples/plugins/foreign_consumer`](../examples/plugins/foreign_consumer/src/lib.rs) a plugin
built separately from the type it reads.

## Nodes And Profiling

Prefer several small nodes (or an embedded graph/node-group) over a single "mega node" that
performs several stages. Per-node telemetry (`MetricsLevel::Detailed` and above) is only
actionable when stages are separate nodes.

## Hosts

Host applications exchange payloads through the host bridge (`HostGraph::push*`, `take*`,
`latest`, `subscribe`). Configure latest-only policies (`set_latest_input` /
`set_latest_output`) for live streams such as camera frames so stale values are replaced rather
than queued.

- **Discover ports** with `HostGraph::host_inputs()` / `host_outputs()`. Each
  `HostPortDescriptor` carries the port name, its `TypeExpr` and `TypeKey` when known, and the
  graph nodes it connects to.
- **Drive on input, not on a timer.** `HostGraph::drive_blocking(&stop, on_outputs)` (or the
  runtime-agnostic `async fn drive`) waits for inbound payloads, ticks, and calls `on_outputs`
  after each turn; `HostGraphStopHandle::stop()` ends it. For custom loops use
  `wait_for_input(timeout)` / `tick_on_input(timeout)`, or `HostBridgeHandle::inbound_waiter()`,
  which is both a blocking waiter and a `Future`.
- **Async hosts (tokio):** `async fn drive` waits without blocking, but each graph tick runs
  inline on the polling task. For anything CPU-heavy, move the `HostGraph` into
  `tokio::task::spawn_blocking(move || graph.drive_blocking(&stop, on_outputs))`, keep a cloned
  `HostGraphStopHandle` on the async side, and call `stop()` to end the loop (it wakes the
  waiter). Feed inputs from async tasks through a cloned `HostBridgeHandle` (`graph.host()`).
  See the `drive` module docs in `daedalus-engine` for a full example.
- **Typed feeds resolve through the graph's registry** (`push::<T>`, `bind_input::<T>`; see
  [Generic Pushes](#generic-pushes)); a feed the bridge refuses returns `FeedOutcome::Rejected`.
- **Port arguments:** write paths (`push*`, `set_*_policy`, `bind_input`/`bind_output`,
  `subscribe`) take `impl Into<PortId>`; read paths (`take*`, `drain*`, `latest`) take
  `impl AsRef<str>`. Build `PortId`s once (or use `bind_input`) in hot loops so pushes do not
  allocate.
- **Host bridge events are off by default.** Enable `with_host_event_recording(true)` (or
  `HostBridgeHandle::set_event_recording(true)`) when debugging dropped or missing payloads.
- **Inspect outputs** with `HostGraph::inspect_payload(&payload)`, or take and inspect every
  queued output at once with `HostGraph::inspect_outputs()`. Inspection uses the value serializers
  registered in the plugin registry and falls back to a `PayloadSummary` (type key, Rust type,
  residency, size) for types without one; `to_json()` renders either as plain JSON.
- **Per-port counters**: `HostBridgeHandle::input_port_stats(port)` and `output_port_stats(port)`
  report accepted, replaced, dropped, delivered, and pending counts for one port.
- **Persist graphs** as `GraphDocument`s (`format: "daedalus.graph"`, `schema_version`,
  `requires`, `metadata`, `graph`). `Engine::compile_document*` checks `requires` against the
  installed plugins before planning, and `PluginRegistry::graph_document(graph)` fills
  `requires` from the plugins that provide the graph's nodes. Documents are strict:
  `GraphDocument::from_json` rejects unknown fields at every level (only the `metadata` maps are
  free-form) and reports the JSON path of the offending field. Editors can validate against
  [`docs/schema/daedalus.graph.v1.schema.json`](schema/daedalus.graph.v1.schema.json), generated
  by `GraphDocument::json_schema()` (planner `schema` feature).

## Migrating From Pre-2.0 Names

| Pre-2.0 | 2.0 |
| --- | --- |
| `EdgePayload::{Any, Payload, GpuImage, Value}` | `transport::Payload` with `type_key()` and `residency()` |
| `ErasedPayload` memoized transfers | cached residents on `Payload` (`with_cached_resident`) |
| `GpuSendable::{upload, download}` | `#[device(...)]` upload/download adapters |
| `ConversionRegistry` | `#[adapt(...)]` adapters resolved by the planner |
| `NodeIo::get_payload::<T>()` / `Payload<T>` multi-modal input | `Cpu<T>` / `Gpu<T>` parameters plus `fallback` |
| `ComputeAffinity` on the node | still present; residency is now driven by parameter types and adapters |
