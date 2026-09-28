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
| `Option<T>` | optional input | any |
| `FanIn<T>` | all values arriving on an indexed fan-in port | any |
| `Cpu<T>` / `Gpu<T>` | explicitly request a device residency | any |

Use `access = "move"` or `access = "modify"` only when the node truly consumes or mutates its
input. Read access lets fanout share one allocation.

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

Daedalus does not depend on any camera or media library. Integration with a frame source (a
camera stack, a decoder, a compositor) belongs in a small glue module in the application that
uses both, or in a standalone bridge crate if several applications share it. Neither library
should depend on the other.

The glue is small and always has the same shape:

1. **Pick a stable key.** One `pub const FRAME_TYPE_KEY: &str = "vendor:frame";` used for every
   payload. Treat it as a public contract.
2. **Describe the frame.** Define a plain descriptor struct (width, height, pixel format,
   timestamp, planes with stride/length, residency) with `#[derive(DaedalusTypeExpr, ToValue)]`.
   That struct is the graph/UI schema, so editors and graph documents see a structured type
   instead of `opaque`.
3. **Register once.** In a plugin install function (not per frame), register the frame type, a
   value serializer that turns a frame into its descriptor (so host payload inspection shows
   structured data), and any adapters, such as a `MetadataOnly` adapter from the frame to its
   descriptor or a `View` adapter to a CPU image view for already-CPU frames.
4. **Wrap without copying.** Build payloads with
   `Payload::shared_with(FRAME_TYPE_KEY, Arc::new(frame), residency, layout, bytes)`, mapping
   the source's buffer kind to `Residency`: host memory → `Cpu`, externally owned buffers such as
   dmabuf → `External`, GPU textures → `Gpu`.
5. **Feed the host bridge** with `push_payload` on a latest-only input so stale frames are
   replaced rather than queued.

Nodes then take the frame type (or a view type reachable through adapters) directly, and the
planner handles the rest.

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
- **Port arguments:** write paths (`push*`, `set_*_policy`, `bind_input`/`bind_output`,
  `subscribe`) take `impl Into<PortId>`; read paths (`take*`, `drain*`, `latest`) take
  `impl AsRef<str>`. Build `PortId`s once (or use `bind_input`) in hot loops so pushes do not
  allocate.
- **Host bridge events are off by default.** Enable `with_host_event_recording(true)` (or
  `HostBridgeHandle::set_event_recording(true)`) when debugging dropped or missing payloads.
- **Inspect outputs** with `HostGraph::inspect_payload(&payload)`. It uses the value serializers
  registered in the plugin registry and falls back to a `PayloadSummary` (type key, Rust type,
  residency, size) for types without one; `to_json()` renders either as plain JSON.
- **Persist graphs** as `GraphDocument`s (`format: "daedalus.graph"`, `schema_version`,
  `requires`, `metadata`, `graph`). `Engine::compile_document*` checks `requires` against the
  installed plugins before planning, and `PluginRegistry::graph_document(graph)` fills
  `requires` from the plugins that provide the graph's nodes.

## Migrating From Pre-2.0 Names

| Pre-2.0 | 2.0 |
| --- | --- |
| `EdgePayload::{Any, Payload, GpuImage, Value}` | `transport::Payload` with `type_key()` and `residency()` |
| `ErasedPayload` memoized transfers | cached residents on `Payload` (`with_cached_resident`) |
| `GpuSendable::{upload, download}` | `#[device(...)]` upload/download adapters |
| `ConversionRegistry` | `#[adapt(...)]` adapters resolved by the planner |
| `NodeIo::get_payload::<T>()` / `Payload<T>` multi-modal input | `Cpu<T>` / `Gpu<T>` parameters plus `fallback` |
| `ComputeAffinity` on the node | still present; residency is now driven by parameter types and adapters |
