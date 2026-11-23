# From dev to next

One table: each crate on `dev`, and what replaced it on `next`. It is for reading `dev` beside
`next`, not a guide to using either. [`testing/capability-audit.md`](../testing/capability-audit.md)
has the same thing by capability, one row each, and is checked by the gate.

`next` is not `dev` with changes. It was built from nothing against [design.md](design.md), and
`dev` was only read. Nothing on `next` keeps a `dev` API for compatibility.

| On `dev` | On `next` | PRs |
|---|---|---|
| `crates/core` | `core/types` (type keys, ids, errors), `core/graph` (the graph model and its JSON) | A4, A6 |
| `crates/data` | `core/value` (a value is a type key, a memory space and an owner) | A5, A14 |
| `crates/registry` | `core/registry` (one entry a node id, several bodies) | A8 |
| `crates/macros` | `core/macros` (`#[node]`, `#[transfer]`), `core/derive` (`NodeConfig`, `Type`) | A9, A13, A15 |
| `crates/planner` | `core/planner`, and `core/plan` for the stored step list | B1 to B15, A10 |
| `crates/runtime` | `core/runner` (walks the step list, no heap), `core/host` (feed, step, take, reload, telemetry) | C1 to C24, with no C19 |
| `crates/engine` | `core/host`: `LoadedHost`, with the planner called directly | C10 |
| `crates/daedalus` | `core/daedalus`, the facade | C14 |
| `crates/host-wrapper` | `core/host`, and the C surface that is planned | C10, G10, G11 |
| `crates/transport` | Gone. Values are `core/value`, and values between processes are `core/wire` | A5, G1 |
| `crates/gpu` | `backends/wgpu`, `backends/wgpu-author`, `backends/vk-sys`, `backends/dma-sys` | E1 to E15 |
| `crates/wgsl-infer` | Inside `backends/wgpu-author` | E4 |
| `crates/accelerator` | Gone. The `Backend` trait in `core/backend` is the interface | A7, I1 |
| `crates/accel`, `crates/kernels` | Gone. Nothing in Daedalus calls a byte kernel: a node that needs one brings it | |
| `crates/mcu` | The CPU backend's blob, source and chip parts in `backends/cpu` | D3, D4, D5, D6 |
| `crates/ffi` | `core/abi` (the step interface), `core/loader` (dylib plugins), `backends/process` and `languages/*` (workers) | A3, G2 to G9, G12 |
| `crates/surface` | Planned as `core/capi` | G10 |
| `crates/nodes` | Gone. Node groups are in the planner, and the examples replace the demo nodes | B1 |
| `crates/daemon` | `core/wire` (the service protocol), `core/client`, `core/host` (the listener), `service/daedalusd`, `service/daedalus-ctl` | H1 to H6 |
| `crates/frame-bench` | `testing/crates/budget-harness` | I2, I3 |
| `crates/bench-counters` | Gone from CI. Instruction counts are taken with callgrind | I2 |

## What has no successor

- A frame type, a frame library, colorimetry and pixel conversions. A type belongs to whoever
  wrote the nodes that use it.
- A CUDA backend.
- Presets. Settings that were environment variables are in the daemon's config file
  (`/etc/daedalusd/daedalusd.toml`, or the file `--config` names). The environment still sets one
  thing: the socket's directory, from `RUNTIME_DIRECTORY` (set by systemd), then `XDG_RUNTIME_DIR`.
- The Rust-ABI plugin path, plugin fingerprints, and every versioned plugin handshake. A plugin
  describes its layout a shape at a time and the host checks it fits: see
  [plugins.md](plugins.md#the-check-at-load).
  What to do instead is under [Plugins that were built with their host](#plugins-that-were-built-with-their-host).
- JSON and base64 on the worker path, and the payload handles and leases that went with them.
- The separate MCU macro and node types. A chip uses the same `#[node]`.

## Feeding a Rust value

`feed_typed(port, Arc<T>)` needs `T: Typed`, and checks it against the port. A port's type has a
key, so the value fed to it has one too: `#[derive(Type)]` with a `#[type_key("...")]`, or a
hand-written `Typed` impl for a type the derive cannot read, as
[foreign-values.md](foreign-values.md#a-type-of-your-own) shows for a struct that holds a file
descriptor. A value of another type than the port's is refused with `HostError::FedType`,
which names the port, the type it takes and the type that was fed. The same bound is on
`WireCodec::typed`, on a domain's `push_typed`, and on a sync group's host sink.

## Plugins that were built with their host

On `dev` a host could load a dylib that was built in the same cargo run as itself
(`PluginLibrary::load`, then `install_into(registry)`, which reported `InstallPath::RustAbi`). The
plugin's `#[node]` functions arrived as they were declared: every port's label, unit and default,
the config struct, the fire rule. That path is gone, and nothing on `next` does the same thing
through a dylib.

A consumer that did this links the crate and registers its nodes:

```rust
let mut registry = Registry::new();
registry.register_all(&my_nodes::NODES)?; // the `<NAME>_NODE` records `#[node]` makes
```

Everything a declaration carries arrives, because nothing crosses a boundary. What is given up is
swapping the plugin's file without rebuilding the host, which the same-build rule on `dev` did
not allow either.

A dylib on `next` is a different thing ([plugins.md](plugins.md)). It is loaded with
`Loader::new().open(path)` and joined with `plugin.join(&mut registry)`, and it is written with
`daedalus_loader::export_plugin!(PluginDef { .. })`, which has no `deps[..]`. What its nodes can
and cannot say:

| A dylib node declares | A dylib node cannot declare |
|---|---|
| Its id and source | A summary or description |
| Each port's name, type key and whether it is optional | A port's label, unit, group, description, default, limits or step |
| The backend it runs on, which the plugin itself provides | A config struct, fan-in ports, state, a fire rule, metrics, a group |
| A `run` function over the step's cells | A `#[node]` function run by the host's CPU backend |

So a dylib suits a backend or a codec shipped apart from the host (an NPU runtime, say), and
nodes written in Rust for the CPU belong in a linked crate.

## A host's calls, from dev to next

For a consumer that drove graphs through `dev`'s engine. The left column is the `dev` name as a
consumer wrote it; nothing on `next` keeps it.

| On `dev` | On `next` |
|---|---|
| `Engine`, `EngineConfig` | No engine object. `plan(&graph, &registry, &backends, &Config)` gives a plan, and `LoadedHost::load(planned, LoadOptions)` loads it |
| `EngineConfig` metrics level | `Telemetry::new(Level)` for counters and per-step rows; `LoadOptions::overhead_window` for per-step time and bytes; `LoadOptions::node_metrics` for what nodes report |
| `EngineConfig` frame overhead, `HostGraph::frame_overhead` | `LoadOptions::overhead_window`, read with `loaded.tick_overhead()` |
| `planner.enable_gpu` | Whether the GPU backend is in the list of backends given to `plan` |
| `runtime.mode` Serial | `loaded.step()` runs one tick on the caller's thread, which is the only mode a loaded host has |
| backpressure settings | Each edge's policy in the graph (`fifo`, `latest_only`, `bounded`, `per_tick`), and `set_port_policy` for a host input at run time |
| `compile_document_host_graph` | `GraphDoc::parse(text)`, then `plan` on its graph, then `LoadedHost::load` |
| `HostGraph::explain_plan` | `daedalus_planner::explain::explain_plan(&planned)` |
| `HostGraph::host_inputs`, `host_outputs` | `loaded.host().ports()`, each with its direction and type key; `port_infos()` adds policy, held and queued |
| `HostGraph::set_latest_input` | A `latest_only` policy on the edge from the host bridge; a feed then replaces what is queued |
| `HostGraph::set_held_input` | The host bridge marks the input `held` (or `shared`) in the graph; a feed replaces its value and wakes nothing |
| `HostGraph::tick` | `loaded.step()` |
| `HostGraph::tick_ready`, `run_executor_once` | `loaded.start()` gives a `StreamGraph` with `tick_ready()`, `tick_until_idle()` and `drive_blocking(..)`; `step_until_settled(timeout)` on the loaded host waits out work still on a device |
| `HostGraph::stop_handle` | `StreamGraph::stop_handle()`; `loaded.cancel_handle()` asks a running tick to stop |
| `HostGraph::inbound_fd` | `StreamGraph::inbound_fd()`, and `wait_for_input(timeout)` |
| `HostGraph::value_serializers` | A `ValueSerializers` the host builds and passes to `push_value` and `inspect_payload` |
| `HostBridgeHandle::feed_payload` | `host.feed(port, Value)`, `feed_typed(port, Arc<T>)`, `feed_inline`, `feed_shared`, `feed_handed`; through a `StreamGraph`, `graph.input(port)` gives a handle with `feed` |
| `HostBridgeHandle::push` | `host.push_value(port, &HostValue, &serializers)`: a plain value, built into the port's type by that type's entry |
| `Payload` | `daedalus_value::Value` |
| `FeedOutcome` | `FeedOutcome { woke, dropped }` |
| `ExecutionTelemetry` node metrics | `Telemetry::report(runner).nodes` (a `NodeRow` a step), and `loaded.metric_rows()` for what nodes report themselves |
| `ExecutionTelemetry` edge metrics | No per-edge table. `port_infos()` gives a host port's queue depth, `port_drops(port)` its drops, and a full edge fails its tick naming the edge |
| `registry.graph_document`, `refresh_graph_ports_with` | No registry call. A document is `GraphDoc`; the planner fills a host bridge's ports from the graph's edges and checks every node's ports against its declaration |
| `registry.graph_requirements`, `GraphDocument::requires` | `registry.requirements(&graph)` gives what a graph needs, for the document's `requires`; `registry.check_requires(&doc)` refuses a document the host can't meet, by package and version |
| `GraphDocument::from_json`, `to_json` | `GraphDoc::parse(text)`, `doc.to_json()` |

## What changed shape

- A backend is asked what it can run and compiles what it is given. There is no device
  assignment pass that knows about GPUs.
- A solved graph is a flat list of steps with numbered value slots. The runner walks it and
  knows nothing of the planner or the registry.
- Failure handling is one rule everywhere: never give up, back off to a cap, turn nothing off
  automatically.
