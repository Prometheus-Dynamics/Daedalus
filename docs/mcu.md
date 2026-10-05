# MCU Profile

The MCU profile runs a Daedalus graph on a microcontroller in a few KiB of flash, with RAM equal
to the graph's edge queues and node state and no heap. The graph is planned on the host, at
build time, by the regular planner; the device runs generated Rust code that calls the node
functions directly. Crates:

- [`daedalus-mcu`](../crates/mcu/device/src/lib.rs) (`#![no_std]`, no `alloc`): fixed-capacity
  edge `Queue`s, the node descriptor types, `McuType` keys, `NodeState`, `Ctx`/`Clock` and the
  error enums, plus the `#[daedalus_mcu::node]` attribute (from `daedalus-mcu-macros`).
- [`daedalus-mcu-build`](../crates/mcu/build/src/lib.rs) (host, `std`): plans a graph against the
  device nodes and writes the device module, from a `build.rs`.
- [`examples/mcu_blink`](../examples/mcu_blink): a five-node graph, its firmware for Cortex-M4F
  and Cortex-M0+, and a native test of the same generated code.

## Which Runtime

| | Full runtime (`std`) | `no_std` runtime (tier 2) | MCU profile |
| --- | --- | --- | --- |
| Targets | Linux, macOS, Windows, WASM | bare metal with `alloc` | bare metal, no heap needed |
| Planning | on the host, at run time | on the device or the host | on the host, at build time |
| Graph changes | patches, hot reload, plugins | rebuild the plan on the device | rebuild the firmware |
| Payloads | `Payload` (`Arc<dyn Any>`, type keys, lineage) | same | typed values in typed queues |
| Adapters | any registered adapter, GPU, foreign interfaces | CPU adapters | builtin numeric widening only |
| Flash / RAM (measured below) | - | ~236 KiB flash, ~20-29 KB heap (one node) | ~3-4 KiB flash, 160 B RAM (five nodes) |

Use the MCU profile when the graph is fixed per firmware build and the device is small
(Cortex-M0+/M4 class, 32 KB of RAM or less). Use the `no_std` runtime
([development.md](development.md#tier-2-no_std-serial-runtime-and-engine)) when the device must
plan or change graphs itself, needs runtime adapters or `Payload` features, and has a heap to
spare. Use the full runtime everywhere else.

## Workflow

1. **Nodes** live in their own `#![no_std]` crate, written as plain functions:

   ```rust
   use daedalus_mcu::{Ctx, NodeState, node};

   pub struct Lowpass { y: f32, primed: bool }
   impl NodeState for Lowpass {
       const INIT: Self = Lowpass { y: 0.0, primed: false };
   }

   #[node(id = "blink.lowpass", inputs("x", "alpha"), outputs("y"), state(Lowpass))]
   pub fn lowpass(x: f32, alpha: f32, state: &mut Lowpass) -> f32 { /* ... */ }
   ```

   The device crate depends on it twice: as a regular dependency (the functions run on the
   device) and as a build dependency (its `build.rs` reads the descriptors).

2. **The graph** is a regular Daedalus graph: a `GraphDocument` JSON file (as editors save it,
   [`examples/mcu_blink/graph.json`](../examples/mcu_blink/graph.json)) or a
   `daedalus_planner::Graph` built in Rust. Node ids are the `#[node]` ids; the host bridge node
   (`io.host_bridge` with `host_bridge` metadata) is the application's I/O, edge metadata sets
   policies (`daedalus.edge.pressure`: `latest_only`, `bounded` + `daedalus.edge.capacity`, ...),
   `const_inputs` set constants and `daedalus.host_input_types` declares host port types.

3. **`build.rs`** plans and writes the module:

   ```rust
   let nodes = [scale::NODE, lowpass::NODE, threshold::NODE];
   let json = std::fs::read_to_string("graph.json").unwrap();
   let source = daedalus_mcu_build::compile_document(&json, &nodes, &Default::default())
       .unwrap_or_else(|err| panic!("{err}"));
   daedalus_mcu_build::write_out_dir("graph.rs", &source).unwrap();
   println!("cargo:rerun-if-changed=graph.json");
   ```

   Planning errors (unknown nodes, type mismatches, missing converters, unconnected inputs) and
   unsupported features fail the build with the planner's diagnostics.

4. **The device crate** includes it, `mod graph { include!(concat!(env!("OUT_DIR"),
   "/graph.rs")); }`, and drives it:

   ```rust
   let graph = cortex_m::singleton!(: Graph = Graph::new()).unwrap();
   loop {
       graph.push_sample(adc.read())?;    // one push_<port> per host input
       graph.tick()?;                     // or tick_with(&clock) / tick_at(now_micros)
       if let Some(on) = graph.pop_led() { led.set(on); } // one pop_<port> per host output
   }
   ```

   `Graph::new` is a `const fn`, so the graph can also live in a `static`.

### Node Functions

`#[daedalus_mcu::node(id = .., inputs(..), outputs(..), state(T), fire = "all")]` keeps the
function and adds a module of the same name with the descriptor `NODE`, the type aliases
`In<k>`/`Out<k>`/`State` and a uniform `run` that the generated code calls.

- Inputs: `T` (moved in), `&T`, `&mut T`, and `Option<T>`/`Option<&T>` for optional inputs.
  `inputs(..)` names them in order (default: the parameter names).
- Outputs: the return value, `()`, one value or a tuple, optionally `Result<_, E>` with
  `E: Into<NodeError>`; an `Option<T>` value is a conditional output (`None` pushes nothing).
  `outputs(..)` names them (default `"out"` for a single output).
- `state(T)`: the `&mut T` parameter is the node's state slot, initialised from
  `NodeState::INIT` (implemented for numbers, `bool`, `()`, `Option<T>` and arrays).
- `&Ctx`: the tick counter and the time passed to `tick_at`/`tick_with`.
- Port types need an `McuType` key, which the planner type checks: builtin scalars use the full
  runtime's keys (`typeexpr:{"Scalar":"F32"}`, ...), so the same graph types the same way in both;
  other types get one with `daedalus_mcu::mcu_type!(Sample => "demo:sample")`. Values that fan
  out to several edges are cloned.

The full runtime's `#[node]` generates handlers over `NodeIo`/`Payload` (`Arc`, `Any`,
allocation), so the device attribute is separate: it generates only the descriptor and typed
glue. `daedalus-mcu-build` turns the descriptor into the same `NodeDecl` a `#[node]` declares
(port keys and schemas, optional inputs, fire mode, conditional outputs), so planning,
readiness and lints treat device nodes like any other.

## Semantics

The generated `tick` runs the nodes once in the planner's schedule order, with the serial
runtime's rules:

- **Readiness.** A node runs when every connected required input has a value; optional inputs
  never block and are `None` when nothing arrived; a node without connected required inputs
  runs every tick. In fire mode `any` (default) the node drains its edges every tick and gets the
  oldest value of each (what arrives while it is not ready is dropped). In fire mode `all` it
  waits, popping nothing, until every connected required edge holds a value, then pops one value
  per edge.
- **Constants** (`const_inputs`, builtin scalars and `bool`) are typed literals in the code. A
  required input with neither an edge nor a constant is a build error.
- **Edges** are inline ring buffers sized from their policy:

  | Edge policy | Queue | When full |
  | --- | --- | --- |
  | `latest_only` | 1 slot | replaces |
  | `bounded` (capacity N) | N slots | drops the oldest (or per its overflow policy) |
  | `drop_oldest` / `drop_newest` | `fifo_capacity` slots | drops the oldest / the new value |
  | FIFO (default), `error_on_full` | `fifo_capacity` slots (default 4) | `McuError::QueueFull` |

  An edge between two nodes whose consumer fires in mode `any` is drained every tick and a
  producer pushes at most once per tick, so it gets one slot whatever its policy. Larger queues
  appear only where values can wait: host inputs, host outputs the application pops, and inputs
  of `fire = "all"` nodes.
- **Conversions.** An edge the planner resolves with the builtin numeric widening adapter
  (`u16 -> f32`, ...) converts with `From` at the push; any other adapter is a build error.
- **Errors** are small `Copy` enums without formatting: `McuError::Node { node, error }` (a node
  returned `Err`; the tick stops there) and `McuError::QueueFull { edge }`. Indices refer to the
  generated `NODE_IDS` and `EDGES`, which cost no flash unless used. The `defmt` feature derives
  `defmt::Format` for them.
- **No allocation.** The executor has no heap and no `alloc` dependency; a firmware without a
  global allocator links. The `alloc` feature only adds keys for `String`/`Vec<u8>` ports.

## Measurements

`examples/mcu_blink` (five nodes, eight edges: `host.sample` (u16, widened) -> scale -> lowpass
-> threshold -> rising/blink -> three host outputs), built with the workspace's `mcu` profile
(`opt-level = "z"`, fat LTO, `panic = "abort"`, one codegen unit) by `scripts/ci.sh mcu`:

| Target | Flash (.vector_table + .text + .rodata + .data) | Static RAM (.data + .bss) | Heap |
| --- | --- | --- | --- |
| `thumbv7em-none-eabihf` (Cortex-M4F) | 3200 B (1024 vector table + 2176 code) | 160 B | none (no allocator linked) |
| `thumbv6m-none-eabi` (Cortex-M0+) | 3716 B (192 vector table + 3524 code, incl. soft-float) | 160 B | none |

The 160 B of RAM are the whole `Graph` (eight queues, five state slots, the tick counter) plus
the `singleton!` flag; the stack adds what a tick's node calls need. For comparison, the `no_std`
runtime running a one-node graph on the M4F takes ~213 KiB of code plus 23 KiB of rodata, a
~18-20 KB heap peak (32-bit) and 521 allocations, and ~560 KiB with on-device planning. Per tick
the MCU profile allocates nothing (the native test counts). `scripts/ci.sh mcu` prints these
numbers and fails above 8 KiB of flash or 512 B of static RAM per target.

## Design Decisions

- **Generated Rust, not a binary plan.** The plan becomes `const` data and straight-line code:
  no parser, no plan interpreter, no `TypeId` dispatch, and the compiler inlines the node
  functions into `tick`. A runtime-loaded plan (for example postcard over a fixed node table)
  would need a type-erased slot store and an interpreter; it is not implemented.
- **Planning reuses the host stack.** `daedalus-mcu-build` plans with `daedalus_planner` against
  a `daedalus_runtime::plugins::PluginRegistry` (its builtins: host bridge, primitive types,
  widening adapters) plus the device declarations, and reads edge policies and the schedule from
  `build_runtime`, so type checking, adapter choice, policies and order match the full runtime.
- **One queue per edge, typed.** Each edge stores its consumer's port type; fan-out clones into
  each queue, so no reference counting is needed.

## Limitations

Not supported by the MCU profile (each is a build error naming the node or edge): adapters other
than builtin numeric widening (including user `#[adapt]`s, branch adapters and foreign
interfaces), several edges into one input (fan-in, `FanIn<T>`), an input with both an edge and a
constant, constants of non-scalar types, GPU nodes, coalescing and freshness policies other than
latest-only, and more than one host bridge node. Plugins, `dylib-plugins`, patches, hot reload,
telemetry, `Payload` lineage and the `HostGraph` API are runtime features with no device
counterpart. Nodes that fail stop the tick (fail-fast). The example's `memory.x` is a generic
128 KiB/32 KiB layout for linking and measuring; adapt it, and replace the stand-in ADC and LED,
for a board.
