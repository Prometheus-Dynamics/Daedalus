# MCU Profile

The MCU profile runs a Daedalus graph on a microcontroller in a few KiB of flash, with no heap.
The graph is always planned on the host by the regular planner; what the device runs depends on
the mode each firmware chooses:

- **Compiled**: generated Rust that calls the node functions directly. The smallest build; the
  graph is fixed per firmware.
- **Compiled + tunable**: the same, with chosen graph constants turned into parameters the
  application or a host tool changes at run time.
- **Loaded**: an interpreter over the firmware's node library runs plan blobs loaded at run
  time, so wiring, constants and edge policies change without a firmware update.

Crates:

- [`daedalus-mcu`](../crates/mcu/device/src/lib.rs) (`#![no_std]`, no `alloc`): fixed-capacity
  edge `Queue`s, the node descriptor types, `McuType` keys, `NodeState`, `Ctx`/`Clock`, `Scalar`
  values, parameters (`Tunable`, `ParamUpdate`), the wire codec and the error enums, plus the
  `#[daedalus_mcu::node]` attribute (from `daedalus-mcu-macros`). Feature `loaded`: the plan
  interpreter and the node library types.
- [`daedalus-mcu-build`](../crates/mcu/build/src/lib.rs) (host, `std`): plans a graph against the
  device nodes and renders it as a compiled module, a node library or a plan blob, from a
  `build.rs` or the `daedalus-mcu` command line tool.
- [`examples/mcu_blink`](../examples/mcu_blink): a five-node graph in all three modes, with
  firmware for Cortex-M4F and Cortex-M0+ and native tests of the same code.

## Which Runtime

| | Full runtime (`std`) | `no_std` runtime (tier 2) | MCU profile |
| --- | --- | --- | --- |
| Targets | Linux, macOS, Windows, WASM | bare metal with `alloc` | bare metal, no heap needed |
| Planning | on the host, at run time | on the device or the host | on the host |
| Graph changes | patches, hot reload, plugins | rebuild the plan on the device | rebuild the firmware, set parameters, or load a plan blob |
| Payloads | `Payload` (`Arc<dyn Any>`, type keys, lineage) | same | typed values in typed queues |
| Adapters | any registered adapter, GPU, foreign interfaces | CPU adapters | builtin numeric widening only |
| Flash / RAM (measured below) | - | ~236 KiB flash, ~20-29 KB heap (one node) | ~3-13 KiB flash, 160-584 B RAM (five nodes) |

Use the MCU profile when the device is small (Cortex-M0+/M4 class, 32 KB of RAM or less) and
its node set is fixed per firmware. Use the `no_std` runtime
([development.md](development.md#tier-2-no_std-serial-runtime-and-engine)) when the device must
plan graphs itself, needs runtime adapters or `Payload` features, and has a heap to spare. Use
the full runtime everywhere else.

### Which Mode

| | Compiled | Compiled + tunable | Loaded |
| --- | --- | --- | --- |
| Changes at run time | nothing | marked constants | wiring, constants, parameters, edge policies, which library nodes run |
| Needs a firmware update for | any change | wiring, unmarked constants | new or changed node code |
| Code | straight-line `tick`, nodes inlined | + parameter table, setters, update decoder | + interpreter, blob validator, one type-erased adapter per library node |
| RAM | queues + state | + one field per parameter | a fixed arena (plan tables, queues, state, constants) |
| Blink example, M0+ | 3.6 KiB flash, 160 B RAM | 6.9 KiB, 176 B | 12.4 KiB, 584 B |

Choose compiled for minimal devices and frozen graphs (a production build of a graph tuned on a
tunable build is compiled with `freeze_params`). Choose tunable when the structure is settled but
thresholds, gains or periods are calibrated per unit or in the field. Choose loaded when the same
firmware must run different graphs (per product variant, per customer, or updated over the air)
and a few KiB more flash and an arena of RAM are acceptable.

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
   `const_inputs` set constants, `daedalus.host_input_types` declares host port types and
   `daedalus.mcu.params` marks tunable constants ([Parameters](#parameters)).

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

## Parameters

Tunable parameters are graph constants marked in the graph, on the node that owns them: node
metadata `daedalus.mcu.params` maps input names to an inclusive range `[min, max]` (or unit for
the type's full range); a list of input names marks them without ranges.

```json
"metadata": {
  "daedalus.mcu.params": {"type": "Map", "value": [
    [{"type": "String", "value": "alpha"},
     {"type": "List", "value": [{"type": "Float", "value": 0.0}, {"type": "Float", "value": 1.0}]}]
  ]}
}
```

The marking lives in the graph, not in `#[node]`, because which constants are tuned is a
property of a deployment, and the same node can have a fixed constant in one graph and a tuned one
in another. A marked input must have a constant of a scalar type (`bool`, the integers up to 64
bits, `f32`, `f64`); the constant must be inside its range. Parameter ids are numbered in
schedule order, then input order, and named `<node>.<input>` (node label, else id).

In a compiled module each parameter becomes a field initialised with the constant, and the
graph gets:

- `PARAM_NAMES: [&str; N]` and `PARAMS: [ParamSpec; N]` (type and range per id);
- `impl daedalus_mcu::Tunable`: `set_param(id, Scalar) -> Result<(), ParamError>`,
  `param(id) -> Option<Scalar>`, and `apply_update(&[u8])` for a `ParamUpdate` message;
- a typed setter per parameter, `set_<node>_<input>(value)` (`set_lowpass_alpha(0.5)`).

`set_param` converts the value with the planner's rules for constants (`ValueType::check_value`:
an integer to any integer type it fits and to floats that represent it exactly, floats to
floats, `bool` to `bool`; `ParamError::Type` otherwise), then checks the range
(`ParamError::Range`, also for NaN). A new value is used from the next tick on: setters take
`&mut self`, so they never run inside a tick. Without markers (or with
`CompileOptions::freeze_params`) the module is exactly the compiled one: constants are literals
and no parameter code exists; `scripts/ci.sh mcu` checks the compiled firmware's size to the
byte.

**Update messages** are postcard-encoded `(id: u16, value: Scalar)` (`ParamUpdate`, 3 to 14
bytes): the id as a varint, the value's `ScalarKind` tag (`bool` 0, `i8` 1, `i16` 2, `i32` 3,
`i64` 4, `u8` 5, `u16` 6, `u32` 7, `u64` 8, `f32` 9, `f64` 10), then the value (one byte for
`bool`/`i8`/`u8`, zigzag varints for signed, varints for unsigned, little-endian floats).
`threshold.on = 2.5` (id 1) is `01 09 00 00 20 40`. The transport (UART, USB, BLE, CAN) and its
framing are the application's.

**Manifest.** `McuPlan::manifest` (written by the example's `build.rs` as `tunable.json`) lists
the plan's node, edge, host port and parameter ids with names, types, defaults and ranges.
Host tools encode updates by name with `PlanManifest::param_update`, which applies the device's
checks, or from the command line:

```console
$ daedalus-mcu param tunable.json threshold.on 2.5
010900002040
```

## Loaded Mode

A loaded-mode firmware contains a **node library** and the interpreter; plans are compiled on
the host into **plan blobs** and loaded at run time.

1. **`build.rs`** generates the library from the nodes the firmware includes, and writes its
   manifest for host tools:

   ```rust
   let library = daedalus_mcu_build::library(&[scale::NODE, lowpass::NODE, threshold::NODE])?;
   daedalus_mcu_build::write_out_dir("library.rs", library.to_rust()?)?;
   daedalus_mcu_build::write_out_dir("library.json", library.to_json())?;
   ```

   `library.rs` defines `LIBRARY` (a `daedalus_mcu::loaded::Library`): per node its input and
   output type ids, its state layout and a type-erased `run` adapter over the node's `run` glue,
   plus `LIBRARY_HASH`, a hash of the node interfaces (ids, port names, types, optional inputs,
   conditional outputs, fire mode; not the node code).

2. **Plans** are compiled against the library manifest, with the same planning, lowering and
   checks as compiled mode (`daedalus_mcu_build::compile_loaded`, then `McuPlan::to_blob`), in a
   build script or with the host tool:

   ```console
   $ daedalus-mcu plan library.json graph_b.json plan_b.bin plan_b.json
   ```

   Blobs are deterministic: the same graph and library give the same bytes.

3. **The firmware** owns an `Interpreter<ARENA>` (`ARENA` bytes of RAM, at most 65535) and loads
   a blob:

   ```rust
   let interp = cortex_m::singleton!(: Interpreter<512> = Interpreter::new(&LIBRARY)).unwrap();
   interp.load(blob)?;                              // validate, then swap (`blob: &[u8]`)
   let sample = interp.input("sample").unwrap();    // host ports by name (or by manifest id)
   let led = interp.output("led").unwrap();
   loop {
       interp.push(sample, adc.read())?;            // typed: McuError::Port on a wrong type
       interp.tick()?;
       if let Some(on) = interp.pop::<bool>(led)? { gpio.set(on); }
   }
   ```

   `load` first validates the whole blob without touching the running plan, then lays the new
   plan out in the arena: queues empty, node states at `INIT`, the tick counter at 0. It runs
   between ticks (it takes `&mut self`), so plans swap at a tick boundary; on error the running
   plan keeps running. `Interpreter::required(&LIBRARY, &blob)` returns the arena bytes a blob
   needs. Parameters work as in compiled mode (`Interpreter` implements `Tunable`; ids from the
   plan manifest).

**Validation** (`LoadError`): the `DMCU` magic and the format version (`Version`), the library
hash (`Library`), library entries and input counts (`Node`), required inputs without a source
and constants of another type than their input (`Input`), edges whose producer does not exist,
does not run before its consumer, or whose capacity or overflow policy is invalid (`Edge`),
producer and consumer types that differ and do not widen (`Type`), parameters whose value,
range or type is invalid (`Param`), host ports of unknown types (`Port`), the arena size
(`Arena { needed }`), and truncated input or trailing bytes (`Malformed`).

**Transport and persistence** are the application's: receive the blob over UART, USB, BLE or
CAN into a RAM buffer, or write it to a flash partition and pass the `&[u8]` from there, then
call `load` between ticks. Frame and checksum the transfer (a CRC32 over the blob) and keep the
last good blob to load at boot; the interpreter checks structure and types, not integrity.

**Over-the-air updates of node code are out of scope.** A blob can only use nodes the firmware's
library contains; new or changed node functions need a firmware update (for example with
MCUboot or embassy-boot), which also changes the library hash when interfaces change, so old
blobs are rejected instead of misread.

**Interpreter limits.** Port values are `Copy` (queue slots are bytes) and node states have no
drop glue (both checked at compile time in `library.rs`); a library has at most 256 nodes of
at most 8 inputs and outputs; constants and parameters are scalars.

### Formats and Versioning

- **Plan blob** (`FORMAT_VERSION` 1), postcard encoding:

  ```text
  magic [u8; 4] = "DMCU", version u8, library u64, plan u64,
  entries: seq<u8>                     library entry of each node, in schedule order
  host_inputs, host_outputs: seq<{ name: u32, ty: u16 }>
  nodes: seq<{ wait_all: bool, inputs: seq<Input> }>
    Input = Absent | Edge(Edge) | Const(Scalar) | Param { value, min, max: Scalar }
    Edge  = { from: Host(u16) | Node(u16, u8), capacity: u16, overflow: u8 }
  outputs: seq<Edge>                   the edge into each host output
  ```

  Names are 32-bit FNV-1a hashes (`daedalus_mcu::name_hash`); type ids are the `ScalarKind`
  tags, then the library's other port types in manifest order; overflow is 0 drop oldest,
  1 drop newest, 2 error. Edges and parameters are numbered in order of appearance (the plan
  manifest lists them). A producer node must come before its consumer.
- **Versioning.** The version byte changes whenever the blob layout or its meaning changes; a
  firmware rejects other versions (`LoadError::Version`), so host tools must target the
  firmware's version. Node interface changes change the library hash. `ParamUpdate` has no
  version byte: it is the stable `(id, Scalar)` pair, and ids come from the manifest of the
  plan the device runs (check `plan_hash`).
- **Manifests** (JSON): `daedalus.mcu.library` (hash, nodes with ports and type keys, extra types)
  and `daedalus.mcu.plan` (plan and library hashes, node, edge, port and parameter ids).

The device decodes with a small hand-written reader of the postcard wire format and the host
encoder uses the same `daedalus_mcu::wire` code, so no serde runs on the device.

## Semantics

Every mode runs the nodes once per tick in the planner's schedule order, with the serial
runtime's rules (the interpreter implements the same rules as the generated `tick`; the native
tests compare them):

- **Readiness.** A node runs when every connected required input has a value; optional inputs
  never block and are `None` when nothing arrived; a node without connected required inputs
  runs every tick. In fire mode `any` (default) the node drains its edges every tick and gets the
  oldest value of each (what arrives while it is not ready is dropped). In fire mode `all` it
  waits, popping nothing, until every connected required edge holds a value, then pops one value
  per edge.
- **Constants** (`const_inputs`, builtin scalars and `bool`) are typed literals in compiled code
  and typed values in the arena in loaded mode. A required input with neither an edge nor a
  constant is a build error.
- **Edges** are ring buffers sized from their policy:

  | Edge policy | Queue | When full |
  | --- | --- | --- |
  | `latest_only` | 1 slot | replaces |
  | `bounded` (capacity N) | N slots | drops the oldest (or per its overflow policy) |
  | `drop_oldest` / `drop_newest` | `fifo_capacity` slots | drops the oldest / the new value |
  | FIFO (default), `error_on_full` | `fifo_capacity` slots (default 4) | `McuError::QueueFull` |

  An edge between two nodes whose consumer fires in mode `any` is drained every tick and a
  producer pushes at most once per tick, so it gets one slot whatever its policy. Larger queues
  appear only where values can wait: host inputs, host outputs the application pops, and inputs
  of `fire = "all"` nodes. Compiled queues and the interpreter's byte queues share the ring
  arithmetic (`daedalus_mcu::Ring`).
- **Conversions.** An edge the planner resolves with the builtin numeric widening adapter
  (`u16 -> f32`, ...) converts at the push (`From` in compiled code, `Scalar::coerce` in the
  interpreter); any other adapter is a build error.
- **Errors** are small `Copy` enums without formatting: `McuError::Node { node, error }` (a node
  returned `Err`; the tick stops there), `McuError::QueueFull { edge }` and, in loaded mode,
  `McuError::Port { port }`. Indices refer to the generated `NODE_IDS` and `EDGES` (compiled)
  or the plan manifest (loaded), which cost no flash unless used. The `defmt` feature derives
  `defmt::Format` for them.
- **No allocation.** Nothing in `daedalus-mcu` allocates; a firmware without a global allocator
  links. The native tests count allocations around pushes, ticks, pops, parameter updates and
  plan loads in every mode: zero. The `alloc` feature only adds keys for `String`/`Vec<u8>` ports
  (compiled mode).

## Measurements

`examples/mcu_blink` (five nodes, eight edges: `host.sample` (u16, widened) -> scale -> lowpass
-> threshold -> rising/blink -> three host outputs), built with the workspace's `mcu` profile
(`opt-level = "z"`, fat LTO, `panic = "abort"`, one codegen unit) by `scripts/ci.sh mcu` with
the pinned Rust 1.99.0 (the exact compiled size moves with the toolchain). Flash
is `.vector_table + .text + .rodata + .data`, static RAM `.data + .bss`; no heap in any mode
(no allocator linked).

| Mode (firmware) | `thumbv7em-none-eabihf` (M4F) flash | `thumbv6m-none-eabi` (M0+) flash | Static RAM | Budget (flash / RAM) |
| --- | --- | --- | --- | --- |
| Compiled (`daedalus-mcu-blink`) | 3072 B | 3392 B | 160 B | 8 KiB / 512 B, and exactly these sizes |
| Compiled + tunable, 4 parameters (`-tunable`) | 5932 B | 6764 B | 176 B | 10 KiB / 512 B |
| Loaded, 5-node library, two blobs in flash (`-loaded`) | 11516 B | 12396 B | 584 B | 16 KiB / 1 KiB |

- **Compiled**: 1024 B (M4F) / 192 B (M0+) of vector table; the M0+ code includes soft-float.
  The 160 B of RAM are the whole `Graph` (eight queues, five state slots, the tick counter) plus
  the `singleton!` flag; the stack adds what a tick's node calls need.
- **Tunable**: +2.8 KiB (M4F) / +3.3 KiB (M0+): the type conversion and range check (with `f64`
  soft-float for `f64` values and parameters), the update decoder and the setters; +16 B of RAM
  for the four parameter fields.
- **Loaded**: +8.2 KiB (M4F) / +8.8 KiB (M0+): the blob validator (about 2 KiB), the
  interpreter, five adapters and the conversion code; the blobs are 174 B (plan A) and 123 B
  (plan B). RAM is the 512 B arena plus the interpreter's state; plan A uses 424 B of the arena
  (plan tables about 280 B, queues, states, constants and parameter bounds the rest), plan B 312 B.

For comparison, the `no_std` runtime running a one-node graph on the M4F takes ~213 KiB of code
plus 23 KiB of rodata, a ~18-20 KB heap peak (32-bit) and 521 allocations, and ~560 KiB with
on-device planning. `scripts/ci.sh mcu` prints every mode's numbers per target and fails above
its budgets, or when the compiled firmware's size changes.

## Design Decisions

- **One plan, three renderings.** `daedalus-mcu-build` lowers the planner's output once
  (`McuPlan`: schedule, queue shapes, constants, parameters, host ports, every MCU restriction)
  and renders it as Rust (`to_rust`) or as a blob (`to_blob`); the device's interpreter applies
  the readiness and queue rules of the generated code. Compiled mode stays straight-line code
  with no parser, interpreter or dispatch; the loaded and tunable code paths are separate
  modules that a compiled firmware never links.
- **Planning reuses the host stack.** `daedalus-mcu-build` plans with `daedalus_planner` against
  a `daedalus_runtime::plugins::PluginRegistry` (its builtins: host bridge, primitive types,
  widening adapters) plus the device declarations, and reads edge policies and the schedule from
  `build_runtime`, so type checking, adapter choice, policies and order match the full runtime.
  The device's widening table and constant conversion rules are tested against the runtime's
  adapters and `ValueType::check_value`.
- **One queue per edge, typed.** Each edge stores its consumer's port type; fan-out clones into
  each queue, so no reference counting is needed.
- **Validate, then swap.** The interpreter walks a blob twice with the same code: once to
  validate and size it (no memory besides the stack), then to write the plan into the arena. A
  bad blob never disturbs the running plan, and the arena needs no room for two plans.
- **One codec.** Parameter updates and blobs use the postcard wire format; the device decoder
  and the host encoder share `daedalus_mcu::wire`, and any postcard implementation can produce
  them from the documented schema.

## Limitations

Not supported by the MCU profile (each is a build error naming the node or edge): adapters other
than builtin numeric widening (including user `#[adapt]`s, branch adapters and foreign
interfaces), several edges into one input (fan-in, `FanIn<T>`), an input with both an edge and a
constant, constants of non-scalar types, GPU nodes, coalescing and freshness policies other than
latest-only, and more than one host bridge node. Loaded mode also needs `Copy` port values,
states without drop glue, scalar constants and at most 8 ports per node. Plugins,
`dylib-plugins`, patches, hot reload, telemetry, `Payload` lineage and the `HostGraph` API are
runtime features with no device counterpart. Nodes that fail stop the tick (fail-fast). The
example's `memory.x` is a generic 128 KiB/32 KiB layout for linking and measuring; adapt it, and
replace the stand-in ADC and LED, for a board.
