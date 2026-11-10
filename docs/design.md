# Daedalus: the design

This is the design the `next` branch builds. It is the source of truth for every brief in
[`docs/prs/`](prs/). It is written so that an agent with no other context can work from it.
[`architecture.md`](architecture.md) walks through the same design with worked examples and reasons.

## What Daedalus is

Daedalus runs graphs of typed nodes. You write a node as a plain function or a shader, connect
nodes into a graph, and a planner proves the graph is valid before anything runs. CPU and GPU are
the first places a node can run, and they are not special: each is a **backend**.

**Daedalus knows:**

- How to connect typed ports and prove a graph valid.
- How to ask backends what they can run and let them compile it.
- How to stitch the results into one flat list of steps, with moves and waits between them.
- How to run that list, report what failed, and keep going.

**Daedalus does not know:**

- What any node does.
- What a GPU, shader, NPU or chip is, or what an image or any other value means. Backends and
  whoever wrote the nodes know that. The core only matches type keys and memory-space ids.
- Any board, chip or device.

Planning allocates. Running does not. Everything needed to run a solved graph works with no heap.

## The system

```text
  #[node] on a function      #[node] on a shader      #[node] on a model      a Python worker
         |                          |                        |                       |
         v                          v                        v                       v
   cpu backend                wgpu backend            someone's npu backend     process backend
         |                          |                        |                       |
         +--------------------------+-----------+------------+-----------------------+
                                                |
                                      one Backend interface
                                                |
   +--------------------------------------------v---------------------------------------------+
   | Registry      one entry per node id; each entry has one declaration and several bodies   |
   | Planner       1 solve   2 ask backends   3 assign   4 backends compile   5 stitch         |
   | Solved graph  a flat list of steps, moves and waits. Steps, Blob or Source                |
   | Runner        walks the list. No planner, no registry, no heap                            |
   +------------------------------------------------------------------------------------------+
                                                |
         linked into an application   |   daedalusd   |   generated firmware on a chip
```

## Repository layout

```text
core/        abi, types, value, graph, backend, registry, plan, planner, runner, macros, derive,
             host, wire, client, loader, capi, daedalus (the facade)
backends/    cpu, cpu-author, wgpu, wgpu-author, dmabuf, process, sim, and *-sys crates
languages/   python, node, java, cpp, rust: SDKs for writing nodes run by the process backend
service/     daedalusd, daedalus-ctl
testing/     crates/* (conformance suite, benches, budgets), board-runs/, fixtures/ (outside the
             workspace globs)
examples/    each directory directly inside is a crate
```

Workspace members are globs, so adding a crate never edits the root `Cargo.toml`. Every directory
directly inside `core/`, `backends/`, `languages/`, `service/`, `testing/crates/` and `examples/`
must be a crate.

## The flow

1. **Solve.** Expand groups, apply defaults, run rewrites, check ports, typecheck,
   resolve const inputs, insert conversions, align rates, order the graph, lint. Nothing here knows
   about any backend.
2. **Ask.** Each backend receives an `Offer` holding only the nodes it has a body for, and returns
   the ones it `can_take`.
3. **Assign.** Forced placements first (the graph names a backend). Then choose so the graph
   crosses backends as few times as possible. Ties go to the Host's priority order. The result is
   deterministic: the same input gives the same assignment.
4. **Compile.** Each backend receives its regions and returns `Compiled`. It may merge nodes. A
   backend that refuses a node loses it, and that node is assigned again. There is no negotiation.
5. **Stitch.** The planner inserts transfers and fences between regions and lays everything out as
   a flat step list with numbered value slots.

**Region rules.** A region is a connected set of nodes on one backend. It keeps every boundary
input and output of its nodes, cannot span a rate change, is convex (a path cannot leave it and
re-enter), and every step it produces lists the nodes it covers.

## The backend interface

Everyone builds against this. It lands in A3, A5 and A7. A7 can refine it. After that it changes
only through an A-series PR.

```rust
// core/backend. Runs while planning, so it may allocate.
pub trait Backend: Send + Sync {
    fn id(&self) -> &str;
    fn memory(&self) -> Vec<MemorySpace>;
    fn transfers(&self) -> Vec<Transfer>;
    fn can_take(&self, offer: &Offer) -> Vec<NodeRef>;
    fn compile(&self, region: &Region, cx: &CompileCx) -> Result<Compiled, Refusal>;
}

pub enum Compiled {
    Steps(Vec<StepUnit>),      // run now, in this process; each unit lists what it covers
    Blob(Vec<u8>),             // a stored plan a loader or small runner reads
    Source(GeneratedCode),     // code compiled ahead of time, for example on a chip
}

// core/abi. Crosses plugin boundaries, so it is repr(C), layout-hashed, and has no allocation.
#[repr(C)]
pub struct Step {
    pub run: unsafe extern "C" fn(*mut (), *mut Slots, *mut StepCx) -> StepEnd,
    pub data: *mut (),
    pub release: Option<unsafe extern "C" fn(*mut ())>,
}

#[repr(C, u32)]
pub enum StepEnd { Done, Pending(Fence), Skipped, Fault }
```

`StepEnd::Fault` carries no data. The step writes the detail through `StepCx`: an optional failing
node (`at`) and a bounded reason string. The plan already records which nodes each step covers, so
the report always has `covers`, and `at` is only filled when the backend knows it. Reporting a
fault allocates nothing.

A backend can be linked or loaded as a plugin. One plugin can ship a backend and its nodes. A
backend ships two crates: a small authoring crate that nodes depend on, and the part that runs.

## Writing a node

There is one macro. It reads the id, ports and state. For the body it expands to a call into the
named backend's authoring crate, so the macro knows no backend and a new backend needs no change to
it. The `backend` setting is a path in the caller's scope, and the item at that path must export
the authoring macro.

```rust
// CPU is the default.
#[node(id = "demo.track", inputs("boxes"), outputs("tracks"))]
fn track(boxes: &Boxes) -> Tracks { .. }

// A per-pixel GPU function.
#[node(id = "demo.normalize", backend = wgpu(pixel), inputs("image"), outputs("image"))]
const NORMALIZE: &str = "fn apply(c: vec4f, cfg: Config) -> vec4f { return (c - cfg.mean) / cfg.std; }";

// Hardware Daedalus has never heard of.
#[node(id = "vision.detect", backend = acme_npu(model = "yolo.rknn"), inputs("image"), outputs("boxes"))]
struct Detect;
```

**GPU nodes come in three levels.** A per-pixel function can be merged into one shader with its
neighbours. A whole shader can share a submit but not merge. Your own recording code gets the
encoder and controls everything. The merge rule: the second node reads only the first node's result
at the same pixel.

### One node, several backends

The id is the node. A node's definition has two parts: what the graph sees, and how it runs. Each
`#[node]` emits one constant record holding both, and the registry joins records with the same id.

```rust
#[node(id = "demo.blur", inputs("image"), outputs("image"))]
fn blur_cpu(image: &Image) -> Image { .. }

#[node(id = "demo.blur", backend = wgpu, inputs("image"), outputs("image"))]
const BLUR: &str = include_str!("blur.wgsl");
```

| A record arrives with | What happens |
|---|---|
| A new id | Creates the entry, with this declaration and one body |
| A known id, same declaration, new backend | Adds a body |
| A known id, different declaration | Registration error naming both places and the port that differs |
| A known id and a backend it already has | Registration error naming both places |

- **Shared by every body:** id, port names, directions and types, config ports and their defaults.
- **Belongs to one body:** backend, code, what it needs from that backend, and its state.
- A shader has no Rust signature, so the backend's authoring crate supplies its port types. They
  are compared the same way as types read from a function.
- Order and crate don't matter.
- The graph names only the id, or forces a backend. A body whose backend isn't present is
  unavailable, which alone is no error. A node fails only if nothing can take it, and the error
  lists each body and why it was out.
- A node keeps its body while the graph runs. If a reload moves it to another body, its state
  starts fresh and that is reported.
- On a chip, only the chosen body is generated, so extra versions cost no flash.

## Moving data

A node says what it outputs and what it needs. The planner matches the types and puts the right
memory handler between them. It works the same for any program.

```text
node A: I output acme.image, in dmabuf memory
node B: I need acme.image, in GPU memory

planner: same type, different memory
         -> look up the handler for acme.image from dmabuf to GPU, and put it between them
         -> no handler: planning fails, naming both ports and both memories
```

A value has a type key, a memory space and an owner. The core stores and passes these and reads
nothing else. There is no frame or image type in Daedalus. A type belongs to whoever wrote the
nodes that use it, and can be a plain Rust struct.

```rust
#[derive(Type)]
struct Image { fd: OwnedFd, width: u32, height: u32, stride: u32 }

#[node(id = "acme.read", outputs("image"), memory(image = dmabuf))]
fn read(source: &mut Source) -> Image { .. }

// The GPU backend publishes the import. The type's owner only describes the memory.
impl ImportExternal for Image {
    fn describe(&self) -> ExternalImage { .. }
}
```

Writing a handler by hand is one function, for types that need more:

```rust
#[transfer(from = "dmabuf.block", to = "wgpu.buffer", cost = import, backend = daedalus_wgpu_author)]
fn to_gpu(image: &Image, gpu: &GpuCx) -> Texture { gpu.import_external(&image.describe()) }
```

### What it costs

| Between two nodes | At run time |
|---|---|
| Merged by a backend | Nothing. No value exists |
| Same memory, one consumer | Nothing. They share a slot, and ownership moves with no step and no reference count |
| Same memory, several consumers | One retain per extra consumer, once, at the output |
| Different memory, handler imports | One step, which the backend can absorb into its own work. A cached import pays only the first time it sees a buffer |
| Different memory, must copy | A copy. The planner picks this only when no cheaper path exists |

The planner knows every value's consumers before anything runs, so nothing is counted or looked up
per tick. A handler is registered by type, from one memory to another, with a cost class of
`handoff`, `import` or `copy`. The planner picks the lowest-cost path.

### Memory backends

A backend doesn't have to run nodes. A memory backend declares memory spaces and the handlers
between them, and offers no nodes. `backends/dmabuf` is one: it holds blocks of memory with an fd
and a length, and knows nothing about what's stored in them.

## GPU setup

The core never sees a device. The GPU backend either opens its own or is handed one:

```rust
WgpuBackend::open(options)    // finds and opens its own device, on first use
WgpuBackend::adopt(handle)    // is given a device someone else set up
```

The Host only ever receives a finished backend.

## The forms a solved graph takes

| Form | What it is | Used for |
|---|---|---|
| `Steps` | Function pointers and data, in this process | Normal running |
| `Blob` | A stored plan a small runner reads | Loading a graph without recompiling |
| `Source` | Generated Rust, compiled ahead of time | Chips, and the fastest path anywhere |

A form doesn't depend on the machine. A chip is a machine with a small CPU backend and the same
`#[node]`.

```rust
// build.rs
daedalus::plan("graph.json", NODES)?.form(Form::Source).write("graph.rs")?;

// firmware
graph.feed(sample); graph.step(); let out = graph.take();
```

The separate MCU macro, `NodeDesc`, `McuType` and `Scalar` go away. The three runtimes on `dev`
(full, `no_std` tier, MCU profile) become one.

## How it runs

- **Linked:** a Host in your application. `feed`, `step`, `take`.
- **`daedalusd`:** the same Host and code, in its own process. A Host can also open a listener.
- **Plugins:** Rust dylibs through the loader, and any language through the process backend.
  Plugin isolation is a per-node choice: run it in the process backend if you want it isolated.

## Control flow

`if`, `for-each` and `until` regions, and nothing beyond that. Daedalus is not a programming
language. Each one is a region holding a nested step list, and the same region rules apply.

## Failure

Never give up. Backoff is capped at 8 s. There is no automatic turning-off by default. An operator
can turn something off by hand, and automatic off exists only as a policy someone opts into.

A merged region that fails backs off and reports the region, the nodes it covers, and the failing
node if known. It never unmerges by itself. Running unmerged is a manual setting, for finding the
faulty node.

## Budgets

Each of these is measured on `dev` today. `next` must beat or match it, on the same benchmark. A
PR that makes a number worse says so and doesn't merge without a reason.

| Measure | `dev` |
|---|---|
| Per-stage overhead, CM5, unfused | 655 ns |
| Per-stage overhead, CM5, fused | 498 ns |
| 16-stage idle tick | 27,064 instructions |
| Stable-ABI node | about 3,100 instructions per tick |
| Detector mirror graph | about 7 µs per frame |
| 8 GPU ops, RX 6800 XT | 0.37 ms with 8 submits, 0.15 ms with 1 |
| 8 GPU ops, V3D | 1.30 ms with 8 submits, 1.32 ms with 1 |
| Five-node firmware | 3-12 KiB flash, 112-584 B RAM |

The goal is a solved graph that is close to a straight pipe: steps that are merged cost a direct
call, and an idle tick does almost nothing. Steady-state running allocates nothing.

## Boundaries the gate enforces

- `core/*` may not depend on `backends/*`, `languages/*` or `service/*`.
- No type or module in `core/` is named after hardware or media: gpu, wgpu, shader, texture, npu,
  frame, image, pixel, camera, mcu.
- `unsafe` only in `core/abi`, `core/capi`, `core/loader`, `core/runner`, and crates named
  `backends/*-sys` or `languages/*-sys`, with a `SAFETY` comment on each block.
- `core/{abi,types,value,graph,plan,runner}` build for `thumbv7em-none-eabihf`,
  `thumbv6m-none-eabi` and `riscv32imac-unknown-none-elf` with no allocator.
- No dependency on Lemnos anywhere.
- No Rust file over 800 lines.
- No product names outside the planning files.

## Settled details

These were worked out while planning the PRs. Where a brief and this section disagree, this section
wins.

**Handles in CPU nodes.** `Arc<T>` has `T`'s type key, so a parameter or a result written as
`Arc<T>` is a port of type `T`. A CPU node that takes `Arc<T>` gets the cell's handle as it is, and
one that returns `Arc<T>` has it put in the output cell as it is: a value handed on is neither
copied nor put behind a new handle. Taking a handle is taking, so the planner still refuses two
nodes that each take one value.

**The step interface (A3).**
`Step` also carries `release: Option<unsafe extern "C" fn(*mut ())>`, which frees its `data`. It is
`None` for generated code. `Fence` is `{ poll, data }`, and the runner polls it. `StepCx` carries the
fault channel, a cancel word, a progress call and a restart flag. A slot carries a `version: u32`,
which output reuse compares. There is no per-value correlation id or lineage: a value's meaning
belongs to the value.

**What a backend hands back (A7).** `Compiled::Steps` is a list of units, each holding the `Step`, the
nodes it covers, and the boundary ports it reads and writes. The planner checks the region rules
against these. `Blob` and `Source` carry the same information as a covers table.

**Planning.**
- Crossings count distinct (output port, backend) pairs, because that is what stitch has to move.
  A host input or output is a node fixed to a pseudo-backend named `host`, and its edges count.
- The assignment search is exact. Past 1,000,000 expansions per connected component, planning fails
  and suggests forcing a backend. There is no approximate fallback.
- There is no soft preference. A graph forces a backend or leaves the choice to crossings then
  priority. The Host owns the priority list and passes it to the planner.
- **Refusals repeat until none are left.** Each refused (node, backend) pair is removed and
  assignment runs again. This ends because each round removes at least one pair. A node with no
  candidates left fails planning, listing each backend's reason. A refusal by a forced backend fails
  at once.
- A rate change is a declared ratio on a node.
- `until` needs a declared maximum iteration count.
- Overloads are not carried. Nothing writes them on `dev`.
- A FIFO edge with no declared capacity gets 64 slots on a host, set by the planner. Chip builds set
  their own default through planner config.

**Running.** "Running allocates nothing" applies to Daedalus's own code. wgpu allocates inside a
submit, and that count must not grow with the number of nodes in the region. Restart is the
`StepCx` flag. Adaptive serial/parallel selection and configuration from environment variables are
not carried.

**Nodes.** A user type conversion is a one-in, one-out `#[node(conversion)]`. `#[node(shareable)]`
stays, for sharing identical upstream nodes between graphs in one Host. The registry also accepts
owned declarations that a worker sends when it starts.

**Where things live.**
- There is no frame type, no frame library, and no dependency on Lemnos. Device selection is in
  the wgpu backend. Pixel conversions and colorimetry are not carried: they belong to whoever owns
  the type.
- Kernels are `backends/kernels-sys` (SIMD needs `unsafe`), usable by any backend.
- The generic foreign-owner and layout-hash code is `core/value`'s `foreign` module (A14).
- Bytes in graph JSON use a small hex encoder inside `core/graph`. Speed doesn't matter while
  planning, and the worker and service wires are binary.
- `crates/accelerator` is replaced by the Backend interface. There is no CUDA backend on `next`; one
  can be written like any other backend.
- `unsafe` is also allowed in `languages/*-sys`. Unsafe that a macro generates in a user's crate
  doesn't count.
- Heap-free targets also include `thumbv6m-none-eabi`, which has no atomics. The cancel word falls
  back to a plain volatile word there, since those chips are single-core.

**Service.** Listeners are a unix socket, loopback TCP and stdio. Nothing listens on the network.
Runs outlive their connection. A value whose type has no wire codec is refused.

**The foundation as built (A2 to A15).** The code is the reference for these, and later briefs
build on them.

- A body macro is `<path>::__daedalus_body!(kind = fn | const | transfer, name = (<expression>),
  args = (<the backend's own tokens>), params = (<name: Type,>...), ret = <type>, bind = (<see "The planner as built">), decl = <ident>)`
  and expands to a const-evaluable `BodyRecord`. `name` is in parentheses so that a generic instance
  can be `name::<u32>`. The core macro adds `state` and `memory` itself with `BodyRecord::with_node`.
  A `const` body also gets a trailing `init = (<the const's initializer tokens>)`, so an authoring
  macro that has to read the body at expansion (a shader, parsed there) can: the name alone tells a
  macro nothing.
  An authoring crate implements all three kinds; `testing/crates/author-fixture` is the example.
- `PortDecl` and `TypeRecord` hold `TypeKey`s, which have drop glue, so a slice of them is written
  `const X: &[PortDecl] = &[...]` or inline in a struct literal. Passing `&CONST` to a function
  doesn't promote. `#[transfer]` emits a `static` for the same reason.
- There is no `ConversionRecord`. A conversion is a marked one-in, one-out node, and
  `Registry::conversions()` gives B4 the table. Memory handlers are in `Registry::transfers()` and
  `Registry::transfer(key, from, to)`.
- Config types have `FIELDS`, `RULES`, `CHECKERS` and `from_literals`. `ConfigDecl.rules` is what B3
  enforces. A checker is `fn(&Self) -> Result<(), &'static str>`.
- A default key is `rust:<module path>::<name>`, with the name lowercased. Using a type with a
  default key from another crate is a compile error: such a type needs `#[type_key(..)]`.
- Generated code names `::daedalus_registry`, so an author's crate depends on it. C14 makes that
  work through the facade.
- A registry's `std` feature holds everything that allocates. The records build without it.
- `daedalus_abi::testing` (feature `test-run`) runs a step from test code that may not write
  `unsafe`. Only tests turn it on.
- The conformance suite takes a `Scenario` through `run_all_with` or `conformance!(make, scenario)`,
  for a backend whose bodies need a real payload. It installs the counting allocator, so it links
  only into test binaries.
- The sim's `SimConfig` has the knobs planner tests need: `refuse`, `merge`, `form`, `faults`,
  `absorb`, `no_transfers`, `drop_output`, `omit_covers`. A sim body's `text` is its node's id.
  `SimMemory` is the memory-only backend, and it has the absorbable `sim.import`.

**The planner as built (B1 to B15).** `core/planner` is the reference, and `core/planner/README.md`
documents the patch and refresh JSON.

- `plan(graph, registry, backends, config)` takes the registry as an argument. The builder is
  `Planner`. There are no feature flags on the config and no overloads on ports.
- Solve runs expand, defaults, rewrite, hydrate, control, ports, typecheck, consts, convert,
  exclusive, order, rates. A solved node records in `inserted` whether the planner added it, as a
  conversion or as a copy.
- `Backend` gained a defaulted `version()`. It changes whenever what the backend takes or compiles
  may change on this machine, and a cached plan is only reused under the same version.
- The assignment search has a rank lower bound, checked against brute force. Regions are laid out
  in topological order, and `UnitInfo.may_pend` says whether a unit can wait on a fence.
- `#[transfer]` records carry no step of their own: the loader binds them. `ConstOf.value` is a
  `&Lit`, and `Step::discard` frees a step that was compiled and not used.
- `core/graph` grew additive fields: `NodeDoc::new`, `host_inputs`, `sync_groups`, the `inputs` and
  `outputs` lists, and `control`. The schema is regenerated with
  `cargo run -p daedalus-graph --bin graph_schema`.
- The cache key is the input graph's bytes and the settings' bytes, each hashed, and an entry keeps
  the full bytes. The settings cover the config, each backend's id, version, memory and transfers,
  in the order they'll be tried, and everything the registry declares. Priority enters only as
  that order, so a reordering that changes nothing leaves the key alone and one that changes which
  backend comes first changes it. Lints don't count. `PlanCache` is generic, because `Planned` holds
  steps that can only be taken once.
- Sharing takes solved graphs. A node whose output type has no key text isn't shared, because a host
  input couldn't name that type. Sharing is the caller's decision.
- `Planned.warnings` holds what the lints found. `plan_dump` links no nodes or backends, so it plans
  host bridges only. A host with a registry calls `explain_plan`.
- A control node's body takes its ports from its one host bridge, and those ports are held, so a
  node in a body that needs its input exclusively gets a copy and can't take the value it was lent.
  A body with a unit that can wait is a planning error. Control nodes need the steps form, and the
  control backend shares the first host memory space any backend declares.
- `for_each` reads and writes `daedalus_runner::SlotList`, a list of cells of any one type. A
  wrapper for a node that makes or takes a list builds on it (D1).
- A step that returned `Pending` isn't run again until its fence has resolved. It stays dirty
  meanwhile, so being woken any number of times makes one later run. A step therefore has at most
  one piece of work unfinished, and what backs up behind it is its input edges' business.
- A step's `Slots` is its own: the cells for `UnitInfo.reads` in order, then `UnitInfo.writes` in
  order, so index 0 is the first input. A compiled step holds no slot numbers, and the plan's slot
  lists connect those positions to the graph. A port with no edge is `NO_SLOT` in those lists: an
  empty cell for an input, thrown away and released for an output. The cell buffer is scratch sized
  to the step with the most ports.
- A unit must name every boundary port of its region, and may also name ports inside it. A named
  port on an edge inside the region gets a slot in the region's memory, which is how per-node steps
  in one region pass values to each other. A merged unit names only the boundary and takes no slots
  for what it absorbed.
- A body macro also gets `bind = (state = (..), config = (..), fixed = N, params = (..), returns = (..))`
  from `#[node]`. Each parameter is `role position "port" name: Type,`, so a backend's expansion needs
  no parsing of its own. The roles are `read`, `take`, `modify`, each with an `opt_` form, and `state`,
  `config_ref`, `config_val`, `ctx`, `fanin`.
- A CPU value at most 16 bytes long and `Copy` travels inline, in a buffer the step owns until it's
  published. Anything else is an `Arc` written at plan-time-known cost. A body's error is a `FixedError`
  (a code and at most 64 bytes of reason), which is why a `String` error doesn't compile on this path.
- A config's text and lists become `'static` once per plan, because `Literal` holds them that way.
  Replanning a graph with string constants keeps that memory.
- A slot number is reused only when no region is in the steps form. In the steps form a slot is a
  queue that persists across ticks, so a value that waited would collide with the next writer. D3
  must decide this for blobs from how its runner treats a slot. D3's blob keeps its own queue per
  edge, so a plan's slot numbers don't reach the small runner and reuse doesn't touch it.
- A region lists its nodes producers first, and that's the order its steps run in.
- A CPU node joins the node before it only when that node is its only wired input, through an edge
  with one reader and no `Bounded` policy, and neither end sets `daedalus.node.fusion` to false, so
  merged and unmerged runs are the same call for call. A node waiting on two inputs starts a new step,
  and so does the node after one with a second output in use, because a fault drops everything its
  step wrote. Inside a merged step each node wakes the next as separate steps would: a node that
  finishes with nothing to send still runs an optional input after it, and a skip ends the step as
  `Skipped`. Repeated faults back off the merged step as a whole.
- A by-value `Copy` parameter declares `Access::Read`, since it can't take the original away, so
  fan-out of plain numbers needs no copy node.
- A step can't know plan-wide type and space numbers either. A wrapper writes placeholders and the
  runner stamps each output at publish from `Runner::set_stamps`.

**The service as built (H1 to H4).** `core/wire`'s `service` module, `core/client`,
`core/host/src/listen` and `service/daedalusd` are the reference.

- The service protocol shares the worker protocol's packets and value codec and nothing else. Its
  kind bytes are `0x80` and up, and its layout hash covers the worker's. A value carries its own
  type tree, since a service client has no port declarations to type it by. The server's hello
  lists the type keys it has codecs for.
- A `LoadedHost` is one graph, so what sessions share is a `ServiceHost`: the registry, the
  backends, stored graphs by session name, the runs, the plan cache, the watches and the trace.
  Runs and watches are numbers the host gives out. A run is stepped by `step` requests, one tick
  at a time, and has no thread of its own.
- Each session has a reader thread and a writer thread. Answers wait in a queue of 256, and a
  client past that, or one a write has blocked on for 10 s, is disconnected. A watch holds 64
  events and reports `lagged`. Nothing that runs a graph writes to a client.
- A port is fed and taken through a `WireCodec` for its type key. Every scalar has one. A port
  whose type has none is refused with `unsupported`. A number, a bool and unit are fed in place,
  as little-endian bytes, never as a typed payload: a plugin node may forward its input cell,
  which is only sound for a value with no reference count. Text and bytes are fed as typed
  payloads by `WireCodecs::scalars()` and as shared bytes by `WireCodecs::byte_form()`, which the
  daemon uses.
- A node's state is in its step. A reload moves a step into the new runner as it is when it
  covers the same nodes at the same places with the same bodies, constants and ports, and
  compiles every other step again. A node whose step did not move starts fresh and is reported.
  Nodes merged into one step are kept or started fresh together.
- A runner built by `owned_run` owns its steps and releases them when its buffers are dropped,
  which is what lets a plugin's library close once the last run that used it is gone. Its cancel
  word is in an `Arc`, so another thread can hold a `CancelHandle` to it.
- `daedalusd` links no backend of its own. Each backend comes from a plugin, wrapped so the
  planner can use it, and every plugin backend's values live in the one space `plugin.host`.
  Loading a plugin's name again is a new generation whose backend replaces the old one.
- The daemon's default socket is `$RUNTIME_DIRECTORY/daedalusd.sock`, else
  `$XDG_RUNTIME_DIR/daedalusd/daedalusd.sock`, else `/run/daedalusd/daedalusd.sock`. Sockets a
  service manager passes in (`LISTEN_FDS`) are used in place of the configured ones.
