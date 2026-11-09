# Daedalus: the design

This is the design the `next` branch builds. It is the source of truth for every brief in
[`docs/prs/`](prs/). It is written so that an agent with no other context can work from it.

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
core/        abi, types, value, graph, backend, registry, plan, planner, runner, macros, host,
             wire, client, loader, capi, daedalus (the facade)
backends/    cpu, cpu-author, wgpu, wgpu-author, dmabuf, process, sim, and *-sys crates
languages/   python, node, java, cpp, rust: SDKs for writing nodes run by the process backend
service/     daedalusd, daedalus-ctl
testing/     crates/* (conformance suite, benches, budgets), board-runs/, fixtures
examples/    each directory directly inside is a crate
```

Workspace members are globs, so adding a crate never edits the root `Cargo.toml`. Every directory
directly inside those folders must be a crate.

## The flow

1. **Solve.** Expand groups, apply defaults, run rewrites, check ports and overloads, typecheck,
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
#[transfer(from = dmabuf, to = wgpu, cost = import)]
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
