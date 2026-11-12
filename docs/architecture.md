# Daedalus: the architecture walkthrough and the plan

This is the full plan document in text form. [`architecture.html`](architecture.html) is the same
document with its diagrams and every PR on one page: open it in a browser.

[`design.md`](design.md) is the source of truth. This file explains and illustrates it: worked
examples, why decisions went the way they did, where each `dev` crate goes, and how the work is
grouped and ordered. If the two ever disagree, `design.md` wins. The PR list and the briefs are in
[`../TODO.md`](../TODO.md) and [`prs/`](prs/).

Plan for review · read against Daedalus dev 712e7b7 · `next` holds the plan, nothing built on it yet · 2026-11-08 · version 8

Daedalus connects and structures. It solves your graph, and hands the pieces to backends that know how to run them. Each backend can merge and optimize what it's given. Daedalus knows no node and no hardware.

## Who you write against

**You write a node against its backend. The backend talks to Daedalus.** Daedalus defines one backend interface and doesn't know how any backend works. Anyone can write a backend and plug it in, in their own repo, without changing Daedalus.

| Who | Knows | Doesn't know |
|---|---|---|
| **Daedalus** | The graph: types, ports, order, rates, memory locations, who owns a value | What any node does. What any hardware is. How any backend works |
| **A backend** | Its hardware, its nodes' bodies, how to compile and merge them, how to move data in and out | What the rest of the graph is, or what other backends exist |
| **A node** | What it computes, and which backend it's for | The graph it's in, and how it gets called |

The original idea still holds: typed nodes from simple functions, and a planner that proves the graph before anything runs. CPU and GPU stop being special. They become the first two backends.

## The stack

*Diagram (see `docs/architecture.html`): Arrows show who runs what. Source dependencies point the other way: a node depends on its backend's crate, a backend depends on the interface, and Daedalus depends on neither. Your program is what connects them.*

## The backend interface

This is the whole contract between Daedalus and a backend. Names are proposals. A backend answers four questions.

```rust
pub trait Backend: Send + Sync {
    fn id(&self) -> &str;

    /// What memory do I have?          "wgpu:0", "npu:0"
    fn memory(&self) -> Vec<MemorySpace>;

    /// How can data get in and out?    image in dmabuf -> image in wgpu:0, zero copy
    fn transfers(&self) -> Vec<Transfer>;

    /// Where else can my nodes write an output?   a GPU sharing the CPU's memory: dma-heap
    fn placements(&self) -> Vec<Cow<'static, str>>;

    /// Which of these nodes could I run on this machine?
    fn can_take(&self, offer: &Offer) -> Vec<NodeRef>;

    /// Turn the nodes I was given into something runnable. I may merge them.
    fn compile(&self, region: &Region, cx: &CompileCx) -> Result<Compiled, Refusal>;
}

pub enum Compiled {
    Steps(Vec<StepUnit>),         // the normal form
    Source(GeneratedCode),    // when the graph is sealed into a build
    Blob { bytes: Vec<u8>, units: Vec<UnitInfo> }, // when a small device loads a plan
}

#[repr(C)] pub struct Step {
    run: unsafe extern "C" fn(*mut (), *mut Slots, *mut StepCx) -> StepEnd,
    data: *mut (),
    release: Option<unsafe extern "C" fn(*mut ())>,
}
pub enum StepEnd { Done, Pending(Fence), Skipped, Fault }

// Fault carries no data. The step writes an optional failing node and a bounded
// reason through StepCx. The plan already records which nodes each step covers,
// so a report always names them, and reporting allocates nothing.
```

**A backend can be a plugin.** There's one stable interface, so a backend can be linked in or loaded from a file, and it's the same either way. Calling a step through the interface costs a function pointer call in both cases. One plugin can ship a backend and its nodes together.

### What this removes from Daedalus

- `ComputeAffinity` and the per-node match on CPU or GPU, on every tick.
- `Residency` as a fixed enum. A location becomes a memory space a backend registered.
- The planner's GPU pass. Backends claim instead.
- The 63 GPU `cfg` lines in the runtime. A backend is a crate you link or don't.
- The carriers list. Whether code is linked or in a plugin is how it was loaded. Where it runs is the backend.


### Rules the planner enforces

- A region must keep its inputs and outputs. Inside, the backend can do anything.
- A region can't span a rate change. Those are the only places queues remain.
- A region can't leave and re-enter itself, or it couldn't be one step.
- Every compiled step lists the nodes it covers, so errors and telemetry still name nodes.
- No negotiation. A backend that refuses at compile time loses the node, and the planner assigns it again, until nothing is refused.


## The handoff

Step by step: solve the graph, hand the pieces to the backends, let them optimize, and take the result back up.

*Diagram (see `docs/architecture.html`): Each backend only ever sees its own nodes. The next section covers how the planner decides when more than one could take a node.*

### Why merging needs this

The planner groups GPU nodes today, but each one still records and submits on its own. Two different fixes are possible, and only the backend can know which applies:

| 8 small GPU operations | 8 submits | 1 submit | What helps |
|---|---|---|---|
| **RX 6800 XT** | 0.37 ms | 0.15 ms | One submit |
| **V3D (Pi)** | 1.30 ms | 1.32 ms | Only fewer passes: merged shaders |

Shaders can merge when the second reads only the first's result at the same element. If it reads neighbouring elements, merging would run the first shader once per neighbour, so they stay as two passes, sharing one submit.

### When a merged region fails

A merged region is one step, so it fails as one, and the normal failure rule applies: it backs off and keeps trying. Nothing recompiles or unmerges by itself. It does what you told it to, and it reports:

- **Always:** which region failed, and the list of nodes in it.
- **If the backend can tell:** exactly which node. The field is optional, and it's only filled in on the error path, so healthy runs pay nothing. A shader that fails to compile can usually be traced to one node. A GPU fault at run time usually can't.
- **By hand:** you can tell a region to run unmerged to find out. `explain_plan()` names every merge.


## When more than one backend can take a node

This happens when a node has more than one body, such as a blur written for both CPU and GPU. Here's a graph where it matters. `blur` and `sharpen` have both bodies, `detect` is GPU only, and `overlay` is CPU only.

| Option | blur, sharpen | detect | overlay | Backend jumps |
|---|---|---|---|---|
| **A** | GPU | GPU | CPU | **2:** a source to GPU, GPU to CPU |
| **B** | CPU | GPU | CPU | 3: a source to CPU, CPU to GPU, GPU to CPU |

The planner picks A. The rules, in order:

1. **Forced.** A node set to a backend in the graph goes there.
2. **Fewest backend jumps** across the whole graph.
3. **Ties** go to the Host's priority order.


Rule 2 has one consequence to know about. In a graph that's entirely CPU, a `blur` with a GPU body stays on the CPU, because moving it adds two jumps. If you want it on the GPU, you force it. The same graph on a board with no GPU just has fewer options, and needs no changes.

This replaces the old rule that Daedalus never chooses. A jump count is deterministic, so the same graph on the same machine always gets the same plan. The search is exact. Past 1,000,000 expansions per connected component, planning fails and suggests forcing a backend. It never settles for a worse answer.

## Writing a node

The simple case doesn't change at all. You only learn about a backend when you want its hardware.

*Diagram (see `docs/architecture.html`): Only the blue row is common. There is one macro, and the backend setting says who the node is for.*

**One macro for everyone: `#[node]`.** The same ports, ids and state for every backend. `backend = ..` says who it's for, and anything inside the brackets is passed to that backend for it to read. Leave it out and you get the CPU.

### Most nodes: nothing to learn

```rust
#[node(id = "demo.track", inputs("boxes"), outputs("tracks"))]
fn track(boxes: Boxes, state: &mut Tracker) -> Tracks {
    state.update(boxes)
}
```

### A GPU node that can merge

```rust
#[node(id = "demo.normalize", backend = wgpu(element),
       inputs("x"), outputs("y"))]
const NORMALIZE: &str = "
    fn apply(c: vec4f, cfg: Config) -> vec4f {
        return (c - cfg.mean) / cfg.std;
    }";
```

### Hardware Daedalus has never heard of

```rust
#[node(id = "acme.infer",
       backend = acme_npu(model = "net.rknn"),
       inputs("x"), outputs("y"))]
struct Detect;
```

### One node, several backends: same id

```rust
#[node(id = "demo.blur", inputs("x"), outputs("y"))]
fn blur_cpu(x: &Values) -> Values { .. }

#[node(id = "demo.blur", backend = wgpu,
       inputs("x"), outputs("y"))]
const BLUR: &str = include_str!("blur.wgsl");
```

### How two versions register

**The id is the node.** A node's definition has two parts. One is what the graph sees: the id and the ports. The other is how it runs: a backend and the code for it. Each `#[node]` emits one record holding both, and the registry joins records that share an id.

### What the registry ends up holding

```rust
demo.blur
  declaration:  x -> y
  bodies:
    cpu       from crate blur
    wgpu      from crate blur
    acme_npu  from plugin acme_vendor
```

| A record arrives with | What happens |
|---|---|
| **A new id** | Creates the entry, with this declaration and one body |
| **A known id, same declaration, new backend** | Adds a body |
| **A known id, different declaration** | Registration error naming both places and the port that differs |
| **A known id and a backend it already has** | Registration error naming both places |

Order and crate don't matter, because every record has to agree with the same declaration. A vendor can add a faster `demo.blur` for their block from their own plugin without touching yours. This replaces today's `fallback` pairs.

| Shared by every body | Belongs to one body |
|---|---|
| Id, port names, directions and types, config ports and their defaults. The graph is wired against these before anyone knows which body will run | Backend, code, what it needs from that backend (a model file, say), and its state. A GPU body holding a texture and a CPU body holding a buffer have nothing to share |

**Types when there is no Rust signature:** a shader has no function to read. Its backend's authoring crate supplies the types instead (`wgpu(element)` means buffer in, buffer out), and they are compared the same way as types read from a function.

**What the planner does with it:** the graph only ever names `demo.blur`, or forces a backend. Each backend is offered only the nodes it has a body for. A body whose backend isn't present is unavailable, and that alone is no error. The node fails only if nothing can take it, and the message lists each body and why it was out. A node keeps its body while the graph runs. If a reload moves it to a different body, its state starts fresh and that is reported.

**On a chip:** records are constant data, and the same check runs in `build.rs`. Only the body that gets chosen is generated into the firmware, so extra versions cost no flash.

**How one macro stays common:** `#[node]` reads the id, ports and state. For the body, it expands to a call into whichever backend is named, so the macro itself knows no backend and a new backend needs no change to it. A backend ships a small authoring crate for that, separate from the part that runs. If the backend isn't there when the graph starts, the planner says so by name.

### Three levels of GPU node

| You write | The backend can |
|---|---|
| **An element-wise function** | Merge it into a shader with its neighbours |
| **A whole shader** | Share a submit, but not merge it |
| **Your own recording code** | Hand you the encoder, as `encode_bound` does today. You control everything |

A shader is data, so it can cross any boundary. A Python worker could supply an element-wise shader, and it would merge like any other.

## On a regular Linux device

Say a board with a GPU and an NPU, running resize, normalize, detect, track.

### Connecting it up

```rust
let host = Host::new()
    .backend(WgpuBackend::open(Default::default())?)
    .backend(AcmeNpu::open()?)
    .backend(Cpu::new());       // priority: breaks ties

host.load_plugin("vision.so")?;
let graph = host.start("cam", document)?;
```

In a daemon, the same list comes from a config file. A backend can also be loaded from a file.

### What `start` does

1. The planner solves types, conversions and order.
2. Each backend says which nodes it could run, and the planner assigns them with the fewest jumps.
3. Each one compiles. wgpu merges two shaders into one.
4. The planner adds transfers and fences from the table.
5. The result is a flat list of steps, cached, and the graph starts.


### Tick by tick

- A CPU step is a function pointer; the plan's unit lists say which slots it reads and writes.
- An edge inside a single-rate chain is a slot, a straight pipe.
- Telemetry is compiled into the list, or absent.
- A failing node's step is swapped for a backoff step, so healthy nodes pay nothing for the check.


### When you change the graph

- Only the regions the patch touches are compiled again.
- Nodes keep their state by id.
- If the new graph isn't valid, the planner says why and the old one keeps running.


## On a microcontroller

Today's MCU side is carved out: its own node macro, its own value and type system, its own build step, and only the planner in common. In this design it stops being a separate thing. The main system grows to cover it. A microcontroller is a machine with a small CPU backend.

*Diagram (see `docs/architecture.html`): The dashed box isn't built. It's what the backend interface allows: a chip's PIO or DMA block could take nodes like any other backend.*

### The same node, no special macro

```rust
#[node(id = "shared:scale",
       inputs("raw", "volts_per_count"), outputs("volts"))]
pub fn scale(raw: f32, volts_per_count: f32) -> f32 {
    raw * volts_per_count
}
```

That one function runs in a Linux graph or on a chip. If a node uses something a chip doesn't have, such as a heap, it fails to build for the chip.

### The same graph, planned at build

```rust
// build.rs
daedalus::plan("graph.json", NODES)?
    .form(Form::Source)
    .write("graph.rs")?;

// firmware: the same verbs as a Host
graph.feed(sample);
graph.step();
let volts = graph.take();
```

### How it fits: machine and form are separate choices

The form of a compiled graph doesn't depend on the machine. These are the same three forms everywhere, and any machine can use any form it can fit:

| Form | What it is | Linux device | Microcontroller |
|---|---|---|---|
| **Steps** | A list made when the graph starts. It can change while running | The default | If it has room |
| **Blob** | The same list, planned elsewhere and loaded | Possible | Today's "Loaded": wiring changes with no new firmware |
| **Source** | Generated code built into the program. Zero overhead, can't change | A sealed product graph | Today's "Compiled" |

### What has to grow

- **The core has to work without a heap.** Types, ports and the step list can't assume `Arc` or `Any`. Typed slots already move it this way.
- **`#[node]` becomes the only macro.** `#[daedalus_mcu::node]`, `NodeDesc`, `McuType` and the separate `Scalar` values go away.
- **Three runtimes become one.** The full runtime, the `no_std` tier and the MCU profile collapse into one runner, with capacities fixed at plan time on a chip.
- **It has to stay tiny:** today that's 2,128 to 2,672 B of flash and 112 B of RAM for five nodes, pinned in
  `scripts/ci.d/chip.sh`, under gate limits of 12,288 B flash and 584 B RAM. This must not grow.


## Everything connected

Press a button to follow one kind of connection.

*Diagram (see `docs/architecture.html`): The pink line on the right is the handoff: offers go down to the backends, and compiled regions come back up. Only the top two rows differ between linked and daemon use.*

## How data moves

**A node says what it outputs and what it needs. The planner puts the right memory handler between them.** It works the same for any program. There is no frame or image type in Daedalus, and Lemnos isn't involved.

Daedalus needs three facts about any value:

- **What it is:** a type key. Daedalus only compares them. The type belongs to whoever wrote the nodes, and can be a plain Rust struct.
- **Where it is:** a memory space some backend registered.
- **Who owns it:** so it can be handed over without a copy.


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

### What it costs

| Between two nodes | At run time |
|---|---|
| **Merged by a backend** | Nothing. No value exists |
| **Same memory, one consumer** | Nothing. They share a slot, and ownership moves with no step and no reference count |
| **Same memory, several consumers** | One retain per extra consumer, once, at the output |
| **Different memory, handler imports** | One step, which the backend can absorb into its own work. A cached import pays only the first time it sees a buffer |
| **Different memory, must copy** | A copy. The planner picks this only when no cheaper path exists |

The planner knows every value's consumers before anything runs, so nothing is counted or looked up per tick.

### Memory backends

A backend doesn't have to run nodes. A memory backend declares memory spaces and the handlers between them, and offers no nodes. There is no production memory-only backend on `next`: `backends/dmabuf` was removed in #194.

### GPU setup

The core never sees a device. The GPU backend either opens its own with `WgpuBackend::open`, or is handed one with `WgpuBackend::adopt`. The Host only ever receives a finished backend.

## Branches and loops

This is where it could turn into a programming language, so it has a hard limit. The real logic stays inside nodes. The graph gets at most three structures, each a region that runs some number of times, and a node's output decides how many:

| Structure | Region runs | Decided by |
|---|---|---|
| **If** | 0 or 1 times | A boolean a node produced |
| **For each** | Once per item | A list a node produced |
| **Until** | Until told to stop | A boolean a node produced, with a limit |

There are no variables and no expressions, because those belong in nodes. A backend can take a whole region if it understands it, so a GPU could run a for-each as one dispatch. Otherwise the region just becomes a jump in the step list. Today's `Option<T>` outputs and nested groups keep working.

## In process and external

A Host is linked into your program, or held by `daedalusd`. Those are the two forms. Any Host can open a listener for tools, and `daedalusd` is simply a program that does nothing else.

*Diagram (see `docs/architecture.html`): The blue rows are the same source in every column.*

- **The daemon adds** a listener, a journal of what should be running, and graphs that outlive their clients.
- **Safety:** a daemon that loads plugins gives code execution to anyone who reaches it, so it listens only on a unix socket, loopback TCP or stdio. Nothing listens on the network.
- **Plugin isolation is now a per-node choice:** run a node under the process backend and a crash takes down only that node's process.


## Project structure

```rust
daedalus/
  core/
    abi/          Step, StepEnd, Slots, StepCx, Fence, handshake, layout hash (no heap)
    types/        type keys, type expressions, ids, errors (no heap)
    value/        value, memory space, owner, slots, foreign owners (no heap)
    graph/        the graph model and JSON document (no heap)
    backend/      the one interface
    registry/     records, joining bodies, conversions
    macros/       the one #[node]
    plan/         step list format and Blob encoding (no heap)
    planner/      solve, ask, assign, compile, stitch
    runner/       walks the step list: slots, queues, state, failure rule (no heap)
    host/         backend set, plan cache, instances, listeners
    wire/  client/  loader/  capi/
    daedalus/     the Rust facade, re-exports only
  backends/
    cpu/  cpu-author/     typed calls, Blob and Source forms, chips
    wgpu/  wgpu-author/   shader merging, one submit
    process/              the worker protocol and its host
    sim/                  a fake backend for testing the core
    vk-sys/ and other *-sys crates: the unsafe parts
  languages/
    python/  node/  java/  cpp/  rust/  SDKs for writing nodes run by the process backend
    rust-capi/  rust-capi-sys/  a safe Rust host over the C surface, and its loader
  service/
    daedalusd/  daedalus-ctl/
  testing/
    crates/*      conformance suite, benches, budgets
    board-runs/   checks a local session runs on real boards
  examples/

outside this repo:
  NPU and fixed-block backends, node packs
```

## Where each crate goes

| Today | Goes to |
|---|---|
| `core`, `data` | `core/types`, `core/graph`, `core/value` |
| `registry` | `core/registry` |
| `transport` | `core/value` and `core/wire`. `Residency` becomes memory spaces. The frame interface goes away |
| `planner` | `core/planner`. The GPU pass and host-bridge knowledge go. Lowerings become the rewrite hook (B5) |
| `runtime` | The interpreter loop is replaced by `core/runner`. Plugin loading goes to `core/loader` |
| `engine` | `core/host`. Its 11 run forms and 9 compile forms collapse |
| `gpu` | `backends/wgpu`, and it gains merging |
| `wgsl-infer` | `backends/wgpu-author`, using naga when the macro expands |
| `kernels`, `accel` | Gone. Nothing in Daedalus calls a byte kernel, so there is no library of them and no matrix |
| `macros` | `core/macros`, the one `#[node]` and `#[transfer]`; `core/derive`, `NodeConfig` and `Type` |
| `ffi/core`, `ffi/host` | `backends/process`, and workers finally join graphs |
| `ffi/rust`, `python`, `node`, `java`, `cpp` | `languages/*` |
| `daedalus` | `core/daedalus`, re-exports only |
| `daedalus/dylib` | `core/loader` |
| `mcu/*` | Folded in. Its code generator goes to `backends/cpu`, and its macro, types and values are removed in favour of the main ones |
| `surface`, `host-wrapper` | `core/capi` and `languages/rust-capi` |
| `daemon` | `service/daedalusd`, `service/daedalus-ctl`, `core/client`, `core/wire` |
| `bench-counters`, `frame-bench` | `testing/crates` (I2, I3) |
| `nodes` | Dropped. Examples replace it |
| `accelerator` | Replaced by the Backend interface: an NPU is a backend |
| `StreamGraph`, `GpuContext` | Removed |

## Where this stands

| Settled | Answer |
|---|---|
| **Backends as plugins** | Yes. One stable interface, linked or loaded |
| **More than one backend** | Forced, then fewest jumps, then priority order |
| **A merged region failing** | Backs off and reports the region. Never unmerges by itself. Unmerged mode is by hand |
| **Branches and loops** | If, for-each and until |
| **Lemnos** | Not part of this design. Nothing depends on it |
| **Plugin isolation** | A per-node choice. Run a node under the process backend if you want it isolated |

### What we'll have to measure

Nothing here needs a decision. These are the claims on this page that only a real build can prove:

- That the step list is as fast as today's fastest path.
- How much merged shaders save on a Pi's GPU.
- That the chip builds stay as small as they are now.


The page comes from reading the code. None of this design has been compiled yet.

## After the cleanup

| Asked for | Result |
|---|---|
| **Cancel and progress** | Stopped where it was. It's designed fresh as PR C7 on the step list. The Rust SDK follow-up went with it. |
| **Branches** | `main`, `dev` and `next` are all that's on GitHub. 19 remote branches, 37 local branches and 43 worktrees are gone. |
| **Anything lost** | Each one was merged, or its work reached dev under another commit. Three things never landed: the cold-path outlining, the 128-byte payload, and the uncommitted resident-schema work. They are bundled with the stopped cancel commit in `/mnt/nvme-wd/daedalus-pruned-2026-11-08`, along with every branch tip. |
| **Other agents** | The `nostd` session and the ArcValue fix session have nothing unlanded. |
| **To-do list** | dev's `TODO.md` went from 615 lines to 24. Of 173 items, 145 were already done, 11 are dropped, and 17 are open: 8 belong to PRs here and 9 are board runs. |

## The clean cut

- **A fresh start.** `next` began empty. Nothing comes across by default, and each thing is brought in by the PR that needs it.
- **`dev` is the reference and stays untouched.** Code that took real hardware or careful measurement to get right is ported with its tests: WGSL details, driver workarounds, ISA kernels. Everything else is written for the new design.
- **Only Daedalus matters here.** Breaking other repositories doesn't count against a PR. They stay on the `dev` they're locked to, then read `next` and send requests.
- **No Lemnos dependency.** The gate fails on one.
**Status:** group A is merged. Groups B and C are being built in local sessions, one owner per group, and nothing runs in the cloud until the user says so. The contracts as built are in `docs/design.md`, under "The foundation as built".

- **Work is a pre-planned list of PRs into `next`,** each specific enough that a cloud session can take it with no other context.
- **The cloud builds, and a local session runs the boards.** A PR is never held back for a board run.
- **Budgets are enforced.** dev's numbers are the limit. What GitHub's runners can measure is checked in CI (instruction counts under callgrind, allocations, flash and RAM). Real timings are board runs.


## Settled while planning

Working through dev's code raised these. Each is decided, and it's written into `docs/design.md`, which wins over any brief.

| Question that came up | Settled |
|---|---|
| A backend refuses a node, then the next one does too | Refusals repeat until none are left, and each removes a (node, backend) pair. The planner's first draft allowed one retry, which fails with three backends. |
| How is a crossing counted? | Per (output port, backend), because that's what has to be moved. One output going to three GPU nodes is one crossing. |
| The assignment search gets huge | It's exact. Past a limit, planning fails and suggests forcing a backend. It doesn't silently settle for a worse answer. |
| Is there a soft preference, like dev's `GpuPreferred`? | No. Force a backend, or let crossings and then the Host's priority order decide. |
| Can `until` run forever? | No, it needs a declared maximum. |
| An edge with no capacity (dev's is unbounded) | 4 slots from the planner, 64 from the host's loader. Chips set their own default. Running can't allocate, so unbounded can't exist. |
| wgpu allocates about 29 times per submit | "Running allocates nothing" is about our code. wgpu's count must not grow with the node count in a region. |
| What is a frame? | Nothing, to Daedalus. A node declares its types, and a memory handler moves them. The type belongs to whoever wrote the nodes. Neither Daedalus nor Lemnos defines one. |
| GPU setup | The GPU backend: `open` finds its own device, `adopt` is handed one. The core never sees a device. |
| How does a plugin's step data get freed? | `Step` has its own `release`. |
| Bytes in graph JSON, when the core can't import the kernels | A tiny hex encoder in `core/graph`. Speed doesn't matter while planning. |
| Chips with no atomics (the RP2040) | `thumbv6m` is a checked target. The cancel word is a plain volatile word there. |

## What doesn't come across

Everything else dev does has a PR. I6 keeps the checklist, and the last PR walks it.

| Not coming across | Why |
|---|---|
| CUDA backend and its interop | Vendor runtime, and the design doesn't need it built in. Anyone can write it as a backend. |
| Overloads, node feature flags, `ComputeAffinity` | Nothing writes overloads on dev. The others are replaced by bodies and forced placement. |
| `crates/accelerator`, `crates/wgsl-infer` | Replaced by the Backend interface, and by naga when the macro expands. |
| Handler tables, fused and direct-slot routes | The step list replaces them. |
| Stable-ABI versions 4 to 7, frame v1 and v2, the Rust-ABI install path | One ABI on `next`, with no compatibility layer. |
| The separate MCU macro and its types | A chip uses the same `#[node]`. |
| Adaptive serial or parallel, config from environment variables | Small features the design doesn't need. |
| Plugin package integrity and lockfiles | No place in the design yet. |
| Frames, colorimetry, pixel conversions | Daedalus has no frame type. These belong to whoever owns the type. |
| Raw Vulkan handles for plugins | Raw access stays inside `backends/vk-sys`. |

## Rules for every cloud session

- Base branch `next`, working branch `pr/<id>`, one PR into `next`. Start from `docs/prs/<id>.md` and `docs/design.md`. `python3 scripts/pr-status.py` shows what's ready.
- Read `dev` with `git show origin/dev:path`. Bring in only what the PR lists.
- Stay inside the paths the PR creates. No PR edits the root `Cargo.toml`, `TODO.md` or a changelog, and `Cargo.lock` is ignored until the last PR. That's what lets dozens run at once.
- A PR that needs a shared interface changed stops and says so. Those change through A-series PRs only.
- A PR merges when `scripts/gate.sh` passes, and its description pastes the proof.
- Hardware that can't be reached from the cloud gets a record at `testing/board-runs/<id>.md` whose first line is a `Status:` line.
- Failure rule everywhere. No estimates. No version bumps. No tags. No force-push.


## The groups

110 PRs. 23 of them have a board check that a local session runs afterwards.

| Group | PRs | Board runs | Builds on | Waves |
|---|---|---|---|---|
| **A. Foundation** | 15 | 0 | nothing | 1 to 7 |
| **B. Planner** | 15 | 0 | A | 6 to 13 |
| **C. Runner and Host** | 23 | 3 | A, B | 5 to 15 |
| **D. CPU backend and chips** | 16 | 4 | A, B, C | 2 to 19 |
| **E. GPU backend** | 17 | 15 | A, B, C, D | 5 to 16 |
| **G. Plugins and languages** | 12 | 2 | A, C | 4 to 16 |
| **H. Service** | 6 | 0 | A, C, G | 5 to 17 |
| **I. Proof and finish** | 8 | 2 | A, B, C, D, E, G, H | 1 to 20 |

## What can run at once

Worked out from each PR's needs. A PR can start when everything it needs has merged, and within a wave everything can run in parallel. The narrow middle is the planner, which is one chain by nature.

| Wave | Start together | PRs |
|---|---|---|
| **1** | 2 | A1 I6 |
| **2** | 5 | A2 A3 A4 D8 D11 |
| **3** | 3 | A5 A6 D14 |
| **4** | 4 | A7 A10 A14 G1 |
| **5** | 5 | A8 A11 C1 E1 H1 |
| **6** | 14 | A9 A12 B1 B14 B15 C2 C3 C5 C9 C11 C18 E2 E13 H2 |
| **7** | 17 | A13 A15 B2 B5 C4 C6 C7 C8 C13 C15 C20 C22 C24 E4 E9 G2 G4 |
| **8** | 16 | B3 B4 C16 C17 E7 E8 E14 E17 G3 G5 G6 G7 G8 G9 G12 I1 |
| **9** | 3 | B6 E11 E15 |
| **10** | 1 | B7 |
| **11** | 1 | B8 |
| **12** | 3 | B9 C12 E5 |
| **13** | 5 | B10 B11 B12 B13 E6 |
| **14** | 4 | C10 D0 D1 E12 |
| **15** | 11 | C14 C21 C23 D2 D3 D4 D9 D10 E3 G10 H3 |
| **16** | 6 | D5 E10 G11 H4 I4 I5 |
| **17** | 6 | D6 D13 H5 H6 I2 I8 |
| **18** | 2 | D7 I3 |
| **19** | 1 | D12 |
| **20** | 1 | I7 |
