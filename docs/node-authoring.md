# Writing a node

A node is a function with `#[node]` on it. The macro reads the node's id, its ports and their
types from the function, and emits one constant record, `<NAME>_NODE`, that a registry takes.

```rust
use daedalus::prelude::*;

#[node(id = "demo.scale", inputs(x, factor), outputs(y))]
fn scale(x: f64, factor: f64) -> f64 {
    x * factor
}
```

A crate that writes CPU nodes depends on `daedalus`, on `daedalus-cpu-author` and on
`daedalus-registry`: the code the macro generates names the last two.
[`examples/01_basics`](../examples/01_basics) is a complete program.

## Ports

- `inputs(..)` and `outputs(..)` name the ports. Each input is a parameter of that name, in
  order. The outputs are the return value: one output for a plain value, and a tuple for several.
- A port's type is the Rust type. The planner compares types by their key, and refuses an edge
  whose ends differ unless a conversion node exists between them.
- How a parameter is written says how the node uses the value:

| Parameter | Meaning |
|---|---|
| `x: T` | The node takes the value. A `Copy` type is read, since taking it changes nothing for others. |
| `x: &T` | The node reads it. Any number of readers share one value. |
| `x: &[T]` | The node reads a list. The port's type is a list of `T`, held as a `Vec<T>`. |
| `x: &str` | The node reads text, held as a `String`. |
| `x: Arc<T>` | The node takes the value's handle. The port's type is `T`. Returning the same `Arc<T>` hands the value on with no copy and no allocation. |
| `x: &mut T` | The node changes it in place. The planner gives it a copy if someone else also reads it. |
| `x: Option<T>` | The port is optional: the node runs with or without it. |
| `items: FanIn<'_, T>` | Any number of edges, on ports `items0`, `items1` and so on, in number order. |

- Returning `Option<T>` sends nothing on a tick where it is `None`.
- An `Option<T>` input is `Some` when its edge holds a value, and an edge is a queue that keeps a
  value until the node reads it. So it can deliver a value that was sent on an earlier tick, when
  the node did not run on that tick: `None` does not mean "not sent this tick". Put the
  `per_tick` policy on the edge to make it mean that (see
  [host.md](host.md#queues-and-policies)).
- Returning `Result<T, Fault>` reports a failure. See [Failure](#failure).

## Bytes a plugin handed over

A plugin's node can hand bytes to the host without a copy (see [plugins.md](plugins.md#handing-bytes-over)).
A node that takes them receives a `SharedBytes`, the one type for bytes with an owner:

- `x: SharedBytes` takes the handle by value. Taking it moves the count the cell holds, so the bytes
  are not copied and nothing is allocated. Returning the same `SharedBytes` hands the bytes on
  unchanged.
- `x: Vec<u8>` copies the bytes out, and the copy is counted in `bytes_copied`. A `Vec<u8>` owns
  its memory, so a copy is the one thing it can't avoid. Expect it, and take a `SharedBytes` when
  the bytes only pass through.

## Value types

A scalar (`bool`, the integers, `f32`, `f64`, `String`, `Vec<u8>`, `()`) has a type key already.
A type made of others needs nothing declared: `Vec<T>` is a list of `T`, `Option<T>` a value that
may be missing, `(A, B)` up to four a tuple, and `BTreeMap<K, V>` a map, for any `T` that has a
type. Two such types are the same when they are made of the same things, so a `Vec<f32>` output
connects to a `&[f32]` input and to nothing else. `Vec<u8>` is bytes, not a list. In a graph
document a host port names one as the planner prints it: `list<f32>`, `optional<u32>`,
`(u32, f32)`, `map<string, i64>`. One named output that returns a tuple carries the tuple; several
named outputs take one part each.
A type of your own gets one with `#[derive(Type)]`:

```rust
#[derive(Type)]
#[type_key("acme:point")]
struct Point { x: u32, y: u32 }
```

Without `#[type_key]` the key is `rust:<module path>::<name>`, with the name lowercased, which only the crate
that defines the type may use. Daedalus has no built-in value types beyond the scalars: a type
belongs to whoever wrote the nodes that use it.

With `#[structural]` instead, the type has no key and is compared by its fields, in order, the way
`Vec<T>` and tuples are. A worker's port of the same fields then connects to it.

A value of 16 bytes or less that is `Copy` travels in place. Anything else travels behind a
handle and is never copied on the way. An output that is not plain data allocates nothing once
the graph is warm when the nodes that consume it are on the CPU backend, and it reads or takes the
value the way the graph does. Its memory is kept from one tick to the next and refilled. A node
that is read by the host, or by another backend, gets a new handle each tick, one allocation.
A node that takes `Arc<T>` and returns it, or returns an `Arc<T>` it keeps, allocates nothing. To
change a large value in place and hand it on, take `Arc<T>` and use `Arc::make_mut`, which copies
only when someone else still holds the value.

## Values that cross to another process

A node in a worker process (see [ffi.md](ffi.md)) gets its inputs as bytes: the wire form of the
port's type. The type a port declares decides two things: whether a large value can go by
reference, read where the host put it, and how a node reads it there. That is decided by the
type's shape and the value's size, never by its name.

- **A structure.** A port declared as what the value is made of (fixed scalars, structs of them, a
  list) has a layout the worker can read in place. A list is a `u32` count and then its items,
  packed and little-endian. A struct is its fields in order, with no padding. Each port's layout
  comes from its type (`WireType::layout()`):
  - `Fixed`: every part has a fixed size.
  - `Array`: fixed fields, then one list of fixed-size items. The host puts the items on a 64-byte
    boundary.
  - `Walkable`: several lists or strings.
  - `Decoded`: maps and opaque types.
- **An opaque type.** A named type with no structure declared (`Opaque`, with an inspect hook for
  tools) crosses only as the bytes its codec writes. The host places them like any other bytes, but
  a reader in another language can't make sense of them without the codec. A typed value with no
  bytes at all is refused before anything crosses, and the refusal names the port and the type.

So a type meant to cross a process is declared as its structure. Three shapes that Opaque types
carry today, declared as structure instead:

```rust
// Keypoints: a list of 28-byte records. Layout: Array { offset: 4, stride: 28 }.
struct KeyPoint { x: f32, y: f32, size: f32, angle: f32, response: f32, octave: i32, class: i32 }
// port type: Vec<KeyPoint>

// A label image: width and height, then one label per pixel. Array { offset: 12, stride: 4 }.
struct LabelImage { width: u32, height: u32, labels: Vec<u32> }

// A tensor: element type, shape, then the data. Two lists, so Walkable: a reader finds the data
// after the shape, still without a copy.
struct Tensor { dtype: u8, shape: Vec<u64>, data: Vec<u8> }
```

A struct whose one list comes last is an `Array`. Put the fixed fields first. Prefer one list of
records over several parallel lists.

A type's owner, the crate or repository that declares a type other graphs use, decides how cheaply
it crosses. Daedalus doesn't change anyone's types. The guidance for an owner is:

- Declare the type as its structure, with `#[derive(Type, WireForm)]` and `#[structural]`, not as
  `Opaque` bytes from a codec.
- Make records fixed-size structs of numbers, so a `&[T]` in a worker and a `ListView` on the CPU
  read them where they are.
- Put a struct's one list last, after its fixed fields.
- Treat a change to a published type's fields as a new type: a reader compares the type by its
  fields, so the producer and every reader change together.

In the Rust SDK:
- A `&[f32]` parameter (or `&[u32]`, `&[i64]` or any other number) borrows a placed list's items
  where they are, when they are aligned and the machine is little-endian. Otherwise it decodes
  them.
- A `&[T]` parameter of a record type that implements `Flat` (its `SIZE` and how its bytes read)
  reads every record straight from the placed bytes, with no decoded value in between.
- A hand-written body reads a list of records, or a struct that ends in one, through
  `input_view::<T>()`. That gives a `View` with `len`, `get`, `iter` and `header`.

A large output of any type with a form is written once into the worker's value buffer, with its
items aligned, and the host's cell holds it there.

A CPU node's value reaches a worker only as its wire form. Give the type `#[derive(WireForm)]`
next to `#[derive(Type)]`: the CPU step then writes an output a worker reads, and reads an input a
worker wrote, field by field in the order the type declares. Numbers, `bool`, `String`, `Vec`,
`Option`, `BTreeMap`, tuples and `Arc` already have one. A type without one that a graph sends to a
worker, or reads from one, is refused when the plan is built, and the refusal says which type to
derive it on. A CPU parameter `&[f32]` (or any other number) reads a list from a worker where its
bytes are. A `ListView<'_, T>` parameter reads a list of records whose wire form has a fixed size
(`#[structural]` structs of numbers, such as a keypoint) where its bytes are too, one record at a
time: `len`, `get(i)` and `iter()`, with no decode and no allocation. Any other type is decoded once
per tick, and `explain_plan` lists each such decode.

A `&[T]` of records is decoded instead: viewing records as a slice needs their memory to be their
wire form and every bit pattern to be a valid value, which only `unsafe` code could promise for a
type a node author declares. `ListView` needs neither, and reads a list held as a Rust value just
as well, so a node written with it is right wherever the plan puts its input.

A CPU node can write a list output in place with an `OutView<'_, T>` parameter, instead of
returning a `Vec<T>`. The parameter is an output port named after it, of type `Vec<T>`, listed
after the outputs the function returns. Where it is written depends on the plan, not the node:

- Read by one worker that lends memory: into the worker's value buffer, where it reads it. Nothing
  is copied and nothing is allocated.
- Read by a worker that lends none: into memory the step keeps from tick to tick, as wire form.
- Read by CPU nodes alone: into the list itself, one the step keeps and refills once no reader
  holds it.

```rust
#[node(id = "demo.ramp", inputs(n))]
fn ramp(n: u32, mut out: OutView<'_, f32>) -> Result<(), Refused> {
    for (item, at) in out.items_mut(n as usize)?.iter_mut().zip(0..n) {
        *item = at as f32;
    }
    Ok(())
}

#[node(id = "demo.points", inputs(n))]
fn points(n: u32, mut out: OutView<'_, KeyPoint>) -> Result<(), Refused> {
    out.reserve_bytes(n as usize * 28)?;
    out.extend((0..n).map(|at| KeyPoint { x: at as f32, ..KeyPoint::default() }))
}
```

- `items_mut(n)` gives `n` numbers to write in place, for a list of numbers.
- `reserve(n)` reserves room for `n` items of a fixed size; `reserve_bytes(b)` reserves `b` bytes.
- `push(&item)` and `extend(items)` write after the items written so far, through the type's wire
  form: its size pass first, then its write.
- A node that doesn't know `n` reserves again as it goes. In a worker's lent memory the room grows
  in place when the memory after it is free, and otherwise the write is refused. Elsewhere it grows
  as a list does.

On a CM5 this took a 1 MiB list's hand-over to a worker from 0.238 to 0.054 ms/MB, and the
allocations per tick from 1 to 0: [testing/board-runs/out-view.md](../testing/board-runs/out-view.md).

A write past what was reserved is refused, with how many bytes past and what to do. So is an item
whose write doesn't match its size pass. A refusal is the node's failure: the room it held is
released, and nothing is handed on.

A worker's port declares its type's structure, so the CPU type is compared by its fields too:
`#[structural]` says so.

```rust
#[derive(Clone, Type, WireForm)]
#[structural]
struct KeyPoint { x: f32, y: f32, size: f32, angle: f32, response: f32, octave: i32, class: i32 }
```

## Constants and config

An input that no edge feeds can be set by the graph as a constant:

```json
{"id": "demo.scale", "consts": {"factor": {"type": "float", "value": 3.0}}}
```

A constant can be a number, a bool, text, bytes, a list or a struct. A list fills a `Vec<T>`, a
slice parameter or an array of exactly its length, and lists nest. A struct fills a type that
derives `FromLiteral`: its fields are read by name, every field must be given, and the constant
may name no other.

```rust
#[derive(Type, FromLiteral)]
struct Gains { low: u32, high: u32, taps: Vec<f32> }

#[node(id = "demo.shape", inputs(gains), outputs(out))]
fn shape(gains: &Gains) -> u32 { .. }
```

A constant that doesn't fit its port fails planning, or the node's setup, and names the port. It is
decoded once when the graph is planned, so running the graph allocates nothing for it.

Several settings with defaults and limits are a config struct:

```rust
#[derive(Clone, Default, NodeConfig, Type)]
#[type_key("demo:range")]
struct Range {
    #[port(default = 0, min = -1000, max = 1000)]
    low: i64,
    #[port(default = 100)]
    high: i64,
}

#[node(id = "demo.clamp", inputs(x, config = Range), outputs(y))]
fn clamp(x: f64, range: &Range) -> f64 { x.clamp(range.low as f64, range.high as f64) }
```

The graph sets its fields as one struct constant on the port `config`, or one constant a field,
named after the field; a group exposes a stage's field the same way, as an input that maps to it.
The planner folds the fields set one by one into the struct, so a backend reads one form, and
refuses a field set both ways. A name that is an input port stays the port. A field it leaves out
keeps its default. A value outside a field's limits, or of the wrong type, fails planning, and a
constant that names neither a port nor a field is refused with the config's field names listed.

## State

```rust
#[derive(Default)]
struct Total(i64);

#[node(id = "demo.total", inputs(x), outputs(sum), state(Total))]
fn total(x: i64, state: &mut Total) -> i64 { state.0 += x; state.0 }
```

State belongs to one node in one graph and lives between ticks. A reload keeps it when the
node's step did not change: see [host.md](host.md#reload).

## The context

A parameter of type `&Ctx` (from `daedalus_cpu_author`) gives the tick's clock, and lets a long
body ask whether a cancel was requested and report progress.

## Limits and steps

A port can say what its value may be and how a control for it should step:

```rust
#[node(id = "pnp.square",
       inputs(port(name = "side", min = 0.000001, max = 1000.0, step = 0.0001, unit = "m"), corners))]
```

A config field takes the same words in its `#[port(..)]`. Either way they are the port's rule
(`PortDecl::rule`, a `PortRule`): `min`, `max`, `step`, `odd` and `policy`, each optional. A host
reads the rule where it reads the port's label and unit, so a node editor and a check read the
same facts.

- `min`, `max` and `odd` are limits on the value. They are recorded; nothing checks a value
  against them yet.
- `step` is a hint for a control. It is never a limit: a value need not be a multiple of it.

## Metrics

The host times every node and counts what it publishes (see [host.md](host.md)). When a body has
stages of its own, start by asking whether they should be nodes: stages that hand each other
values the graph can own are better as a group of real nodes, because then per-node timing, edge
policies, placement and switching a node off all apply to each stage. A body keeps its stages to
itself when they share borrowed scratch or state inside one call, when a stage is a loop body or
runs a number of times the data decides, when a stage is so short that a step of its own would
distort what it measures, or when the number is not a time at all (candidates found, bits
corrected).

For those, a node declares the numbers its body reports about its own work:

```rust
#[node(id = "detect", inputs(frame), outputs(tags),
       metrics(threshold = "ns", decode = "ns", candidates = "count"))]
```

Each metric has a name and a unit, `"ns"` for a duration in nanoseconds or `"count"`. They are
part of the node's declaration and of the plan, so a host lists a graph's metrics without running
it (`LoadedHost::metrics`).

The declaration also makes an enum of the node's metrics, named after the function: `detect`
gets `DetectMetric::Threshold`, `DetectMetric::Decode` and `DetectMetric::Candidates`. The body
reports through its `&Ctx`:

```rust
fn detect(frame: &Frame, ctx: &Ctx) -> Tags {
    let mask = ctx.timed(DetectMetric::Threshold, || threshold(frame));
    let quads = find(&mask);
    ctx.metric(DetectMetric::Candidates, quads.len() as u64);
    ctx.timed(DetectMetric::Decode, || decode(frame, &quads))
}
```

- `ctx.metric(id, value)` reports one value, a `u64`: nanoseconds or a count, as declared. A
  metric the node did not declare has no variant, so reporting it does not compile.
- `ctx.timed(id, || work)` runs the work and reports how long it took. It needs `std`, and
  `use daedalus_cpu_author::Timed;` in scope. A
  `no_std` body measures with whatever clock its chip has and calls `ctx.metric` with the
  nanoseconds: `ctx.now_ns` is the host's clock at the start of the tick, not a stopwatch.
- A stage may report more than once in a tick, and may report nothing.
- The host keeps, for each metric, the sum of the values, how many there were and the largest
  (`LoadedHost::metric_rows`), when it was loaded with `LoadOptions::node_metrics`. When it was
  not, a report costs the body one test and writes nothing, and `ctx.timed` reads no clock;
  `ctx.wants_metrics()` says which, for a body that would otherwise measure something only a
  metric is for. Nothing here allocates.
- A node the backend merges into one step with its neighbours keeps its own metrics.

## Firing

A node runs when one of its inputs has a new value and every required input has one. With
`fire = "all"` it waits until every connected input has a new value.

## Failure

A body that returns `Err(Fault { code, reason })`, or that panics, is a fault: the tick goes on
without it, the failure is counted and reported with the node's name and the reason, and the
node is tried again. After three failures in a row it backs off, with a delay that doubles and
never exceeds its cap. Nothing turns a node off by itself. An operator can turn one off by hand,
and automatic turning-off exists only as a policy a host opts into.

A fault's reason is a headline. It is kept in a fixed 120 bytes, so that reporting a failure
allocates nothing, on a chip or anywhere else. A longer one is cut on a character boundary, and
the fault says so: `reason_cut` on what the host reads back (`FaultReport`, and a fault event over
the service). A node with more to say than fits gives the rest to the host as the fault's
detail, below.

The detail is everything else: the lines a check found, a tool's output, a whole error chain. A
body with a `&Ctx` parameter writes it before it returns its error, in as many pieces as it
likes:

```rust
#[node(id = "check.odd", inputs(a), outputs(out))]
fn odd(a: u32, ctx: &Ctx) -> Result<u32, Fault> {
    let mut found = false;
    for n in (1..=a).step_by(2) {
        ctx.detail_fmt(format_args!("{n} is odd\n"));
        found = true;
    }
    if found {
        return Err(Fault { code: 3, reason: "there are odd numbers" });
    }
    Ok(a)
}
```

`ctx.detail(text)` takes a piece of text and `ctx.detail_fmt(format_args!(..))` formats straight
into the host's memory, so the node allocates nothing for it. `ctx.wants_detail()` says whether
anybody keeps it, for a body that would otherwise gather something costly: on a chip, or with a
host that set the bound to zero, nobody does and the calls do nothing. The host keeps what the
step wrote with its failure, up to a bound it sets, and drops what a step wrote and then didn't
fail. It is the same call in the host's own process and in a plugin library.

A fault's `code` is the node's own. The node that failed owns its numbers: they mean what its
author says, all 32 bits of them, and Daedalus passes them through untouched to whoever reads
the failure (`FaultReport::code`, and `code` on a fault event over the service). A host acts on
which failure it was by matching the code, with the node's id beside it, so two plugins that
use the same number are still told apart. A node that gives none has code zero.

Daedalus raises a few faults itself, and those have codes of its own, marked as such
(`own_code` beside the code) so that they can't be taken for a node's: the body panicked (1), a
move between two memory spaces failed (2), a worker went (3), a worker was stopped by the host
(4), a worker wasn't asked because it is down or its inputs didn't fit (5), and the run was
cancelled (6). They are `daedalus_abi::own`.

A node that panics is a fault like any other, and the fault's reason is `panicked: ` and then
what the panic said, cut to 120 bytes. It is the same for a node in the host's own process, in a
plugin library and in a Rust worker, whose reason reads `node panicked: ...`.

## One node, several backends

The id is the node. Two `#[node]` items with the same id and the same ports, each naming another
backend, are two bodies of one node, and the planner picks one. The graph names only the id, or
forces a backend.

```rust
#[node(id = "demo.blur", inputs(src), outputs(dst))]
fn blur_cpu(src: &Samples) -> Samples { .. }

#[node(id = "demo.blur", inputs(src), outputs(dst),
       backend = daedalus_wgpu_author(src: Samples, dst: Samples))]
const BLUR: &str = include_str!("blur.wgsl");
```

## Conversions

`#[node(conversion)]` on a one-in, one-out node tells the planner it may insert that node where
an edge's two types differ.

## Other markers

- `reusable`: the node is pure, so when its inputs have not changed its last output is used and
  the node is not called.
- `shareable`: identical upstream nodes in several graphs of one host may be run once.

A port is in its backend's own memory unless the node says otherwise: `memory(out = "dmabuf")`
puts the port `out` in the memory space named `dmabuf`, and the planner moves the value from there
to whoever reads it. The space has to be one a backend in the plan supplies. Almost nothing needs
this. See [foreign-values.md](foreign-values.md).
## A node made at run time

`#[node]` declares a node when the program is built. A host that only knows a node while it runs
declares it with `Registry::register_owned`, giving an `OwnedDecl`: the id, the ports with their
types, and one body.

- A body that is only text is `OwnedBody::text(backend, text)`. That is what a worker's node has.
- A body with code in this process is `OwnedBody::code(backend, text, code)`, where `code` is
  whatever the named backend reads as a body's code. The registry never looks inside it.
- For the CPU backend that code is a `daedalus_cpu::steps::CpuStepBody`: a closure that builds
  the node's step, and can hold whatever the host knew when it made the node. Such a node runs as
  a step in this process. It can't be merged with its neighbours' code or written out as a blob
  or as source, since it has no function to name.
- A declaration is kept for the life of the process, as every record is. The same declaration
  registered again reuses its record, and one with other code is another declaration.

For the usual case, a function of a few values, `daedalus_cpu::closure::closure_node` makes the
whole declaration from a plain closure, with the cells, the port types and the fault handled:

```rust
use daedalus_cpu::closure::closure_node;
use daedalus_cpu::Fault;

let limit = read_limit();
registry.register_owned(closure_node(
    "app.over_limit",
    "made at run time",
    &["level"],
    Some("over"),
    move |level: u32| Ok::<bool, Fault>(level > limit),
)?)?;
```

- The closure takes up to four inputs and gives one value, or `()` with `None` for the output's
  name. Its types are the ports' types: a closure that doesn't fit the names given is refused
  when the declaration is made, in words.
- A value is the numbers and `bool`, `String`, or `Arc<T>` of any typed `T`. They sit in a cell
  the way a `#[node]` function's do, so the two kinds of node pass values to each other. A
  `String` is copied for the closure, and an `Arc<T>` is handed over as it is: use that for a
  value too large to copy.
- An `Err(Fault { code, reason })` is the step's fault, with the node's own code.
- Its inputs come from edges. A constant is something the closure holds, which is what a
  closure is for.

## Graph documents

A graph is JSON in the format [`schema/`](schema) describes: nodes by id, edges between ports,
and one or more host bridge nodes whose ports are the graph's inputs and outputs. A `label` on a
node names it in reports and across reloads.
