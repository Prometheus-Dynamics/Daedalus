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

A value of 16 bytes or less that is `Copy` travels in place. Anything else travels behind a
handle and is never copied on the way. An output that is not plain data allocates nothing once
the graph is warm when the nodes that consume it are on the CPU backend, and it reads or takes the
value the way the graph does. Its memory is kept from one tick to the next and refilled. A node
that is read by the host, or by another backend, gets a new handle each tick, one allocation.
A node that takes `Arc<T>` and returns it, or returns an `Arc<T>` it keeps, allocates nothing. To
change a large value in place and hand it on, take `Arc<T>` and use `Arc::make_mut`, which copies
only when someone else still holds the value.

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

The graph sets its fields as one struct constant on the port `config`, and a field it leaves out
keeps its default. A value outside a field's limits, or of the wrong type, fails planning.

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

## Firing

A node runs when one of its inputs has a new value and every required input has one. With
`fire = "all"` it waits until every connected input has a new value.

## Failure

A body that returns `Err(Fault { code, reason })`, or that panics, is a fault: the tick goes on
without it, the failure is counted and reported with the node's name and the reason, and the
node is tried again. After three failures in a row it backs off, with a delay that doubles and
never exceeds its cap. Nothing turns a node off by itself. An operator can turn one off by hand,
and automatic turning-off exists only as a policy a host opts into.

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

## Graph documents

A graph is JSON in the format [`schema/`](schema) describes: nodes by id, edges between ports,
and one or more host bridge nodes whose ports are the graph's inputs and outputs. A `label` on a
node names it in reports and across reloads.
