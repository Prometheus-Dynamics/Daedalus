# Running a graph

## Plan, load, feed, step, take

```rust
use daedalus::prelude::*;
use daedalus_cpu::CpuBackend;

let mut registry = Registry::new();
registry.register_all(&[&DOUBLE_NODE])?;
let graph = GraphDoc::parse(text)?.graph;

let planned = plan(&graph, &registry, &[&CpuBackend], &Config::default())?;
let mut loaded = LoadedHost::load(planned, LoadOptions::default())?;

let host = loaded.host();
host.feed_typed("x", Arc::new(21_i64))?;
host.step()?;
let doubled = host.take_copy::<i64>("doubled")?;
```

[`examples/00_quickstart`](../examples/00_quickstart) is this program.

- **Plan.** `plan(graph, registry, backends, config)` solves the graph, asks each backend which
  nodes it can run, assigns every node, has the backends compile their regions, and stitches the
  results into one flat list of steps. An invalid graph fails here, with diagnostics that name
  the node and the port. Backends are tried in the order given, and `Config::priority` reorders
  them.
- **Load.** `LoadedHost::load` sizes every queue and buffer once. Nothing is allocated for them
  after that.
- **Feed.** `feed_typed(port, Arc<T>)` hands in a value by its handle. `feed_inline(port, bytes)`
  puts plain data of 16 bytes or less in place, with no allocation. A feed never runs a tick.
- **Step.** `step()` runs one tick: every step whose inputs changed, in order. A tick with
  nothing to do reads a few words and returns.
- **Wait.** A step can finish a tick with work still running on a device. It is held on a fence
  until the work is done, and its consumers wait with it. `step_until_settled(timeout)` ticks until
  nothing is held and nothing is woken, or until `timeout` passes, and returns a `Waited` with the
  outcome, the ticks it ran and the steps still held. Between two ticks it sleeps only when a fence
  is the one thing left, starting at 50 µs and never sleeping more than 2 ms. `pending()` counts the
  held steps, and `is_settled()` says whether the host has nothing held and nothing woken.
- **Take.** `take::<T>(port)` and `take_copy::<T>(port)` take the oldest value out of an output
  port, and `None` when it is empty. `drain` takes them all.

The host port names are the ports of the graph's host bridge nodes.

## Queues and policies

An edge is a queue. With no policy it is a FIFO of 64. An edge's policy in the graph can make it
keep only the newest value, bound it and say what is dropped when it is full, or block the
producer. An input port can be given a freshness limit, so a stale value is dropped rather than
used.

## Reload

`loaded.reload(new_plan)` replaces the graph between ticks and returns a `ReloadReport`:

- `carried`: nodes whose step is the same in both plans. Their state came across.
- `fresh`: nodes that are in both plans but whose step changed. They start over.
- `added`, `removed`, `removed_ports`, and how many queued values went with removed ports.

A node's state is in its step. A step is the same when it covers the same nodes, at the same
places in the graph, with the same bodies, constants and ports. Nodes that a backend merged into
one step are kept or started fresh together, so a node that must keep its state beside a
neighbour that changes is kept in a step of its own by setting `daedalus.node.fusion` to false
in its metadata. [`examples/02_host`](../examples/02_host) does this.

A plan that does not fit, such as one for other host bridges, is refused and the running graph is
untouched.

## Failure and what a host can see

A step that fails is contained: the tick goes on, and the step backs off and is tried again.
Nothing is turned off automatically.

- `loaded.fault_reports()`: the most recent failures, each with the step, every node it covers,
  the node at fault when known, the reason, and how many times in a row. A failing transfer has the
  transfer's id in `transfer` and covers no node.
- `Telemetry`: counters (ticks, idle ticks, steps called, faults, skips), one row per step at the
  detailed level, and an event per tick at the trace level.
  [`examples/03_metrics`](../examples/03_metrics) prints them.
- `set_node_on(node, on)`: turn a node off or on by hand.
- `set_unmerged(true)`: run every node as its own step, to find which node in a merged step
  fails. It is never done automatically.
- `patch_const(node, port, value)`: change a constant between ticks without a reload.

## Running continuously

`loaded.start()` gives a `StreamGraph`: a worker thread ticks the graph, and the caller feeds
input handles and reads output handles that never block on a slow node.

## Cancelling

`loaded.cancel_word().cancel()` asks the running tick to stop at the next step boundary. Steps
not yet run stay ready for the next tick, so nothing is lost. `cancel_handle()` gives a handle
another thread can keep.

## Several graphs

`daedalus::host::domain` ticks linked graphs together on one thread, with an output of one
feeding an input of another.

## The service

`daedalusd` is the same host in its own process, for clients that are not linked with it.

- It listens on a unix socket, on TCP on the loopback interface, or on its own stdin and
  stdout. Nothing listens on the network.
- A client stores graphs, starts runs, and feeds, steps and takes them. Graphs and runs belong
  to the daemon: a client that disconnects leaves its runs as they were, and another client can
  go on with them.
- `daedalus-ctl` is the command line client, and `daedalus-client` the Rust one.
  `daedalus-ctl --help` lists the commands.
- The daemon's backends come from the plugins in its plugin directory: see
  [plugins.md](plugins.md).
- [`service/daedalusd/packaging`](../service/daedalusd/packaging) has the unit files and a
  sample config.

An application can open the same listener on a host of its own with
`daedalus_host::listen::ServiceHost` (the feature `listen`).
