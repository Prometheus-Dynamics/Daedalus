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
  outcome, the ticks it ran, the steps still held, and how many step bodies faulted. A fault
  doesn't end the wait or make it an error: the step's recovery decides what follows, and a graph
  whose step faulted settles with nothing out of that step, so a caller checks `faults` (and the
  host's fault reports for which step and why). Between two ticks it sleeps only when a fence
  is the one thing left, starting at 50 µs and never sleeping more than 2 ms. `pending()` counts the
  held steps, and `is_settled()` says whether the host has nothing held and nothing woken.
- **Take.** `take::<T>(port)` and `take_copy::<T>(port)` take the oldest value out of an output
  port, and `None` when it is empty. `drain` takes them all. A value a step is still working on
  is not there yet, for the host or for any other step: no take gives it and no step that reads
  it runs on it, until the tick or wait that resolves the step's fence. Values the step finished
  before it stay readable meanwhile. A value is taken once. If the fence fails, the value is
  released, nothing is handed out, and the tick reports a fault.

The host port names are the ports of the graph's host bridge nodes.

## Queues and policies

An edge is a queue. With no policy it is a FIFO of 64. An edge's policy in the graph can make it
keep only the newest value, bound it and say what is dropped when it is full, or block the
producer. A bounded edge's `overflow` is one of `drop_oldest`, `drop_newest`, `error_on_full`,
`block` (the producer waits for room) and `coalesce` (the new value replaces the queued one);
any other word is refused by name when the graph is planned. A queue keeps a value until its reader runs and takes it, however many ticks that is,
so a node that did not run when a value was sent reads it when it next runs.

`per_tick` is the policy for a value that means something only in the tick it was sent in
(`EdgePolicy::PerTick`, `{"kind": "per_tick"}`). The edge holds one value, the newest. A reader
that runs later in the same tick gets it; when the tick ends, a value still there is released,
so a later tick's reader gets nothing and nothing the value holds (a buffer from a small pool,
say) stays pinned. A reader that could not run in that tick, because it was waiting on its own
unfinished work, backing off after faults, held by a full blocking output, or switched out by the
host's activity mask, still gets the value in the first tick it can run. A value fed from the
host lives through the next tick. The chip forms, whose queues keep a value until it is read,
refuse a `per_tick` edge by name.

An output with more than one reader gives each reader an edge of its own: its own queue, with
its own depth and policy. The value is shared by a reference count and never copied (16 bytes or
less of plain data are copied as bytes). So the number of readers changes nothing: each reader
takes a value once, a reader that has not run yet still finds it, and it is released when the
last holder lets go. One consequence matters for values that hold something scarce, a camera
frame from a small pool, say: a reader that rarely runs keeps up to its edge's depth of values,
each a reference, so a frame fanned out to a stage that waits on another input pins up to 64
buffers on a FIFO. Give that edge `latest_only` or `per_tick`. A FIFO edge that fills fails the
tick with an error that names the edge: the node and port at each end, how many values it holds
and its policy (`HostError::EdgeFull`). A host input with several readers is different: it is
one kept value, the newest fed, for all of them. Each extra reader costs its producer one more
queue store and one reference per value sent (a few hundred instructions, and no allocation).

The edges inside a group take their policy from the group: `edge_policies` lists a stage input
and the policy of the edges into it (`GroupDef::edge_policies`, or `"edge_policies": [{"at":
{"stage": "b", "port": "in"}, "policy": {"kind": "latest_only"}}]` in a document). The input may
be a connect's target or one a group input feeds; then the caller's edge into the group gets the
policy for that stage, and an input that feeds several stages can give each its own. An edge not
listed keeps its own policy, a FIFO by default. The group nearest the stage wins: an edge into it
that carries a policy of its own, other than a FIFO, that differs from the group's is refused by
name, and the fix is to drop that edge's policy or make it the group's. An entry no connect or
group input leads into, or a second one for the same input, is refused by name too.

A value fed to a host input meets two
rules, in this order. First the port's own policy, which the host sets at run time
(`set_port_policy`): it may give the value up, give up the oldest to make room, or refuse. A port
left on the default FIFO has no rule of its own and passes every value on. Then the policy of the
edge from the host bridge, as a producing step's publish meets it: `latest_only` replaces the
queued value, a bound with `drop_oldest` or `drop_newest` does what it says, and a FIFO,
`error_on_full` or `block` input that is full refuses the feed with `Full`. The queue never holds more than the edge's policy allows, so
at most one of the two gives a value up on a feed; the feed's `dropped` says why, the port's drop
counts include it, and a value given up wakes no one. Every feed takes this path: `feed`,
`feed_typed`, `feed_inline` and the rest, and a value forwarded from another graph. A `held` or
`shared` input is outside it: its value is replaced on every feed.

A host input's policy on the host bridge says what its value is. A `consume` input (the default)
is work: a feed wakes its readers, each value is read once, and a domain ticks the graph for it.
That holds with several readers too, where the one value is kept for all of them until the next
feed. A `held` or `shared` input is context: its value stays for every tick until the host
replaces it, whatever number of readers it has, and a feed to it wakes nothing. The stored plan
carries each input's policy; a plan stored before it did reads every input as `consume`.

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

- A report's `code` is the failing node's own number, passed through untouched, unless
  `own_code` is set: then it is one of the few Daedalus raises itself (`daedalus_abi::own`). See
  [node-authoring.md](node-authoring.md#failure).
- `loaded.fault_detail(step)`: what the step's body gave `detail` before its most recent failure,
  for a failure that says more than a reason holds. A report's `detail` flag says there is one.
  It is everything the step wrote in the tick it failed in, across retries, kept until the step
  fails again, up to `LoadOptions::fault_detail_bytes` for each step (16 KiB unless set, and
  `cut` says when it was more). Zero keeps none, and then a body's `detail` calls do nothing.
  Only a step that writes one costs anything: a failure with no detail allocates nothing.
  Over the service a fault event carries it as `detail` and `detail_cut`, and `daedalus-ctl`
  prints both.
- `loaded.fault_reports()`: the most recent failures, each with the step, every node it covers,
  the node at fault when known, the reason, and how many times in a row. A failing transfer has the
  transfer's id in `transfer` and covers no node.
- `Telemetry`: counters (ticks, idle ticks, steps called, faults, skips), one row per step at the
  detailed level, and an event per tick at the trace level.
  [`examples/03_metrics`](../examples/03_metrics) prints them.
- What each step costs, at the detailed level: a row has the step's calls and failures, and
  with `LoadOptions::overhead_window` above zero also `timed_runs`, `time_ns` and `bytes`. The
  time is the sum over the step's timed runs, each from the end of the step before it to its
  own end. The bytes are the lengths of the values the step published: a value handed on by
  reference counts its length and is not copied, and a typed value, whose size Daedalus
  doesn't know, counts as nothing. Over the service they are on a telemetry row.

  The cost is stated so that a small board can decide. Calls and failures are always counted:
  one addition a step. With no probe (`overhead_window` zero, the default) the rest costs one
  check a step and one for each output, and nothing is read or written. With a probe, a step
  costs one clock read and three additions, and one more addition for each output it
  publishes. Nothing in either case allocates: the figures are in a table sized when the plan
  loads, 144 bytes a step.
- `loaded.metrics()`: the metrics the graph's nodes declare, each with its node, name and unit
  (see [node-authoring.md](node-authoring.md#metrics)). They come from the plan, so a graph
  need not run to be asked, and a stored plan keeps them.
- `loaded.metric_rows()`: what each node's body has reported for each of its metrics since the
  plan was loaded: the sum, the count and the largest value. The host keeps them only when it
  was loaded with `LoadOptions::node_metrics`, which is separate from `overhead_window`: a host
  can have stage numbers without the per-step clock reads, or the other way round. Off, the
  runner makes one test a step and nothing else happens. On, each report is two additions
  and a compare, in a table sized when the plan loads, 24 bytes a metric. A reload starts the
  figures again. They are read in process: the service's telemetry rows and the trace log do
  not carry them.
- `set_node_on(node, on)`: turn a node off or on by hand.
- `set_unmerged(true)`: run every node as its own step, to find which node in a merged step
  fails. It is never done automatically.
- `patch_const(node, port, value)`: change a constant between ticks without a reload.

## Recovery per step

What a fault does is set per step, from the node's metadata when the plan loads. Without a key a
step fails: the tick is contained and the step backs off, as above.

| Key (node metadata) | Value | Effect |
|---|---|---|
| `daedalus.node.recovery` | `"fail"` (the default), `"skip"`, `"restart"` | `skip` drops the tick and the run goes on, even under fail-fast. `restart` marks the next call so the body resets its own state. |
| `daedalus.node.retries` | an integer from 1 | The body runs again in the same tick, on the same inputs, up to that many times. The fault stands after the last retry. |

- Set one of the two keys, not both. A non-integer, zero or unknown value is refused at load with
  `LoadError::Recovery`, naming the node. The same names are exported as `RECOVERY_KEY` and
  `RETRIES_KEY`.
- Nodes merged into one step must agree. Two nodes in one step that ask for different recoveries
  are refused at load; set `daedalus.node.fusion` to `false` on one of them to keep them apart.
- Metadata set on a group's node reaches every node inside the group, through nested groups too.
  A group's definition can also give one stage metadata of its own, under `metadata` by stage
  name (`"metadata": {"decode": {"daedalus.node.fusion": {"type": "bool", "value": false}}}`),
  and a key set there wins over the one inherited from the group's node.
- A graph-wide default is not read: set the key on each node.
- Nothing is switched off automatically by these keys. Turn a failing node off by hand with
  `set_node_on(node, false)`; it is not called until it is turned on again. A merged step can't be
  switched by node name, so keep the node unmerged first.
- Retries don't sleep. Backoff after a fault is still capped at 8 s by the failure rule.

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
- A warm feed, step or take allocates nothing on the socket path: its names are read in place
  from the packet, the trace entry holds them in itself, and the answer is encoded into a buffer
  the connection keeps. A connection keeps at most four of those, and drops one bigger than
  64 KiB once it is written, so one large answer doesn't hold its memory. A feed is served only
  once all of it has been read: one cut short is refused and changes nothing. A value that isn't
  a scalar is still made by its codec, and a body that runs on a thread of its own is not part of
  the socket path. `alloc_socket_path` in `testing/budgets` holds it to zero.

An application can open the same listener on a host of its own with
`daedalus_host::listen::ServiceHost` (the feature `listen`).
