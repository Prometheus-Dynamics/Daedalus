# Nodes in other languages

A node can be written in Python, JavaScript, Java, C, C++ or Rust and run in a process of its
own. The process backend, `daedalus-backend-process`, starts the worker, sends it each run's
inputs, and reads its outputs. To the planner it is a backend like any other.

## A node in Python

```python
from daedalus import NodeError, T, node

@node("demo.checked_divide", inputs=[("a", T.i64), ("b", T.i64)], outputs=[("out", T.i64)])
def checked_divide(a, b):
    if b == 0:
        raise NodeError("node_error", "division by zero")
    return int(a / b)
```

Each language's SDK is under [`languages/`](../languages), with the worker loop and the node
declarations, and each is run against the same thirteen canonical fixtures:

| Language | SDK | Status |
|---|---|---|
| Python | `languages/python/sdk` | all thirteen fixtures |
| JavaScript (Node) | `languages/node/sdk` | all thirteen fixtures |
| Java | `languages/java/sdk` | all thirteen fixtures |
| C and C++ | `languages/cpp/sdk` | all thirteen fixtures |
| Rust | `languages/rust` | all thirteen fixtures |

The Status section of each language's brief in [`prs/`](prs) (`G5` to `G9`) says exactly what is
served and what is not.

## How a worker is run

```rust
let mut workers = ProcessBackend::new();
workers.add_worker(WorkerSpec::new("python3").arg("worker.py"), &registry)?;
let planned = plan(&graph, &registry, &[&workers, &CpuBackend], &Config::default())?;
```

- A worker says what it serves in its hello: each node's id, ports and types. Each is checked
  against the registry's declaration of that node, and a worker that disagrees with it is
  refused, naming the program and the node.
- A host that has never met a worker doesn't have to write its nodes out first:
  `workers.add_worker_declaring(spec, &mut registry)` registers the nodes the worker declares that
  the registry doesn't have, with the process backend as their body, and returns their ids. A node
  the registry already has is still checked, and a worker that disagrees is still refused.
  `daedalus_backend_process::owned_decl` and `type_expr` are the same translation, for a host that
  reads a hello itself.
- Nodes with equal specs share one worker process.
- A run has an optional deadline. When it passes, the worker is sent a cancel, given a grace
  period, and then killed. Only the process the backend itself started is ever killed.
- A worker that fails (it exits, or its channel breaks) is started again after a wait that
  doubles from 100 ms to 8 s. The failure is one fault. Until the next start is due, a run for that
  worker is skipped with the reason "backing off", which is not another fault and never sleeps on
  the tick. A worker the host stopped itself, at a deadline or a cancel it didn't answer, is
  started again by the next run with no wait.
- A worker inherits the host's environment. `WorkerSpec::env(key, value)` sets a variable,
  `env_remove(key)` keeps one of the host's from it, and `env_clear()` starts it with only what
  the spec sets. `cwd(path)` says where it starts.
- A host's cancel of a tick is sent to the worker as well: the backend checks the step's cancel
  word every few milliseconds while it waits for the worker's outputs. The worker's node sees it
  through `cancelled()` and ends its run cancelled, and the step is reported as a fault with the
  reason "the run was cancelled".
- A node's progress is read back by the host: `LoadedHost::node_progress(node)` gives the
  `(done, total)` the node's step last reported, out of 1000 for a node that reports a fraction.
  `LoadedHost::progress_handle(node)` gives a `NodeProgress` another thread keeps and reads while
  the host ticks, in parallel or not: a read is two atomic loads, with no lock.
- The example `examples/08_ffi_rust_worker` runs a graph whose node is served by a Rust worker,
  with the host feeding it, reading its progress, and cancelling a run.

## The wire

Host and worker talk in packets: a length, then a body. A value is encoded by its port's type,
which the worker declared, so nothing on the wire names a type. There is no JSON and no base64
on this path. The first message each way carries a protocol version and a layout hash, and a
mismatch is refused. `core/wire` is the Rust implementation, and each SDK has its own.

### Large values in shared memory

On Linux a worker's stdin and stdout are one Unix socket, and a large value doesn't travel in its
packet. After the hello the host sends a ping. A worker that can take shared memory asks for it
(kind `0x49`) before its pong, and the host always answers (kind `0x07`): either it grants it,
passing a pool of two memfd buffers over the socket, one for each direction, or it says the worker
stays on the plain path, with the reason. A worker logs that reason as
"host offers no shared memory: plain path: <reason>". A host older than this never answers: the
worker logs the same line, without a reason, when its first run arrives with no answer, and stays
plain. Nothing is ignored on either side.

With memory granted, a packet of 2 KiB or more (the worker's threshold; see below) is written into the buffer for its direction and
only a short note (kind `0x08` from the host, `0x4A` from the worker) travels on the socket. Only a
run goes in the host's buffer and only outputs in the worker's, and the host waits for one run's
outputs before it writes the next, so neither side ever writes over what the other is reading.
Each buffer starts at 1 MiB. A value that doesn't fit goes plain that once, and the host lends a
larger pool (the next power of two) before the next run. A worker that dies, or hangs past its
deadline and is killed, takes its mapping with it, and the worker started in its place is lent a
new pool.

`ProcessBackend::telemetry()` shows each worker's transport: `grants` counts pools lent,
`plain_workers` counts workers that stayed plain (they didn't ask, or were refused), and
`shared_in` and `shared_out` count runs and outputs that travelled in shared memory. The kinds
were added after the layout hash was fixed and aren't part of it. Elsewhere than Linux the worker
runs on pipes and stays plain.

On a CM5 the hand-over of a 1 MiB value there and back went from 3.91 ms/MB to 2.06 ms/MB, and of
a 4 MiB one from 4.10 to 2.69: [testing/board-runs/worker-shm.md](../testing/board-runs/worker-shm.md).

### Values by reference

A worker can ask for more than whole packets in shared memory: right after its ask it sends a want
(kind `0x4B`) naming values by reference. A host that supports it answers its grant with an agree
(kind `0x09`) and lends four more buffers, for output values. A host older than this ignores the
want, so the worker stays on whole packets. The want's features say what goes by reference:
`BY_REFERENCE` (1) for `bytes` and opaque values, and `ANY_TYPE` (2) on top of it for a value of
any type but a fixed scalar (a string, a list, a struct and the rest). The agree names the
features the host took. From then on a port's whole value of such a type, in a run or in
outputs, starts with a form byte: `0` and its bytes follow as before, `1` and it is a place in a
lent buffer (buffer, offset, length), `2` and it is a place in a buffer the host sent by
descriptor (slot, offset, length). A value inside a list, struct or other compound never has a
form byte.

A place names the value's bytes as the host's cell holds them: the content of a `bytes`, opaque
or string value, and the wire form of anything else (a list is its `u32` count, then its items,
packed). Whether a value goes as a place depends only on its size, never on its type: from the
worker's threshold up it goes as a place, below that in the packet. The threshold is 4 KiB for
a worker that agreed values of any type by reference, where a list read in place takes half the
socket's time on a CM5 ([testing/board-runs/worker-any-type.md](../testing/board-runs/worker-any-type.md)),
and 2 KiB otherwise: there whole packets in shared memory and `bytes` values by reference are no
slower than the socket in every round on a CM5, and faster above it
([testing/board-runs/cpu-wire.md](../testing/board-runs/cpu-wire.md)). `WorkerSpec::shared_from` sets it. The host sends it in the grant, and it
applies to whole packets too. The host lays out a value whose type has
fixed fields and then one list of fixed-size items so that the items start on a 64-byte boundary.
A reader can then take them as a slice where they are. Only a `bytes` or opaque value can be a
buffer sent by descriptor.

Each run goes with a note (kind `0x0A`): the value buffer the worker writes this run's outputs
into, if one is free, descriptors to keep and slots to let go, and where the run is. A small run
follows the note in the same frame; a large one is in the host's buffer after its input values.

- **Inputs.** A large input is written into the host's buffer and sent as its place. A cell whose
  type was registered with `ProcessBackend::with_dmabuf::<T>()` (a `DmaBufBacked` type) is sent as
  its buffer's descriptor: once per distinct buffer, kept by the worker in one of 16 slots, and
  named by slot after that. Both sides know a buffer by its device and inode. When all 16 are in
  use, the least recently used is let go, and the worker is told to close it. The worker maps
  each such buffer read-only, so it can never change a value other nodes read. Each read is
  inside the kernel's `DMA_BUF_IOCTL_SYNC` START and END, with READ. A buffer that refuses the
  call (a memfd has no device to sync with) is counted and read as it is.
- **Outputs.** A worker writes a large output into the value buffer the note named, and sends its
  place. The output cell holds that buffer as it is (`OWNER_HANDED`), through a pooled record with
  no allocation. While cells hold a buffer, the host names another. When cells hold all four, the
  run names none, the outputs go in the packet that once, and the run is counted. Nothing waits.
- **A plain worker.** A worker that didn't agree is sent a descriptor value's bytes, copied out of
  a read-only mapping inside the same brackets.

In the Rust SDK a node's `&[u8]` parameter borrows the input where the host put it for the call.
So does a `&[f32]`, `&[u32]` or other number slice of a placed list. A `&[T]` of a `Flat` record
type reads its records straight from the place (see
[node-authoring.md](node-authoring.md#values-that-cross-to-another-process)). A large output of
any type with a form is copied once into the value buffer instead of into the packet. The
host's counters: `agreed` (workers that agreed), `placed_in` and `placed_out` (values sent and
taken by place), `passed_in` (values sent as the place they already were at, with no copy: a
worker's output going back in, or a CPU node's output written into the worker's lent memory),
`fd_sends` (descriptors sent; 0 in steady state), and `outs_full` (runs with no
value buffer free). `daedalus-worker-conformance::large_values` runs any worker through large
values and gives these counters back, and `large_list` does the same with a long `list<u32>`.

On a CM5, by reference took the hand-over of a 1 MiB value from 2.13 ms/MB (whole packets in
shared memory) to 0.22 ms/MB, and of a 4 MiB one from 2.63 to 0.38, of which 0.28 is the node's own
work: [testing/board-runs/worker-by-ref.md](../testing/board-runs/worker-by-ref.md).

## Failure

A worker that exits, hangs or sends something unreadable is a fault of its region, with the
tail of what it wrote to stderr in the report. A fault's reason holds 120 bytes, and a worker's
own reason (up to 512 bytes on the wire) with its stderr tail is usually more: then the whole
text is the fault's detail, which the host reads with `LoadedHost::fault_detail`.

A node in a worker gives the host more than its reason the same way a node in the host does:
it writes a detail before it fails, in any number of pieces, and the host keeps them in order
with the step's fault. Each SDK has the call on the node's context: `cx.detail(text)` in Rust,
Python, JavaScript and Java, and `dd_detail(cx, text)` in C and C++. A Rust plugin served by
`daedalus-plugin-worker` uses `ctx.detail` as it would in the host's process. On the wire it is
a `Detail` message (kind `0x47`) carrying the run and the text, sent before the fault. The kind
was added after the layout hash was fixed and isn't part of it: a host ignores a kind it
doesn't know, so a worker that sends details still works with a host that predates them.

A node in a worker also gives its failure a number of its own, as a node in the host does with
`Fault { code, .. }`: `cx.fault_code(n)` in Rust and Python, `cx.faultCode(n)` in JavaScript and
Java, `dd_fault_code(cx, n)` in C and C++, called before the error is returned. The host carries
it as the fault's code, untouched, with the node's id. It is a `NodeCode` message (kind `0x48`),
added the same way. A node that panics in a Rust worker is reported with the host's own code
for a panic, as it is in the host's process: see
[node-authoring.md](node-authoring.md#failure). It is started again on a later run, after a
delay that doubles and never exceeds 8 s. It is never given up on, and a worker's crash never
ends the host.

## What a worker says

A worker's stdout is the protocol, so it can't print there. It has two other ways to say
something. What it writes to stderr is drained and its last 64 KiB kept. A `Log` message on the
protocol carries a level and a line of text: the host counts it (`ProcessBackend::telemetry().logs`),
keeps it with the stderr tail as `[level] text`, and hands it to the handler set with
`ProcessBackend::on_log`, with the worker's program and pid. A fault report ends with the tail,
so it has the worker's last lines either way.

A worker's messages are read while one of its regions runs, so a line sent between runs reaches
the handler with the next run. The handler runs on the thread that runs the region: it should
take the line and return.

## A Rust plugin in a worker

`daedalus-plugin-worker <library>` serves a dylib plugin's nodes as a worker, so a plugin that
might crash is kept out of the host's process. See [plugins.md](plugins.md).

## From C, without a worker

A C surface for running a graph from a shared library, and a safe Rust host over it, are
planned as `G10` and `G11` and are not built yet.
