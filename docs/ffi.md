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

A Python worker reads its packets on a thread of its own, so a node's `cx.cancelled()` turns true
while the body runs, as in Rust. Python workers stay on the plain transport for large values, by
ruling: the SDK has no shared memory, and any that comes later goes through the Rust implementation.

Each language's SDK is under [`languages/`](../languages), with the worker loop and the node
declarations, and each is run against the same thirteen canonical fixtures:

| Language | SDK | Status |
|---|---|---|
| Python | `languages/python/sdk` | all thirteen fixtures, and the six behaviours (large values travel as plain packets, by ruling: see [its README](../languages/python/sdk/README.md)) |
| JavaScript (Node) | `languages/node/sdk` (worker, and the service client in `client.js`) | all thirteen fixtures and all six behaviours; large values by plain packets, since Node has no fd passing in pure JavaScript; the drive suite over TCP and unix sockets |
| Java | `languages/java/sdk` | all thirteen fixtures and the six behaviours, on the plain transport; drives the daemon through a pure-Java client |
| C and C++ | `languages/cpp/sdk` (`daedalus_worker.h`, `daedalus_worker.hpp`, and the CMake build) over `languages/cpp-sdk` (the C API, in Rust) | all thirteen fixtures and all six behaviours |
| Rust | `languages/rust` | all thirteen fixtures |

The Status section of each language's brief in [`prs/`](prs) (`G5` to `G9`) says exactly what is
served and what is not.

The C and C++ worker's transport is the Rust worker's: the reading and the shared memory are in
`languages/worker-io`, and both SDKs call it. A large value goes by reference on Linux, and plain
elsewhere, as the Rust worker's does.

A behaviour suite holds a worker to what the thirteen fixtures do not cover: a cancel seen during a
run ends it as cancelled, progress, log and detail arrive in order before the outputs, a state
exports and imports and an unset state is a fault, a print in a node body leaves the packet stream
clean, a malformed packet is answered and the next one works, and a 1 MiB value round-trips. Each
case is named and can be run alone (`daedalus-worker-conformance`, `behaviour.rs`), and the case
lists what the worker must do.

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

## Checking a worker's files

Under `locked` (see [plugins.md](plugins.md#integrity-and-the-lock)) the program of a worker is
opened and hashed before each start, and started from that file. What is named with
`WorkerSpec::also_check` is checked before each start too. Under `record` the same files are written
into the lock.

What that proves depends on the language. A worker's program is its interpreter for Python, Node
and Java. Hashing `python3` proves the interpreter, not the worker's script or the modules it
imports. For those, name each file with `also_check`, and the check is before the start, which is
not a binding: the interpreter reads the file by its path after the check.

A program that starts with `#!` is refused under `locked`. Name its interpreter as the program, and
add the script with `also_check`. A native worker, such as a C or C++ worker or a Rust worker, is its
own program, and its one file is the whole check.

## Workers in the daemon

A daemon built with the `workers` feature runs worker programs named in its config, so a node
written in Python, Node, Java, C or C++ is usable from any client of the daemon, with no Rust
host of the user's own. Each `[[worker]]` table names one program:

```toml
[[worker]]
program = "python3"                                   # an absolute path, or a name on PATH
args = ["/usr/lib/daedalusd/workers/worker.py"]       # its arguments, in order
env = { PYTHONUNBUFFERED = "1" }                      # set over the daemon's own environment
cwd = "/usr/lib/daedalusd/workers"                    # absolute; the daemon's own by default
also_check = ["/usr/lib/daedalusd/workers/nodes.py"]  # files it reads by path, checked under the lock
```

The daemon starts each worker after its plugins and greets it. The nodes its hello declares are
then in the registry, and a graph uses them as it uses a plugin's nodes. The integrity mode of
the daemon checks each program and each `also_check` file, as it checks a plugin.

The daemon starts the workers on a thread of its own, so it answers requests while one waits for
its hello. A worker that does not come up is logged once, and tried again in the background with
the same backoff as a plugin, capped at 8 s. Each repeat of the same reason is logged at debug;
a new reason is logged at warning. The daemon serves while a worker is down. A graph that needs a
node of a worker that is not up fails to plan, and the answer names the worker. A worker that dies
while it runs is started again by the process backend at its next step.

`inspect host` lists each worker in its `plugins` list, as `worker <program> <args>: running`,
`restarting`, `refused` (the integrity check refused it), `starting`, or `waiting, not started`
with the reason.

To switch a worker off, remove its `[[worker]]` table and start the daemon again. The config is
read once, at start, so there is no runtime switch; a worker whose program is missing keeps being
tried until the daemon stops or is restarted without it. `--check-config` prints the workers and
says this.

Build a daemon with the feature: `cargo build --release -p daedalusd --features workers`, or with
`--no-default-features --features planner,workers` for a daemon without the planner. The `workers`
feature is not in the default build, because it adds about 290 KB to the planner build (see
[weak-hardware.md](weak-hardware.md#worker-programs)). The packaged daemon is the default build,
so it has no workers: a packaged install that runs worker programs builds its own daemon with the
feature.

A daemon built without `workers` refuses a config that names a worker at `--check-config`, and the
message names the feature. A made plan that needs the process backend is refused at bind by a
daemon without it, and the refusal says the daemon was built without `workers`.

## The wire

Host and worker talk in packets: a length, then a body. A value is encoded by its port's type,
which the worker declared, so nothing on the wire names a type. There is no JSON and no base64
on this path. The first message each way carries a protocol version and a layout hash, and a
mismatch is refused. `core/wire` is the Rust implementation. The C and C++ SDKs use it through `languages/cpp-sdk`; Python, Node and Java each have their own.

### Large values in shared memory

On Linux a worker's stdin and stdout are one Unix socket, and a large value doesn't travel in its
packet. The Python SDK doesn't ask for it: it has no shared-memory transport, by ruling (see
[its README](../languages/python/sdk/README.md)), so its large values are plain packets. After the hello the host sends a ping. A worker that can take shared memory asks for it
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

The Java worker never asks for shared memory, so its large values travel in packets, and the
behaviour suite reports the plain transport for it. That is a decision for a managed worker. The
Java SDK is pure Java at release 21 with no native code, and a Java process cannot map a shared
buffer without it, so the plain path is the one it has.

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

A worker's packets travel on a copy of its stdout, and stdout then points at stderr,
so a print from a node is stderr text and the stream stays clean. It has two ways to say something on
purpose. What it writes to stderr, a print included, is drained and its last 64 KiB kept. A `Log`
message on the protocol carries a level and a line of text: the host counts it (`ProcessBackend::telemetry().logs`),
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

`core/capi` is a C surface for running a graph from a shared library: `include/daedalus.h`
declares it, and a host links `libdaedalus_capi`. A C++ program includes
`languages/cpp/sdk/include/daedalus_host.hpp` over it: `dd::Host` and `dd::Graph` own their handles,
and a failed call throws `dd::HostError`, with the status and the text the library kept. [`languages/rust-capi`](../languages/rust-capi) is a safe Rust
host over it, on the sys crate [`languages/rust-capi-sys`](../languages/rust-capi-sys).

## Driving the daemon from Python

`languages/python/sdk/daedalus/client.py` drives the daemon through the same library, with `ctypes`.
Build it with `cargo build -p daedalus-client-capi`, and give its path to `Client.connect(endpoint,
library=PATH)` or set `DAEDALUS_CLIENT_LIB`. A client is a context manager, a run's values are typed
by the wire codec, and an error carries the daemon's code. The [SDK README](../languages/python/sdk/README.md)
has the calls and their lifetime rules. The drive conformance suite runs the Python client over every
scenario, on both transports.

## Hosting in-process from Python

`languages/python/sdk/daedalus/host.py` loads `libdaedalus_capi` with `ctypes`, built with
`cargo build -p daedalus-capi` and named by `Host(library=PATH)` or `DAEDALUS_HOST_LIB`. It compiles a
graph, pushes, ticks and takes. It offers what this C surface offers: integers, floats, booleans and
bytes, and no cancel, state or fault detail.

## Driving the daemon from Java

`languages/java/sdk` has `dev.daedalus.client.Client`, a pure-Java client of the daemon's service
protocol. `Client.connect(Endpoint.parse("unix:/path"))` or `"tcp:127.0.0.1:4100"` opens a
connection, and the calls mirror the protocol's ops. The codec it uses is the service codec in
`dev.daedalus.service`, which the drive conformance suite holds to the same golden transcripts as
the Rust client and to the corpus of wire bytes. Its usage and its build are in
[`languages/java/sdk/README.md`](../languages/java/sdk/README.md).

## Driving the daemon from C

`languages/client-capi` is a C surface over the daemon client. `include/daedalus_client.h`
declares it, and a program links `libdaedalus_client_capi`. A client handle is one connection to
a running `daedalusd`, over a unix socket (`unix:/path`) or loopback TCP (`tcp:127.0.0.1:4100`).
The daemon runs the graph, so the program needs no plugin of its own.

```c
#include "daedalus_client.h"

daedalus_client_handle_t client = 0;
daedalus_client_connect((const uint8_t *)"tcp:127.0.0.1:4100", 18, 0, &client);
daedalus_client_put_graph(client, (const uint8_t *)"g", 1, graph, graph_len);
daedalus_client_config_t config = {0};
config.telemetry = DAEDALUS_CLIENT_TELEMETRY_BASIC;
uint64_t run = 0;
daedalus_client_start(client, (const uint8_t *)"g", 1, &config, &run);
daedalus_client_feed_i64(client, run, (const uint8_t *)"in", 2, 41, NULL);
daedalus_client_step(client, run, 1, NULL);
int64_t out = 0;
uint8_t present = 0;
daedalus_client_take_i64(client, run, (const uint8_t *)"out", 3, &out, &present);
daedalus_client_close(client);
```

A call returns a status. `DAEDALUS_CLIENT_OK` is zero. After a failure,
`daedalus_client_last_error` gives the reason, and `daedalus_client_last_error_code` gives the
daemon's error code when the daemon refused the call. Text comes back through a buffer: ask
with a null buffer for the size, then call again with a buffer that large. A value that does not
fit, or does not have the type asked for, is kept for the next take of its port.

Watches are pulled. `daedalus_client_watch_next` waits up to a timeout and returns the event as
JSON, so no callback or struct layout is needed. An event is `fault`, `telemetry`, `lagged`,
`ended` or `unknown`, with the same fields `daedalus-ctl` prints.

Compound values use the wire encoding of a typed value with `daedalus_client_feed_wire` and
`daedalus_client_take_wire`. The header gives the bytes. A binding that already has the protocol's
codec produces them.

Four calls keep what the daemon answered for `daedalus_client_report` to copy: the plugin a
`load_plugin_report` loaded, the diagnostics of a `put_graph_report` or `patch_graph_report`, and
the carry of a `reload_report`. `daedalus_client_plan_raw` and `daedalus_client_graph_text` give
the planner's explanation and a stored document as text, so their key order is the one written.

A C++ program includes `languages/cpp/sdk/include/daedalus_client.hpp` over this library. `dd::Client`
and `dd::Watch` own their handles, and a failed call throws `dd::ClientError`, which carries the
status, the text, and the daemon's error code. Closing a client closes its connection for every
handle made from it, as the C call does.

The header and this library grow only by new functions and new statuses. A program built
against an older header keeps working. To check that the library has a newer function, look it
up with `dlsym` and test for null.

## Driving the daemon from Node

`languages/node/sdk/src/client.js` is a pure JavaScript client for the daemon's service protocol,
over `node:net`. It needs no addon. Its codec is a second implementation of the one in
`core/wire`, and the drive suite and the codec check hold the two to the same answers.

```js
import { connect, typed } from "daedalus-node-sdk/client";

const client = await connect({ socket: "/run/daedalusd.sock" });  // or { host: "127.0.0.1", port: 4100 }
const run = await client.start("g");
await run.feed("in", typed({ scalar: "u32" }, 41));
await run.step(1);
const out = await run.take("out");
client.close();
```

The client refuses an address that is not loopback. It does not reconnect. A unix socket is the
default route on Linux, where the drive suite runs over both transports. On Windows the suite
runs over TCP only, and the Windows path of `connect({ socket })` is not verified.
