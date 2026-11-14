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
