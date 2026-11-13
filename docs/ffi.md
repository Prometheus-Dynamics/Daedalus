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
- Nodes with equal specs share one worker process.
- A run has an optional deadline. When it passes, the worker is sent a cancel, given a grace
  period, and then killed. Only the process the backend itself started is ever killed.
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
tail of what it wrote to stderr in the report. It is started again on a later run, after a
delay that doubles and never exceeds 8 s. It is never given up on, and a worker's crash never
ends the host.

## A Rust plugin in a worker

`daedalus-plugin-worker <library>` serves a dylib plugin's nodes as a worker, so a plugin that
might crash is kept out of the host's process. See [plugins.md](plugins.md).

## From C, without a worker

A C surface for running a graph from a shared library, and a safe Rust host over it, are
planned as `G10` and `G11` and are not built yet.
