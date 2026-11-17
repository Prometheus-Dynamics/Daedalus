# Daedalus for Python

The Python SDK does three things, and uses only the standard library:

- **Write a node.** A node is a Python function in a worker process. The worker serves it over
  the wire protocol.
- **Drive the daemon.** `daedalus.client` sends graphs and runs to a running `daedalusd`.
- **Host in-process.** `daedalus.host` loads plugins and runs a graph inside this process.

Nothing is installed from a registry. `pyproject.toml` is metadata only. Put `languages/python/sdk`
on the path (or run from it), and `import daedalus`.

## Write a node

```python
from daedalus import NodeError, T, node, serve


@node("demo.checked_divide", inputs=[("a", T.i64), ("b", T.i64)], outputs=[("out", T.i64)])
def checked_divide(a, b):
    if b == 0:
        raise NodeError("node_error", "division by zero")
    return a // b


if __name__ == "__main__":
    serve()
```

- `inputs` and `outputs` are lists of `(port, type)`. Types are built with `T`: `T.i64`, `T.u32`,
  `T.string`, `T.bytes`, `T.bool`, `T.optional(t)`, `T.list(t)`, `T.map(k, v)`, `T.tuple(...)`,
  `T.struct(...)`, `T.enum(...)`, `T.opaque(key)`.
- A body that takes a parameter named `cx` gets a `Context`: `cx.cancelled()`, `cx.progress(f, text)`,
  `cx.log(level, text)`, `cx.detail(text)`, `cx.fault_code(n)`, and for a node with state, `cx.state`
  (what it held, or `None`) and `cx.new_state`.
- A node with state declares it with `state=T.i64` (or another type). Until the state is set, an
  export of it is a fault that names the node.
- `NodeError(code, message)` is a fault with that code. Any other exception is a `node_error` fault
  whose reason is its traceback.
- A print in a body goes to stderr. Stdout carries packets only.
- Cancel: the worker reads the host's packets on a thread of its own, so `cx.cancelled()` turns true
  as soon as the host cancels the run going on, while the body is still running. A body that polls it
  returns early, and the run ends as `cancelled`.

The worker runs the nodes it is given. A host starts it with `WorkerSpec` (see `docs/ffi.md`).

## Drive the daemon

The client calls the client C library (`libdaedalus_client`, built from `languages/client-capi`) with
`ctypes`. Build it with cargo, then give its path:

```sh
cargo build -p daedalus-client-capi    # target/debug/libdaedalus_client_capi.so (or .dylib, .dll)
export DAEDALUS_CLIENT_LIB=/path/to/libdaedalus_client_capi.so
```

```python
from daedalus import Client, StartConfig, T

with Client.connect("tcp:127.0.0.1:4100") as client:        # or "unix:/path/to/socket"
    client.put_graph("pass", open("pass.json").read())
    run = client.start("pass", StartConfig(telemetry="basic"))
    run.feed("in", 41, ty=T.u32)
    run.step(1)
    print(run.take("out"))                                    # Typed(ty=('scalar', 'u32'), value=41)
    run.stop()
```

- `Client.connect` takes `unix:PATH` or `tcp:ADDRESS`. TCP is for loopback addresses only.
- Calls: `put_graph`, `patch_graph`, `plan`, `make_plan`, `put_plan`, `start`, `step`, `feed`, `take`,
  `cancel`, `stop`, `reload`, `inspect_run`, `inspect_graph`, `graph_text`, `inspect_cache`,
  `inspect_telemetry`, `inspect_host`, `cache_clear`, `trace`, `load_plugin`, `session`, `ping`.
- `watch(run=None, kinds="all", timeout=None)` is an iterator of events. It stops when the stream
  ends, or when `timeout` seconds pass with no event, and then it closes itself. `next_event` and
  `drain` read it by hand.
- Values are typed. `feed(port, value, ty=T.u32)` encodes the value with the wire codec. Without `ty`
  the type is read from the value: a `bool`, `int` (as i64), `float`, `str` or `bytes`.
- Errors are `ServiceError`, a `DaedalusError` with `status`, `status_name`, `message` and `code`. `code`
  is the daemon's error code, such as `unknown_run`. Other failures are `DaedalusError` with no
  `code`. A missing library is `LibraryError`, and a call on a closed client is `ClosedError`.

## Host in-process

The host calls `libdaedalus_capi` (from `core/capi`), built with `cargo build -p daedalus-capi`, and
found through `Host(library=PATH)` or `DAEDALUS_HOST_LIB`.

```python
from daedalus import Host

with Host() as host:
    host.install("/path/to/plugin.so")
    with host.compile(open("pass.json").read()) as graph:
        graph.push("in", 41)
        graph.tick()
        print(graph.take("out"))                              # 41
```

The in-process host offers what the C surface offers, and no more:

- Values are integers, floats, booleans and bytes. A port of another type raises `UnsupportedPort`.
- There is no cancel, no node state, and no fault detail. A tick reports its step calls and faults.
- Ports are named by text, or by index (their place in the sorted names).

## What the worker does not do, and why

**The worker does not use shared memory.** Every value travels as a plain packet in this SDK, however
large. The behaviour suite's `large` case reports the transport as `Plain` for Python, and the test
checks it. This is a decision, not a gap to fill later: there is no second implementation of the
shared-memory ring protocol in Python. If Python workers need it, it will come through the Rust
implementation behind a C ABI, loaded with `ctypes`. The Rust worker SDK has a shared-memory transport where the platform allows it (see `docs/ffi.md`).

## Lifetime and threads

- A `Client` owns a connection, and `close()` (or leaving a `with` block) closes it. Closing any
  handle of a connection closes the connection for all of its sessions, as the C header says. So
  close the root client last. A `Watch` owns one subscription, and its `close()` releases it.
- A `Host` closes the graphs it compiled when it is closed. A `Graph` releases its own handle with
  `close()`.
- The ctypes layer passes each buffer for the length of one call. A call that is repeated for a
  larger buffer keeps the value it took out, so a retry never loses one. A text reply that fits the
  first 64 KiB buffer is one request to the daemon.
- One `Client` may be used from several threads, as the C ABI allows, because ctypes releases the
  GIL for each call. A close made while another thread calls on the same client makes that call fail
  with `stale_handle`.
- Calls on one graph may come from any thread, one at a time. Calls on different graphs do not wait
  on each other.

## Tests

```sh
cd languages/python/sdk && python3 -m unittest discover -s tests     # the SDK's own unit tests
cargo test -p daedalus-lang-python                                  # all of the Python checks
```

The cargo tests run the worker through the behaviour suite and the thirteen fixtures, run the driver
(`languages/python/drive/driver.py`) through the drive conformance suite on every transport, and run
the unit tests above with the two libraries in the environment.
