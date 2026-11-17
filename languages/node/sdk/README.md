# The Node SDK

Node 22 or later. The SDK is an ES module package with no dependencies: nothing is fetched, and
there is no native code. Add the directory to a project by path, or import it from the repo.

It has two parts:

- **The worker API** (`daedalus-node-sdk`, or `./worker`): declare nodes, then serve them to a
  host that runs them in a process of their own.
- **The service client** (`./client`): drive a running `daedalusd` over a unix socket or loopback
  TCP.

## Write a node

```js
import { NodeError, T, node, serve } from "daedalus-node-sdk";

node("demo.checked_divide", { inputs: [["a", T.i64], ["b", T.i64]], outputs: [["out", T.i64]] },
  (inputs) => {
    if (inputs.b === 0n) throw new NodeError("node_error", "division by zero");
    return inputs.a / inputs.b;
  });

serve();
```

A body takes the inputs by port name and returns one value per output, or an array of them for
several. It may be async. Its context `cx` has `cancelled()`, `progress(fraction, message)`,
`log(level, text)`, `detail(text)`, `faultCode(code)`, and `state` with `setState(value)` for a
node that keeps state. A `console.log` in a body goes to stderr, because stdout carries the
protocol. A body that throws a `NodeError` gives its code to the host; any other throw is a
`node_error` with the stack.

`serve()` returns a promise that resolves when the host shuts the worker down, or closes its
input.

## Cancel

The host cancels a run by sending a cancel for it. Before the first run the main thread reads the
packets itself, and nothing can be cancelled then. At the first run a reader thread takes over
stdin and sets a shared flag for the run that is going on, so `cx.cancelled()` is true as soon as
the host has asked, even while a body is busy. The reader is ready before the first body runs. A
cancelled run ends `cancelled`, with no outputs.

## Drive the daemon

```js
import { connect, typed } from "daedalus-node-sdk/client";
import { T } from "daedalus-node-sdk";

const client = await connect({ socket: "/run/daedalusd.sock" });   // or { host: "127.0.0.1", port: 4100 }
await client.putGraph("g", graphJson);
const run = await client.start("g", { telemetry: "basic" });
await run.feed("in", typed(T.u32, 41));
await run.step(1);
console.log(await run.take("out"));                                  // { ty: { scalar: "u32" }, value: 42 }
const watch = await client.watch("all", "fault");
for await (const event of watch) console.log(event);
client.close();
```

Every call returns a promise. Requests pipeline, and each answer is matched by its id. A refused
request rejects with a `ServiceError`, whose `code` is the protocol's name for the refusal (for
example `unknown_run`); a failure of the client itself rejects with a `ClientError`, whose `code` is
`disconnected`, `unknown_delivery`, `too_large` and so on.

Counters and ids are numbers, and a value beyond 2^53 - 1 is a BigInt. A typed value keeps its
wire type, so a 64-bit integer is a BigInt.

## What the Node worker does not do, and why

- **No shared memory, and no values by reference.** Shared memory needs file-descriptor passing,
  and pure JavaScript cannot send a descriptor. A large `bytes` value travels as a plain packet.
  The behaviour suite's `large` case reports the plain transport for Node. This is a property
  of the platform, not a choice: a native addon would be C or C++ logic, which the repo does not
  take.
- **No in-process host.** Loading the C ABI from Node would need a native addon, for the same
  reason. Node drives the daemon instead, through the service client.
- **The reader thread starts at the first run.** A cancel must reach a body that is busy, so after
  the first run the packets are read on a `worker_threads` thread. That thread takes about 60 ms
  to start on the machine where it was measured (about 60 ms, median of 20, under load), and the
  first run waits for it. The hello and the first ping cost the same as before, and a worker that
  never runs never starts the thread. See `languages/node/sdk/src/worker.js` for why the thread is not started
  earlier.

- **The client does not reconnect.** A dropped connection fails the calls in flight and ends the
  watches. A caller that needs a connection that comes back makes a new one.
- **Only loopback TCP.** The client refuses any other address, as the Rust client does.

## Descriptors

On Linux the host gives a worker one unix socket pair, and the same socket is the worker's stdin
and its stdout. Descriptors that share an open file share its mode, so setting non-blocking on
fd 0 sets it on fd 1 too.

The first run is the point where that happens. Until then the worker reads stdin itself with
blocking reads, and nothing changes the mode. When the first run starts the reader thread, it
wraps stdin in a `net.Socket`, as Node does for `process.stdin`. libuv then makes the descriptor
non-blocking, so fd 0 and fd 1 both are, and the worker's writes handle `EAGAIN` by waiting a
millisecond and writing the rest. A partial write continues from where it stopped, so no byte is
lost or moved, and the wait is a sleep, not a spin.

The SDK does not open any other stream on a standard descriptor:

- It never touches `process.stdin`, `process.stdout` or `process.stderr`. `console.log` in a node
  body writes to fd 2 with `fs.writeSync`.
- The reader thread is created with `stdout: true` and `stderr: true`. By default a `new Worker`
  pipes its streams into this process's `process.stdout` and `process.stderr`, which opens fd 1
  as a stream, and on the host's socket pair that also makes fd 0 non-blocking before the reader
  exists.
- A wrapped stdin is read event-driven, not with a blocking read. A worker thread that is blocked
  in a read cannot end, and Node waits for every worker thread when the process exits, so the
  process would hang on shutdown. A file or a terminal, which cannot be wrapped, is read
  synchronously; a file always gives its bytes or an end.

Measured with `/proc/<pid>/fdinfo` on the real worker: before the first run both descriptors are
blocking, and after it both are non-blocking. A test (`languages/node/tests/descriptors.rs`)
checks those states, and that a second run is answered after the mode changed.

Other languages: any runtime that wraps the descriptor in an event loop changes it. Node does, when
it opens a stream, and Python's `asyncio` does with `connect_read_pipe`. Plain Python `sys.stdin`
and `sys.stdout`, and Java's `System.in` and `System.out`, leave the descriptors blocking.

## Tests

`npm test` runs the unit tests in `test/` with `node --test`. The Rust crate `daedalus-lang-node`
runs them too, and runs the worker suites (the value fixtures and the behaviour suite), the
drive suite through `drive/driver.js`, and the descriptor check (Linux).

Not verified: Windows (named pipes, and the `net.Socket` wrapper of stdin) and macOS. The drive
suite runs over TCP only where unix sockets are missing.
