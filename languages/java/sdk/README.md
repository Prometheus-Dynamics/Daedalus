# Java SDK

One jar, `daedalus.jar`, with three parts:

- `dev.daedalus.worker`: write nodes that a Daedalus host runs in a worker process.
- `dev.daedalus.client`: drive a running `daedalusd` over its service protocol.
- `dev.daedalus.wire` and `dev.daedalus.service`: the value codec, and the service codec the client
  uses. Both are plain Java and nothing else.

The jar needs JDK 21 or newer. The SDK is compiled with `--release 21`, uses no dependency, and has
no native code.

## Build the jar

From `languages/java`:

```sh
javac --release 21 -d classes $(find sdk/src/main/java -name '*.java')
jar --create --file daedalus.jar -C classes .
```

The tools come from `JAVA_HOME/bin` when `JAVA_HOME` is set, and from `PATH` otherwise. They must be
JDK 21 or newer, and the tests say so and stop when they are not.

`cargo test -p daedalus-lang-java` builds the same jar, and runs the SDK's checks and the two conformance
suites against it. There is no Maven or Gradle build.

## Write a node

A node is a `static` method with two parameters, `Map<String, Object>` for its inputs and `Context`
for what it can see and report. Its ports are `name:type` strings:

```java
import dev.daedalus.worker.Context;
import dev.daedalus.worker.Node;
import dev.daedalus.worker.NodeException;
import dev.daedalus.worker.Worker;
import java.util.Map;

public final class Demo {
    @Node(id = "demo.checked_divide", inputs = {"a:i64", "b:i64"}, outputs = {"out:i64"})
    public static Object checkedDivide(Map<String, Object> in, Context cx) {
        long b = cx.inputLong("b");
        if (b == 0) throw new NodeException("node_error", "division by zero");
        return cx.inputLong("a") / b;
    }

    public static void main(String[] args) throws Exception {
        Worker.serve(Demo.class);
    }
}
```

- A body returns its output's value, or an `Object[]` with one value per output, in order.
- The typed accessors are `inputLong`, `inputBool`, `inputString` and `inputBytes`. `in.get(name)` is
  there too, and gives the value as the codec decoded it.
- A type is written as in the SDK's codec: `i64`, `u32`, `string`, `bytes`, `list<T>`, `optional<T>`,
  `map<K,V>`, `tuple<A,B>`, `struct{name:T}`, `enum{a,b:T}` or `opaque(key)`.
- A node that keeps state declares it with `state = "i64"`, and reads and writes it with `cx.state()`
  and `cx.setState(value)`. An unset state exports as a fault that names the node.
- `cx.cancelled()` is true once the host has cancelled the run. A body that loops should poll it and
  return. Returning after a cancel ends the run as cancelled.
- `cx.progress(fraction, message)`, `cx.log(level, text)` and `cx.detail(text)` send events before the
  run's outputs. `cx.faultCode(n)` gives a failure the node's own number, to be called before throwing.
- A `NodeException` faults the run with its code. Any other exception is `node_error`.
- `System.out` goes to standard error, so a print cannot reach the packet stream. Standard input is
  the packet stream, and a body cannot read it.

## Drive a daemon

```java
import dev.daedalus.client.Client;
import dev.daedalus.client.Endpoint;
import dev.daedalus.client.Run;
import dev.daedalus.client.Watch;
import dev.daedalus.client.Poll;
import dev.daedalus.service.Model.Inspection;
import dev.daedalus.service.Ops.EventKinds;
import dev.daedalus.service.Ops.StartConfig;
import dev.daedalus.service.Ops.WatchTarget;
import dev.daedalus.service.Protocol.StopMode;
import dev.daedalus.service.TypedValue;
import dev.daedalus.wire.Types;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

try (Client client = Client.connect(Endpoint.parse("unix:/run/daedalus/daedalusd.sock"))) {
    client.putGraph("pass", Files.readString(Path.of("pass.json")));
    Run run = client.start("pass", StartConfig.defaults());
    run.feed("in", new TypedValue(Types.parse("u32"), 41L));
    run.step(1);
    System.out.println(run.take("out"));
    run.stop(StopMode.DRAIN);
}
```

- `Endpoint.parse` reads `unix:PATH` or `tcp:ADDRESS:PORT`. A TCP address must be loopback.
- Every call throws `ClientException`, a checked exception. A refusal by the daemon has the kind
  `SERVICE`, with `code()` (for example `UNKNOWN_GRAPH`) and the message in `getMessage()`. A
  connection that drops has the kind `DISCONNECTED` or `UNKNOWN_DELIVERY`.
- `client.watch(new WatchTarget.Everything(), EventKinds.ALL)` returns a `Watch`, which is an
  `Iterator<Event>` and an `AutoCloseable`. `hasNext()` waits for the next event. `poll(Duration)`
  waits up to a time, and gives a `Poll.Got`, `Poll.Timeout` or `Poll.Ended`.
- `client.inSession("name")` gives a handle whose graph names are in another session, on the same
  connection.
- A `Client` can be shared between threads. A `Watch` is read by one thread. Nothing reconnects:
  after a drop, the calls fail and the watches end, and the program opens a new client.

## What the worker does not do, and why

- **No shared memory.** Large values travel in packets, so the behaviour suite's `large` case reports
  the plain transport. Mapping a shared buffer needs native code or a foreign-function interface that
  is final only from release 22, and the SDK stays at 21 with no native code. This is a decision for a
  managed worker, and the ffi doc says so too.
- **No interruption.** The host's cancel sets a flag, and a body sees it only when it polls. A body that
  never polls runs on until the run's deadline, if it has one, and the host then kills the worker.
- **Typed inputs are read by name, not checked against the port.** The body still receives
  `Map<String, Object>`, because the node's signature is fixed by the annotation. An accessor throws
  `IllegalArgumentException` for a missing port, and `ClassCastException` for a value of another type,
  which the worker reports as `node_error`.
- **No reconnect and no JSON codec in the client.** Graph documents are sent as the daemon's JSON text,
  which the caller produces.

## Layout

- `sdk/src/main/java/dev/daedalus/wire`: `Bytes` (fields and records), `Packets` (packet framing),
  `Types` (the type tree and its text form), `Values` (the value codec).
- `sdk/src/main/java/dev/daedalus/worker`: `Worker`, `Context`, `Node`, `NodeException`, and `Protocol`
  (the worker's kinds, fault codes and layout hash).
- `sdk/src/main/java/dev/daedalus/service`: `Protocol` (constants, enums, the hello), `Ops` and `Reply`
  and `Events` (the messages), `Model` (the plain records), and `Codec`.
- `sdk/src/main/java/dev/daedalus/client`: `Client`, `Run`, `Watch`, `Poll`, `Endpoint`, `Connection`
  (the socket and the reader thread) and `ClientException`.
- `sdk/src/test/java`: the checks the Rust test runs as plain programs.
- `drive`: the driver of the drive conformance suite, which is a client written against the jar.
- `fixtures`: the canonical fixture nodes and the behaviour nodes, served by `fixtures.FixtureWorker`.
