// The canonical fixture nodes, as the Node worker serves them: the thirteen value fixtures, and the
// behaviour suite's four. Their ports and types match testing/crates/worker-conformance/src/
// fixtures.rs exactly.

import { NodeError, T, node } from "../sdk/src/index.js";

const CUSTOM = "fixture.custom";

node("fixture.scalar_add", { inputs: [["a", T.i64], ["b", T.i64]], outputs: [["out", T.i64]] },
  (inputs) => inputs.a + inputs.b);

const echo = (id, ty) => node(id, { inputs: [["in", ty]], outputs: [["out", ty]] }, (inputs) => inputs.in);

echo("fixture.bytes_echo", T.bytes);
echo("fixture.struct_record", T.struct(["name", T.string], ["count", T.i64]));
echo("fixture.enum_choice", T.enum(["idle", null], ["busy", T.u32]));
echo("fixture.optional_value", T.optional(T.i64));
echo("fixture.tuple_pair", T.tuple(T.string, T.bool));
echo("fixture.map_pairs", T.map(T.string, T.i64));
echo("fixture.custom_type_key", T.opaque(CUSTOM));

node("fixture.list_reverse", { inputs: [["in", T.list(T.u32)]], outputs: [["out", T.list(T.u32)]] },
  (inputs) => [...inputs.in].reverse());

node("fixture.multi_output", { inputs: [["x", T.u32]], outputs: [["a", T.u32], ["b", T.u32]] },
  (inputs) => [inputs.x, (inputs.x * 2) >>> 0]);

node("fixture.raw_io", { inputs: [["payload", T.bytes]], outputs: [["out", T.bytes]] },
  (inputs) => Buffer.from(inputs.payload).reverse());

node("fixture.stateful_counter",
  { inputs: [["delta", T.i64]], outputs: [["total", T.i64]], state: T.i64 },
  (inputs, cx) => {
    const current = cx.state === undefined ? 0n : cx.state;
    const total = current + inputs.delta;
    cx.setState(total);
    return total;
  });

node("fixture.failure_diagnostic", { inputs: [["value", T.i64]], outputs: [["out", T.i64]] },
  (_inputs, cx) => {
    cx.detail("diagnostic detail: the value is rejected, ");
    cx.detail("and this is the rest of it");
    cx.faultCode(77);
    throw new NodeError("node_error", "diagnostic: the value is rejected");
  });

// The behaviour suite's nodes (testing/crates/worker-conformance/src/fixtures.rs): each one is
// the same node as its Rust fixture, with the same ports.

const SLEEP = new Int32Array(new SharedArrayBuffer(4));

// Waits until the host cancels the run, then gives its input back. A body that polls `cancelled()`
// is the case: the reader thread sets the flag while this body sleeps in two-millisecond steps.
node("fixture.wait_cancel", { inputs: [["x", T.i64]], outputs: [["y", T.i64]] }, (inputs, cx) => {
  while (!cx.cancelled()) Atomics.wait(SLEEP, 0, 0, 2);
  return inputs.x;
});

// Progress, log, detail, progress, in that order, then the input.
node("fixture.events", { inputs: [["x", T.i64]], outputs: [["y", T.i64]] }, (inputs, cx) => {
  cx.progress(0.5, "half");
  cx.log("info", "after the progress");
  cx.detail("a detail line");
  cx.progress(1.0, "done");
  return inputs.x;
});

// Prints a line to stdout, which the SDK sends to stderr; then the input plus one.
node("fixture.chatty", { inputs: [["x", T.i64]], outputs: [["y", T.i64]] }, (inputs) => {
  console.log("a node line on stdout");
  return inputs.x + 1n;
});

// Appends its input to its string state, and gives the state.
node("fixture.stateful_label",
  { inputs: [["in", T.string]], outputs: [["out", T.string]], state: T.string },
  (inputs, cx) => {
    const next = (cx.state ?? "") + inputs.in;
    cx.setState(next);
    return next;
  });
