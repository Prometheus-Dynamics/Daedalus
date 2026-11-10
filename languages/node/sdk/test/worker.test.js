// The worker loop: that node output never reaches stdout, and a NodeError code reaches the fault.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import * as wire from "../src/wire.js";
import { NodeError, T, node } from "../src/index.js";

const here = fileURLToPath(new URL(".", import.meta.url));

test("a NodeError with a code is a declaration-time check of its port types", () => {
  assert.throws(() => node("demo.bad", { inputs: [["x", { scalar: "complex" }]], outputs: [] }, () => 0), wire.WireError);
  assert.equal(new NodeError("bad_input", "nope").code, "bad_input");
  assert.throws(() => new NodeError("not_a_code", "x"));
});

test("console output in a body goes to stderr, and stdout carries packets only", async () => {
  const script = `
    import { serve, node, T } from ${JSON.stringify(here + "../src/index.js")};
    node("demo.chatty", { inputs: [["x", T.i64]], outputs: [["out", T.i64]] }, (inputs) => {
      console.log("hello from a body");
      return inputs.x;
    });
    serve();
  `;
  const child = spawn(process.execPath, ["--input-type=module", "-e", script], { stdio: ["pipe", "pipe", "pipe"] });
  const chunks = [];
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.stdout.on("data", (chunk) => chunks.push(chunk));
  const packets = [];
  const finished = new Promise((resolve) => child.on("exit", resolve));
  const schema = new wire.Schema([]);
  schema.port("demo.chatty", "input", "x", T.i64);
  schema.port("demo.chatty", "output", "out", T.i64);
  const run = new wire.Writer();
  run.u8(wire.KIND.RUN); run.u64(1n); run.u32(0); run.u32(1); run.str("demo.chatty");
  run.u32(1); run.str("demo.chatty"); run.str("x"); run.i64(5n);
  child.stdin.write(Buffer.concat([u32(run.size), run.finish()]));
  child.stdin.write(Buffer.concat([u32(1), Buffer.from([wire.KIND.SHUTDOWN])]));
  child.stdin.end();
  await finished;
  const stdout = Buffer.concat(chunks);
  // The stream is the hello, then the reply: each packet is a length and a body.
  let at = 0;
  while (at + 4 <= stdout.length) {
    const len = stdout.readUInt32LE(at);
    packets.push(stdout[at + 4]);
    at += 4 + len;
  }
  assert.equal(at, stdout.length, "stdout holds whole packets and nothing else");
  assert.deepEqual(packets, [wire.KIND.HELLO, wire.KIND.OUTPUTS]);
  assert.ok(stderr.includes("hello from a body"));
});

function u32(value) {
  const b = Buffer.alloc(4);
  b.writeUInt32LE(value);
  return b;
}
