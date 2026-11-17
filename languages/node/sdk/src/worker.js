// The worker loop: runs node bodies, and answers the host on fd 1.
//
// The loop has two phases. Until the first run arrives, the main thread reads the packets itself,
// with blocking reads, and answers them there (a ping, a state export or import, a shutdown).
// Nothing can be cancelled before a run exists, so no other thread is needed then. At the first
// run the main thread starts the reader thread (`reader.js`) with the bytes that followed the run's
// packet, waits for it to be ready, and runs the body. From then on the reader owns fd 0, forwards
// each packet in order, and sets the cancel cell when the host cancels the run going on, which
// `cx.cancelled()` reads. Packets are handled one at a time in a promise chain, so a body may be async.
//
// Why the reader starts at the first run and not at start-up: a worker thread takes about 100 ms
// to start on the machine it was measured on, and starting it at the first run keeps the hello and
// the first ping at their own cost. A worker that never runs never starts one. The cost is paid by
// the first run instead. The reader reads stdin through an event loop, not a blocking read: Node
// waits for every worker thread when the process exits, and a thread blocked in a read never ends.
//
// Descriptors. Until the reader starts, the worker's descriptors are blocking, and the main thread
// reads stdin itself. Once the reader wraps stdin (reader.js), libuv makes fd 0 non-blocking, and on
// Linux the host gives the worker one socket for stdin and stdout, so fd 1 becomes non-blocking
// too. The main thread's writes then meet EAGAIN when the host's receive buffer is full, and
// writeSome waits a millisecond and writes the rest: a partial write is continued from where it
// stopped, so no byte is lost or moved. Nothing opens fd 1 or fd 2 as a stream: the worker's own
// `process.stdout` and `process.stderr` are never touched, `console.log` writes to fd 2 directly,
// and the reader thread is created with `stdout: true` and `stderr: true`, so the default piping
// into this process's streams does not happen. See README.md, "Descriptors".
//
// Node bodies see console.log and console.info go to stderr, so stdout carries packets only.
// A body throws NodeError(code, message) for a fault with that code; any other throw is a
// node_error whose reason is the stack, cut to 512 bytes. Events a body reports go out, in order,
// before the run's outputs. A run whose cancel arrived while it ran ends `cancelled`, with no
// outputs.

import fs from "node:fs";
import util from "node:util";
import { Worker } from "node:worker_threads";
import { Framer } from "./framing.js";
import * as wire from "./wire.js";

export const NODES = [];

/**
 * Declares a node: its id, its ports and optional state, and the body to run.
 * @param {string} id
 * @param {{ inputs: Array<[string, object]>, outputs: Array<[string, object]>, state?: object }} spec
 *   Port lists of `[name, type]`, built with `T`.
 * @param {(inputs: Record<string, any>, cx: Context) => any} fn
 *   The body. It returns one value per output, or an array of them for several, or a promise.
 * @returns {typeof fn}
 */
export function node(id, spec, fn) {
  for (const [, ty] of [...spec.inputs, ...spec.outputs]) wire.checkType(ty);
  if (spec.state) wire.checkType(spec.state);
  NODES.push({ id, inputs: spec.inputs, outputs: spec.outputs, state: spec.state ?? null, fn, stateValue: undefined });
  return fn;
}

// A write to a full non-blocking socket fails with EAGAIN. The wait is one millisecond, so a host
// that is slow to read costs a sleep between tries, not a spin. Before the reader starts, fd 1 is
// blocking and this never runs.
const PAUSE = new Int32Array(new SharedArrayBuffer(4));
function writeSome(fd, buf, offset, length) {
  for (;;) {
    try {
      return fs.writeSync(fd, buf, offset, length);
    } catch (error) {
      if (error.code !== "EAGAIN") throw error;
      Atomics.wait(PAUSE, 0, 0, 1);
    }
  }
}

function writeAll(fd, data) {
  let off = 0;
  while (off < data.length) off += writeSome(fd, data, off, data.length - off);
}

function writeOut(packet) {
  wire.writePacket(1, packet, writeSome);
}

function fault(error) {
  if (error instanceof wire.NodeError) return [error.code, error.message];
  return ["node_error", String(error && error.stack ? error.stack : error)];
}

function flushEvents(run, events) {
  for (const event of events) {
    if (event.kind === "progress") writeOut(wire.encodeProgress(run, 0, event.message, event.fraction));
    else if (event.kind === "detail") writeOut(wire.encodeDetail(run, event.text));
    else if (event.kind === "fault_code") writeOut(wire.encodeNodeCode(run, event.code));
    else writeOut(wire.encodeLog(event.level, event.text));
  }
}

/**
 * Serves `nodes` on stdin and stdout until the host shuts the worker down or closes the pipe.
 * @param {Array<object>} [nodes=NODES] the declared nodes; `node()` adds to `NODES`.
 * @param {{ worker?: string }} [options] `worker` is the name the hello gives.
 * @returns {Promise<void>} resolves when the worker has stopped.
 */
export function serve(nodes = NODES, { worker = "daedalus-node-worker" } = {}) {
  // Console output goes to fd 2 directly: no stream is opened on it (see the file's comment).
  console.log = (...args) => writeAll(2, Buffer.from(util.format(...args) + "\n"));
  console.info = console.log;
  const byId = new Map(nodes.map((declared) => [declared.id, declared]));
  const schema = new wire.Schema(opaqueKeys(nodes));
  for (const declared of nodes) {
    for (const [name, ty] of declared.inputs) schema.port(declared.id, "input", name, ty);
    for (const [name, ty] of declared.outputs) schema.port(declared.id, "output", name, ty);
    if (declared.state) schema.port(declared.id, "state", "", declared.state);
  }
  const hello = wire.encodeHello(
    worker,
    nodes.map((declared) => ({ id: declared.id, inputs: declared.inputs, outputs: declared.outputs, state: declared.state })),
    wire.DEFAULT_CAP,
    wire.LAYOUT_HASH,
  );
  writeOut(hello);

  // Phase one: the main thread reads and answers, blocking, until the first run.
  const framer = new Framer(wire.DEFAULT_CAP);
  const chunk = Buffer.alloc(1 << 20);
  for (;;) {
    let body = framer.next();
    while (body === null) {
      let n;
      try {
        n = fs.readSync(0, chunk, 0, chunk.length, null);
      } catch (error) {
        if (error.code === "EOF") n = 0;
        else return Promise.resolve();
      }
      if (n === 0) return Promise.resolve();
      framer.push(Buffer.from(chunk.subarray(0, n)));
      try {
        body = framer.next();
      } catch {
        return Promise.resolve(); // over the cap: the stream cannot be read further
      }
    }
    if (body.length >= 9 && body[0] === wire.KIND.RUN) {
      return phaseTwo(body, framer.rest(), { byId, schema, nodes });
    }
    if (answerNonRun(body, byId, schema) === "stop") return Promise.resolve();
  }
}

/** Handles a packet that is not a run, on the main thread. Returns "stop" for a shutdown. */
function answerNonRun(body, byId, schema) {
  let msg;
  try {
    msg = wire.decodeHost(body, schema);
  } catch (error) {
    writeOut(wire.encodeFault(0, null, "malformed_packet", String(error.message)));
    return "go";
  }
  if (msg.kind === "ping") writeOut(wire.encodePong());
  else if (msg.kind === "state_export") stateExport(msg, byId, schema);
  else if (msg.kind === "state_import") {
    const declared = byId.get(msg.node);
    if (declared) declared.stateValue = msg.value;
  } else if (msg.kind === "shutdown") return "stop";
  // A cancel between runs has nothing to stop.
  return "go";
}

/**
 * Phase two: the reader thread starts with the first run's id and the bytes after its packet, and
 * the first run runs once the reader is ready, so a cancel for it can reach it.
 */
function phaseTwo(firstRun, rest, { byId, schema }) {
  const cell = new BigUint64Array(new SharedArrayBuffer(8));
  const current = firstRun.readBigUInt64LE(1);
  // The reader takes no node options: a worker started with one that only the main thread takes
  // (`--input-type`, from an `-e` script) would refuse to start.
  // `stdout` and `stderr` are true: the worker's own streams are not piped to this process's, so
  // this process never opens its stdout or stderr as a stream (see the file's comment on descriptors).
  const reader = new Worker(new URL("./reader.js", import.meta.url), {
    workerData: { cell, cap: wire.DEFAULT_CAP, rest: new Uint8Array(rest), current },
    execArgv: [],
    stdout: true,
    stderr: true,
  });
  return new Promise((resolve) => {
    let markReady;
    const ready = new Promise((ok) => { markReady = ok; });
    let stopped = false;
    const stop = () => {
      if (stopped) return;
      stopped = true;
      // The reader may still be blocked on stdin; it must not keep the process alive.
      reader.unref();
      resolve();
    };
    const fatal = (error) => {
      writeAll(2, Buffer.from(`daedalus-node-worker: ${error && error.stack ? error.stack : error}\n`));
      stop();
    };
    const handle = async (body) => {
      if (stopped) return;
      let msg;
      try {
        msg = wire.decodeHost(body, schema);
      } catch (error) {
        writeOut(wire.encodeFault(0, null, "malformed_packet", String(error.message)));
        return;
      }
      if (msg.kind === "run") await runRun(msg, byId, schema, cell);
      else if (answerNonRun(body, byId, schema) === "stop") stop();
      // A cancel is forwarded too, and does nothing here: the reader has set the cell already.
    };
    let queue = ready.then(() => handle(firstRun)).catch(fatal);
    reader.on("message", (message) => {
      if (message instanceof ArrayBuffer) {
        queue = queue.then(() => handle(Buffer.from(message))).catch(fatal);
      } else if (message.ready) {
        markReady();
      } else {
        // The reader ends with `end` when stdin closes, and with `error` when a packet is refused.
        if (message.error) writeAll(2, Buffer.from(`daedalus-node-worker: ${message.error}\n`));
        queue = queue.then(stop);
      }
    });
    reader.on("error", fatal);
  });
}

function stateExport(msg, byId, schema) {
  const declared = byId.get(msg.node);
  if (declared && declared.state && declared.stateValue !== undefined) {
    writeOut(wire.encodeStateValue(msg.node, declared.state, declared.stateValue, schema));
  } else {
    // A node with no state, or no state set yet, has nothing to give: a fault names it.
    writeOut(wire.encodeFault(0, msg.node, "bad_input", "the node has no state to export"));
  }
}

function opaqueKeys(nodes) {
  const keys = new Set();
  const walk = (ty) => {
    if (ty.opaque !== undefined) keys.add(ty.opaque);
    else if (ty.optional) walk(ty.optional);
    else if (ty.list) walk(ty.list);
    else if (ty.map) { walk(ty.map[0]); walk(ty.map[1]); }
    else if (ty.tuple) ty.tuple.forEach(walk);
    else if (ty.struct) ty.struct.forEach(([, item]) => walk(item));
    else if (ty.enum) ty.enum.forEach(([, payload]) => payload && walk(payload));
  };
  for (const declared of nodes) {
    [...declared.inputs, ...declared.outputs].forEach(([, ty]) => walk(ty));
    if (declared.state) walk(declared.state);
  }
  return [...keys];
}

async function runRun(msg, byId, schema, cell) {
  const outputs = [];
  let status = "ok";
  for (const id of msg.nodes) {
    const declared = byId.get(id);
    if (!declared) {
      writeOut(wire.encodeFault(msg.run, id, "unknown_node", `no node ${id}`));
      status = "fault";
      break;
    }
    const inputs = {};
    for (const [node, port, value] of msg.inputs) if (node === id) inputs[port] = value;
    const missing = declared.inputs.find(([name]) => !(name in inputs));
    if (missing) {
      writeOut(wire.encodeFault(msg.run, id, "bad_input", `${id} has no input ${missing[0]}`));
      status = "fault";
      break;
    }
    const events = [];
    const cx = {
      cancelled: () => Atomics.load(cell, 0) === msg.run,
      state: declared.stateValue,
      setState(value) { cx.newState = value; },
      progress(fraction, message = "") { events.push({ kind: "progress", fraction, message }); },
      log(level, text) { events.push({ kind: "log", level, text }); },
      // More about a failure than the fault's reason holds: any number of pieces before the
      // throw, kept in order with the step's fault, and dropped by the host if the node succeeds.
      detail(text) { events.push({ kind: "detail", text: String(text) }); },
      // A number of the node's own for this failure, for a host that acts on which failure it
      // was. The host passes it through untouched, with the node's id.
      faultCode(code) { events.push({ kind: "fault_code", code: code >>> 0 }); },
    };
    let result;
    try {
      result = declared.fn(inputs, cx);
      if (result && typeof result.then === "function") result = await result;
    } catch (error) {
      const [code, reason] = fault(error);
      flushEvents(msg.run, events);
      writeOut(wire.encodeFault(msg.run, id, code, reason));
      status = "fault";
      break;
    }
    flushEvents(msg.run, events);
    if (cx.cancelled()) {
      status = "cancelled";
      break;
    }
    if (declared.state && cx.newState !== undefined) declared.stateValue = cx.newState;
    const values = declared.outputs.length === 1 ? [result] : declared.outputs.length === 0 ? [] : result;
    if (!Array.isArray(values) || values.length !== declared.outputs.length) {
      writeOut(wire.encodeFault(msg.run, id, "internal", `${id} returned the wrong number of values`));
      status = "fault";
      break;
    }
    declared.outputs.forEach(([port], i) => outputs.push([id, port, values[i]]));
  }
  if (status === "fault" || status === "cancelled") outputs.length = 0;
  writeOut(wire.encodeOutputs(msg.run, status, outputs, schema));
}
