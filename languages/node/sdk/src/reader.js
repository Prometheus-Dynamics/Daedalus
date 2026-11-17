// The stdin reader: a worker thread that reads the packets from fd 0 for the rest of the worker's life.
//
// The main thread runs node bodies synchronously, so it cannot read stdin while a body runs. This
// thread reads every packet, in order, and posts it to the main thread as an ArrayBuffer. A cancel
// that names the run going on is not posted: the thread stores the run's id in a shared cell, and a
// body's `cancelled()` reads that cell. A cancel for another run is dropped, as the Rust worker
// drops it.
//
// The thread starts when the first run arrives (worker.js explains why). It is given the run's id
// and the bytes that followed that run's packet, so it takes over the stream exactly where the main
// thread stopped. It posts `{ ready: true }` before anything else, so the main thread starts the
// run only once a cancel can reach it.
//
// Stdin is read through a `net.Socket` when it is a pipe or a socket, as Node reads process.stdin.
// That is event-driven: the thread sleeps in the event loop, and the worker's exit tears the socket
// down. A blocking read would keep the worker's thread in a system call that Node cannot interrupt,
// and Node waits for every worker thread when the process exits. A file or a terminal cannot be
// wrapped that way, so it is read synchronously; a file always gives its bytes or an end.
//
// Wrapping stdin makes its descriptor non-blocking, and on Linux that is shared with stdout (the
// host gives one socket for both). The main thread's writes handle that; see worker.js, writeSome.
//
// The thread posts `{ end: true }` when the stream ends, and `{ error }` when a packet is refused.

import fs from "node:fs";
import net from "node:net";
import { parentPort, workerData } from "node:worker_threads";
import { Framer } from "./framing.js";
import * as wire from "./wire.js";

const { cell, cap, rest, current: first } = workerData;
let current = first;
const framer = new Framer(cap, Buffer.from(rest));
let finished = false;

function fail(reason) {
  if (!finished) parentPort.postMessage({ error: reason });
  finished = true;
}

function end() {
  if (finished) return;
  finished = true;
  if (framer.rest().length > 0) parentPort.postMessage({ error: "the stream ended inside a packet" });
  else parentPort.postMessage({ end: true });
}

function deliver(body) {
  if (body.length >= 9 && body[0] === wire.KIND.RUN) {
    current = body.readBigUInt64LE(1);
  } else if (body.length >= 9 && body[0] === wire.KIND.CANCEL) {
    const run = body.readBigUInt64LE(1);
    if (run === current) Atomics.store(cell, 0, run);
  }
  const copy = new Uint8Array(body);
  parentPort.postMessage(copy.buffer, [copy.buffer]);
}

// Whole packets from what has come in. False when the stream is refused.
function take() {
  for (;;) {
    let body;
    try {
      body = framer.next();
    } catch (error) {
      fail(String(error.message));
      return false;
    }
    if (body === null) return true;
    deliver(body);
  }
}

function socketStdin() {
  try {
    return new net.Socket({ fd: 0, readable: true, writable: false });
  } catch {
    return null; // a file or a terminal: Node will not wrap it
  }
}

parentPort.postMessage({ ready: true });
const socket = socketStdin();
if (socket !== null) {
  socket.on("data", (chunk) => {
    framer.push(chunk);
    if (!take()) socket.destroy();
  });
  socket.on("end", end);
  socket.on("close", end);
  socket.on("error", (error) => {
    fail(String(error.message));
    socket.destroy();
  });
} else if (take()) {
  const chunk = Buffer.alloc(1 << 20);
  for (;;) {
    let n;
    try {
      n = fs.readSync(0, chunk, 0, chunk.length, null);
    } catch (error) {
      if (error.code === "EOF") n = 0;
      else {
        fail(String(error.message));
        break;
      }
    }
    if (n === 0) {
      end();
      break;
    }
    framer.push(Buffer.from(chunk.subarray(0, n)));
    if (!take()) break;
  }
}
