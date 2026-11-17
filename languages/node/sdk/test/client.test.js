// The client against a small in-process server: pipelined requests matched by id out of order, the
// additive rules on the client's side (an unknown event kind, an error code it does not have), a
// request over the cap refused before it is sent, and what a dropped connection does to calls.
import { test } from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import * as svc from "../src/service.js";
import { connect, ClientError, ServiceError, typed, Watch, WATCH_QUEUE } from "../src/client.js";
import { T } from "../src/wire.js";

function packet(body) {
  const header = Buffer.alloc(4);
  header.writeUInt32LE(body.length);
  return Buffer.concat([header, body]);
}

/** Starts a server on loopback. `handle(message, socket)` answers each request; the hello is automatic. */
async function serve(handle, { packetCap = 16 * 1024 * 1024 } = {}) {
  const sockets = [];
  const server = net.createServer((socket) => {
    sockets.push(socket);
    let pending = Buffer.alloc(0);
    let greeted = false;
    socket.on("data", (chunk) => {
      pending = Buffer.concat([pending, chunk]);
      while (pending.length >= 4 && pending.length >= 4 + pending.readUInt32LE(0)) {
        const len = pending.readUInt32LE(0);
        const body = pending.subarray(4, 4 + len);
        pending = pending.subarray(4 + len);
        const message = svc.decodeClient(body);
        if (!greeted) {
          greeted = true;
          const hello = { kind: "hello", hello: { version: 1, layout: svc.SERVICE_LAYOUT_HASH, packetCap, capabilities: { ops: svc.OP_NAMES, codecs: [], maxRuns: 0 }, knows: [], needs: [] } };
          socket.write(packet(svc.encodeServer(hello)));
          continue;
        }
        handle(message, socket);
      }
    });
    socket.on("error", () => {});
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    port: server.address().port,
    sockets,
    close: () => new Promise((resolve) => { for (const socket of sockets) socket.destroy(); server.close(resolve); }),
  };
}

const reply = (socket, id, body) => socket.write(packet(svc.encodeServer({ kind: "response", id, reply: body })));

test("requests pipeline, and each answer is matched by its id even when they come back out of order", async () => {
  const server = await serve((message, socket) => {
    const { id, op } = message.request;
    if (op.op === "trace") {
      // The trace answer waits: the ping that followed it is answered first.
      setTimeout(() => reply(socket, id, { reply: "trace", entries: [] }), 50);
    } else if (op.op === "ping") {
      reply(socket, id, { reply: "pong", version: 1 });
    }
  });
  const client = await connect({ host: "127.0.0.1", port: server.port });
  const [trace, ping] = await Promise.all([client.trace(0), client.ping()]);
  assert.deepEqual(trace, []);
  assert.equal(ping, 1);
  client.close();
  await server.close();
});

test("an event of a kind the client does not have arrives as unknown, and a watch ends at its last event", async () => {
  const server = await serve((message, socket) => {
    const { id, op } = message.request;
    if (op.op === "watch") {
      reply(socket, id, { reply: "watching", sub: 4n });
      socket.write(packet(svc.encodeServer({ kind: "event", sub: 4n, body: { type: "unknown", eventKind: 77 } })));
      socket.write(packet(svc.encodeServer({ kind: "event", sub: 4n, body: { type: "ended", run: 1n, reason: "stopped" } })));
    }
  });
  const client = await connect({ host: "127.0.0.1", port: server.port });
  const watch = await client.watch({ run: 1 });
  assert.deepEqual(await watch.next(), { type: "unknown", eventKind: 77 });
  assert.deepEqual(await watch.next(), { type: "ended", run: 1, reason: "stopped" });
  assert.equal(await watch.next(), null);
  client.close();
  await server.close();
});

test("an error code this client does not have is other, and the message is kept", async () => {
  const server = await serve((message, socket) => {
    const { id } = message.request;
    socket.write(packet(svc.encodeServer({ kind: "response", id, error: { code: "unknown_run", message: "no run 9", diagnostics: [] } })));
    // Patch the code to one no version has.
    const body = svc.encodeServer({ kind: "response", id: id + 1n, error: { code: "busy", message: "the pool is full", diagnostics: [] } });
    body.writeUInt16LE(900, 1 + 8 + 1);
    socket.write(packet(body));
  });
  const client = await connect({ host: "127.0.0.1", port: server.port });
  const first = await client.call({ op: "cancel", run: 9n }).catch((error) => error);
  assert.ok(first instanceof ServiceError);
  assert.equal(first.code, "unknown_run");
  const second = await client.call({ op: "ping" }).catch((error) => error);
  assert.ok(second instanceof ServiceError);
  assert.equal(second.code, "other");
  assert.equal(second.message, "the pool is full");
  client.close();
  await server.close();
});

test("a request over the server's cap is refused locally, and nothing is sent", async () => {
  let received = 0;
  const server = await serve((message) => {
    received += 1;
    void message;
  }, { packetCap: 256 });
  const client = await connect({ host: "127.0.0.1", port: server.port });
  const big = "x".repeat(1024);
  await assert.rejects(client.putGraph("g", big), (error) => error instanceof ClientError && error.code === "too_large");
  assert.equal(received, 0);
  client.close();
  await server.close();
});

test("a dropped connection fails a call that may have been acted on, and one that only reads", async () => {
  const server = await serve((message, socket) => {
    if (message.request.op.op === "start") socket.destroy();
  });
  const client = await connect({ host: "127.0.0.1", port: server.port });
  await assert.rejects(client.start("g"), (error) => error instanceof ClientError && error.code === "unknown_delivery");
  await assert.rejects(client.ping(), (error) => error instanceof ClientError && error.code === "disconnected");
  await server.close();
});

test("a loopback address is the only one a client reaches, and a session name is bounded", async () => {
  await assert.rejects(connect({ host: "10.1.2.3", port: 9 }), (error) => error instanceof ClientError && error.code === "refused");
  const server = await serve(() => {});
  const client = await connect({ host: "127.0.0.1", port: server.port });
  assert.throws(() => client.session(""), (error) => error.code === "bad_session");
  assert.throws(() => client.session("x".repeat(65)), (error) => error.code === "bad_session");
  assert.equal(client.session("tools").sessionName, "tools");
  client.close();
  await server.close();
});

test("a feed sends a typed value, and the server's reader sees its type", async () => {
  let seen = null;
  const server = await serve((message, socket) => {
    seen = message.request.op;
    reply(socket, message.request.id, { reply: "fed", woke: 1, dropped: false });
  });
  const client = await connect({ host: "127.0.0.1", port: server.port });
  const run = new (await import("../src/client.js")).Run(client, 3n);
  const fed = await run.feed("in", typed(T.u32, 41));
  assert.deepEqual(fed, { woke: 1, dropped: false });
  assert.deepEqual(seen, { op: "feed", run: 3n, port: "in", value: { ty: { scalar: "u32" }, value: 41 } });
  client.close();
  await server.close();
});


test("a watch keeps at most WATCH_QUEUE unread events: the oldest go, and lagged comes first", async () => {
  const watch = new Watch(null, 1, "all");
  const extra = 5;
  for (let tick = 0; tick < WATCH_QUEUE + extra; tick++) watch.push({ type: "telemetry", tick });
  assert.deepEqual(await watch.next(), { type: "lagged", dropped: extra });
  for (let tick = extra; tick < WATCH_QUEUE + extra; tick++) {
    assert.deepEqual(await watch.next(), { type: "telemetry", tick });
  }
  watch.end();
  assert.equal(await watch.next(), null);
});
