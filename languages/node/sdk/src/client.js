// A client for the daemon's service protocol, over a unix socket or loopback TCP (`node:net`).
//
// Each call sends one request and resolves with its reply. Requests pipeline: several may be in
// flight, and each answer is matched by its id. A watch is an async iterator of its events, in
// the order the server sent them.
//
// This client does not reconnect. A connection that drops fails every call in flight (a call
// that only reads, such as ping, inspect and trace, fails as `disconnected`; any other as
// `unknown_delivery`, since the server may or may not have acted on it), and ends every watch.
//
// Counters, ids and sizes are numbers, and a value beyond 2^53 - 1 is a BigInt. Typed values
// keep their wire types: a 64-bit integer is a BigInt, as the wire codec reads it.

import net from "node:net";
import * as svc from "./service.js";
import * as wire from "./wire.js";

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

/** A count, an id or a tick as a number, or as a BigInt when it is too large for one. */
function safe(value) {
  if (typeof value !== "bigint") return value;
  return value <= MAX_SAFE ? Number(value) : value;
}

/** The error a refused request gives. `code` is the service's name for it (`unknown_run`, and so on). */
export class ServiceError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   * @param {svc.Diagnostic[]} [diagnostics]
   */
  constructor(code, message, diagnostics = []) {
    super(message);
    this.name = "ServiceError";
    this.code = code;
    this.diagnostics = diagnostics;
  }
}

/**
 * A failure of the client itself. `code` is one of: `disconnected`, `unknown_delivery`,
 * `too_large` (a request over the server's packet cap, refused before it was sent), `refused`
 * (the server's hello is not one this client takes), `wire` (a value or message that does not
 * encode or decode), `bad_session` (an empty or too long session name), `closed`, `io`.
 */
export class ClientError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "ClientError";
    this.code = code;
  }
}

/** A typed value: a wire type and a value of it, as `T.u32` and `41` make `typed(T.u32, 41)`. */
export function typed(ty, value) {
  return { ty, value };
}

/** Splits a stream into its packets: a u32 length, then the body. */
class Packets {
  constructor(cap) {
    this.cap = cap;
    this.pending = Buffer.alloc(0);
  }

  push(chunk) {
    this.pending = this.pending.length === 0 ? chunk : Buffer.concat([this.pending, chunk]);
    const bodies = [];
    let at = 0;
    while (this.pending.length - at >= 4) {
      const len = this.pending.readUInt32LE(at);
      if (len > this.cap) throw new ClientError("too_large", `a packet of ${len} bytes is over the cap of ${this.cap}`);
      if (this.pending.length - at - 4 < len) break;
      bodies.push(this.pending.subarray(at + 4, at + 4 + len));
      at += 4 + len;
    }
    this.pending = this.pending.subarray(at);
    return bodies;
  }
}

function packet(body) {
  const header = Buffer.alloc(4);
  header.writeUInt32LE(body.length);
  return Buffer.concat([header, body]);
}

function isLoopback(host) {
  if (host === "localhost") return true;
  if (host === "::1") return true;
  return net.isIPv4(host) && host.startsWith("127.");
}

/** The client's connection, shared by every session view of it. */
function newLink(socket, server) {
  return {
    socket,
    server,
    codecs: new wire.Schema(server.capabilities.codecs),
    cap: Math.min(server.packetCap, wire.DEFAULT_CAP),
    nextId: 1n,
    pending: new Map(),
    watches: new Map(),
    subs: new Map(),
    nextWatch: 1,
    closed: false,
  };
}

/**
 * Connects to a daemon and runs the hello.
 * @param {{ socket: string } | { host: string, port: number }} target a unix socket path, or a
 *   loopback address and port. Other addresses are refused.
 * @returns {Promise<Client>}
 */
export function connect(target) {
  return new Promise((resolve, reject) => {
    let socket;
    if (target.socket !== undefined) {
      socket = net.createConnection(target.socket);
    } else {
      if (!isLoopback(target.host)) {
        reject(new ClientError("refused", `${target.host} is not a loopback address`));
        return;
      }
      socket = net.createConnection({ host: target.host, port: target.port });
      socket.setNoDelay(true);
    }
    const packets = new Packets(wire.DEFAULT_CAP);
    let link = null;
    const fail = (error) => {
      socket.destroy();
      reject(error);
    };
    socket.on("error", (error) => {
      if (link === null) fail(new ClientError("io", String(error.message)));
      else closeLink(link);
    });
    socket.on("close", () => {
      if (link === null) fail(new ClientError("disconnected", "the connection closed before the hello"));
      else closeLink(link);
    });
    socket.on("connect", () => {
      const hello = {
        version: svc.SERVICE_VERSION,
        layout: svc.SERVICE_LAYOUT_HASH,
        packetCap: wire.DEFAULT_CAP,
        capabilities: { ops: [], codecs: [], maxRuns: 0 },
        knows: svc.KNOWN,
        needs: [],
      };
      socket.write(packet(svc.encodeClient({ kind: "hello", hello })));
    });
    socket.on("data", (chunk) => {
      let bodies;
      try {
        bodies = packets.push(chunk);
      } catch (error) {
        if (link === null) fail(error);
        else socket.destroy();
        return;
      }
      for (let at = 0; at < bodies.length; at++) {
        if (link !== null) {
          onBody(link, bodies[at]);
          continue;
        }
        let message;
        try {
          message = svc.decodeServer(bodies[at]);
          if (message.kind === "hello") svc.checkHello(message.hello);
        } catch (error) {
          fail(new ClientError("refused", error.message));
          return;
        }
        if (message.kind === "response" && message.error) {
          fail(new ServiceError(message.error.code, message.error.message, message.error.diagnostics));
          return;
        }
        if (message.kind !== "hello") {
          fail(new ClientError("refused", "the server did not answer the hello"));
          return;
        }
        link = newLink(socket, message.hello);
        resolve(new Client(link, svc.DEFAULT_SESSION));
      }
    });
  });
}

function onBody(link, body) {
  let message;
  try {
    message = svc.decodeServer(body, link.codecs);
  } catch {
    // A body that does not decode means the two sides are out of step: the connection is dropped.
    link.socket.destroy();
    return;
  }
  if (message.kind === "response") answer(link, message);
  else if (message.kind === "event") deliver(link, message);
}

function answer(link, message) {
  const waiter = link.pending.get(message.id);
  if (!waiter) return;
  link.pending.delete(message.id);
  if (waiter.watch) {
    const watch = waiter.watch;
    if (!message.error && message.reply.reply === "watching") {
      if (link.watches.get(watch.id) === watch) {
        watch.sub = message.reply.sub;
        link.subs.set(message.reply.sub, watch);
      } else {
        // Closed before the subscription was answered: the server's watch is ended too.
        sendUnwatch(link, message.reply.sub);
      }
    } else {
      link.watches.delete(watch.id);
      watch.end();
    }
  }
  if (message.error) waiter.reject(new ServiceError(message.error.code, message.error.message, message.error.diagnostics));
  else waiter.resolve(message.reply);
}

/** Ends a server subscription. Its answer is not waited for, and an answer to an unknown id is dropped. */
function sendUnwatch(link, sub) {
  if (link.closed) return;
  const id = link.nextId++;
  const body = svc.encodeClient({ kind: "request", request: { id, session: svc.DEFAULT_SESSION, op: { op: "unwatch", sub } } }, link.codecs);
  link.socket.write(packet(body));
}

function deliver(link, message) {
  const watch = link.subs.get(message.sub);
  if (!watch) return;
  const body = message.body;
  watch.push(publicEvent(body));
  // The last event of a subscription: the run a single-run watch follows ended, or the watch itself did.
  const last =
    body.type === "ended" &&
    (watch.target !== "all" || body.run === 0n || body.reason === "unwatched" || body.reason === "shutdown");
  if (last) {
    link.watches.delete(watch.id);
    link.subs.delete(message.sub);
    watch.end();
  }
}

function closeLink(link) {
  if (link.closed) return;
  link.closed = true;
  for (const [id, waiter] of link.pending) {
    const reason = waiter.readOnly ? "disconnected" : "unknown_delivery";
    waiter.reject(new ClientError(reason, reason === "disconnected" ? "disconnected" : "the connection dropped before the answer"));
    link.pending.delete(id);
  }
  for (const watch of link.watches.values()) watch.end();
  link.watches.clear();
  link.subs.clear();
}

function publicEvent(body) {
  switch (body.type) {
    case "fault":
      return { ...body, run: safe(body.run), tick: safe(body.tick), step: safe(body.step), consecutive: safe(body.consecutive), code: safe(body.code) };
    case "telemetry":
      return { ...body, run: safe(body.run), tick: safe(body.tick), stepCalls: safe(body.stepCalls), faults: safe(body.faults), skipped: safe(body.skipped) };
    case "lagged":
      return { type: "lagged", dropped: safe(body.dropped) };
    case "ended":
      return { type: "ended", run: safe(body.run), reason: body.reason };
    default:
      return { type: "unknown", eventKind: body.eventKind };
  }
}

/** The most unread events a watch keeps, as the daemon's queue for a watcher does. */
export const WATCH_QUEUE = 64;

/**
 * A watch: the events of one subscription, in order. `for await (const event of watch)` reads them.
 *
 * It keeps at most `WATCH_QUEUE` unread events. An event that comes to a full watch drops the
 * oldest, and the next read is `{ type: "lagged", dropped }` with their count, before anything else.
 */
export class Watch {
  constructor(client, id, target) {
    this.client = client;
    this.id = id;
    this.target = target;
    this.sub = null;
    this.queue = [];
    this.dropped = 0;
    this.waiters = [];
    this.ended = false;
  }

  push(event) {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter({ value: event, done: false });
      return;
    }
    if (this.queue.length === WATCH_QUEUE) {
      this.queue.shift();
      this.dropped += 1;
    }
    this.queue.push(event);
  }

  end() {
    this.ended = true;
    for (const waiter of this.waiters.splice(0)) waiter({ value: undefined, done: true });
  }

  /**
   * The next event, or null once the watch has ended and its queued events are read.
   * @returns {Promise<object | null>}
   */
  next() {
    if (this.dropped > 0) {
      const dropped = this.dropped;
      this.dropped = 0;
      return Promise.resolve({ type: "lagged", dropped });
    }
    if (this.queue.length > 0) return Promise.resolve(this.queue.shift());
    if (this.ended) return Promise.resolve(null);
    return new Promise((resolve) => this.waiters.push((result) => resolve(result.done ? null : result.value)));
  }

  /** Ends the watch on the server. Events already queued can still be read. */
  close() {
    const link = this.client.link;
    const current = link.watches.get(this.id);
    if (current === this) link.watches.delete(this.id);
    if (this.sub !== null && link.subs.get(this.sub) === this) {
      link.subs.delete(this.sub);
      sendUnwatch(link, this.sub);
      this.sub = null;
    }
    this.end();
  }

  async *[Symbol.asyncIterator]() {
    for (;;) {
      const event = await this.next();
      if (event === null) return;
      yield event;
    }
  }
}

/** A run: the handle a `start` gives. Its calls are in the client's session. */
export class Run {
  constructor(client, id) {
    this.client = client;
    this.id = id;
  }

  /** Puts a value on an input port. @returns {Promise<{ woke: number, dropped: boolean }>} */
  async feed(port, value) {
    const reply = await this.client.call({ op: "feed", run: BigInt(this.id), port, value });
    return { woke: reply.woke, dropped: reply.dropped };
  }

  /** Runs `count` ticks. */
  async step(count = 1) {
    const reply = await this.client.call({ op: "step", run: BigInt(this.id), count });
    return {
      ticks: reply.ticks,
      tick: safe(reply.tick),
      stepCalls: safe(reply.stepCalls),
      faults: safe(reply.faults),
      skipped: safe(reply.skipped),
      idle: reply.idle,
    };
  }

  /** Takes the oldest value of an output port, as a typed value, or null when it is empty. */
  async take(port) {
    const reply = await this.client.call({ op: "take", run: BigInt(this.id), port });
    return reply.value;
  }

  /** Asks the run to cancel. Its watchers hear of it. */
  async cancel() {
    await this.client.call({ op: "cancel", run: BigInt(this.id) });
  }

  /** Stops the run: `"drain"` (finish what waits) or `"immediate"` (drop it). */
  async stop(mode = "drain") {
    const reply = await this.client.call({ op: "stop", run: BigInt(this.id), mode });
    return { tick: safe(reply.tick), dropped: reply.dropped };
  }

  /** Replaces the run's graph with a stored one. Returns what happened to each node's state. */
  async reload(graph) {
    const reply = await this.client.call({ op: "reload", run: BigInt(this.id), graph });
    const names = (found) => ({ names: found.names, total: found.total });
    return {
      carried: names(reply.carried),
      fresh: names(reply.fresh),
      added: names(reply.added),
      removed: names(reply.removed),
      removedPorts: names(reply.removedPorts),
      droppedValues: reply.droppedValues,
    };
  }

  /** What the host holds for this run. */
  async inspect() {
    return this.client.inspectRun(this.id);
  }

  /** The run's telemetry. */
  async telemetry() {
    return this.client.inspectTelemetry(this.id);
  }

  /** A watch of this run's events. @param {"all" | "fault" | "telemetry"} [kinds="all"] */
  watch(kinds = "all") {
    return this.client.watch({ run: this.id }, kinds);
  }
}

const KIND_BITS = { fault: 1, telemetry: 2, all: 3 };

/** A session view of a connection: its requests are in the session's namespace. */
export class Client {
  constructor(link, session) {
    this.link = link;
    this.sessionName = session;
  }

  /** The server's hello: its ops (names), codecs, run limit and packet cap. */
  get server() {
    const { capabilities, packetCap, version } = this.link.server;
    return { version, packetCap, ops: capabilities.ops, codecs: capabilities.codecs, maxRuns: capabilities.maxRuns };
  }

  /** The same connection, with requests in session `name`. A session is a namespace for graph names. */
  session(name) {
    const bytes = Buffer.byteLength(name);
    if (bytes === 0 || bytes > svc.SESSION_CAP) throw new ClientError("bad_session", `a session name of ${bytes} bytes is empty or too long`);
    return new Client(this.link, name);
  }

  /** Sends one request and resolves with its reply. Rejects with `ServiceError` or `ClientError`. */
  call(op, { readOnly = false } = {}) {
    const link = this.link;
    if (link.closed) return Promise.reject(new ClientError("disconnected", "disconnected"));
    const id = link.nextId++;
    let body;
    try {
      body = svc.encodeClient({ kind: "request", request: { id, session: this.sessionName, op } }, link.codecs);
    } catch (error) {
      return Promise.reject(new ClientError("wire", error.message));
    }
    if (body.length > link.cap) {
      return Promise.reject(new ClientError("too_large", `a request of ${body.length} bytes is over the server's cap of ${link.cap}`));
    }
    return new Promise((resolve, reject) => {
      link.pending.set(id, { resolve, reject, readOnly });
      link.socket.write(packet(body));
    });
  }

  /** Asks the server for its protocol version. */
  async ping() {
    return (await this.call({ op: "ping" }, { readOnly: true })).version;
  }

  /** Loads a plugin from a library file. @returns {Promise<{ name, generation, nodes, backends }>} */
  async loadPlugin(path) {
    const reply = await this.call({ op: "load_plugin", path });
    return { name: reply.name, generation: reply.generation, nodes: reply.nodes, backends: reply.backends };
  }

  /** Stores a graph document under `name`. Returns what the lints found in it. */
  async putGraph(name, json) {
    return (await this.call({ op: "put_graph", name, json })).diagnostics;
  }

  /** Applies a patch document to a stored graph. Returns what the lints found in the result. */
  async patchGraph(graph, patch) {
    return (await this.call({ op: "patch_graph", graph, patch })).diagnostics;
  }

  /**
   * Plans a stored graph.
   * @param {string} graph
   * @param {{ form?: "steps" | "blob" | "source", inspect?: boolean, priority?: string[] }} [options]
   */
  async plan(graph, options = {}) {
    const planOptions = { form: options.form ?? "steps", inspect: options.inspect ?? false, priority: options.priority ?? [] };
    const reply = await this.call({ op: "plan", graph, options: planOptions });
    return {
      graph: reply.graph,
      hash: safe(reply.hash),
      form: reply.form,
      nodes: reply.nodes,
      edges: reply.edges,
      regions: reply.regions,
      steps: reply.steps,
      cached: reply.cached,
      diagnostics: reply.diagnostics,
      explain: reply.explain,
    };
  }

  /** Plans a stored graph with a start's options, and gives the made plan: its hash and its bytes. */
  async makePlan(graph, config = {}) {
    const reply = await this.call({ op: "make_plan", graph, options: startConfig(config) });
    return { hash: safe(reply.hash), bytes: reply.bytes };
  }

  /** Stores a made plan under `name`. A run started on it reads these bytes. */
  async putPlan(name, bytes) {
    await this.call({ op: "put_plan", name, bytes: Buffer.from(bytes) });
  }

  /**
   * Starts a run of a stored graph.
   * @param {string} graph
   * @param {{ fifoCapacity?: number, faultReports?: number, telemetry?: "off" | "basic" | "detailed" | "trace", unmerged?: boolean, priority?: string[] }} [config]
   *   A zero or an empty option leaves the host's own choice.
   * @returns {Promise<Run>}
   */
  async start(graph, config = {}) {
    const reply = await this.call({ op: "start", graph, config: startConfig(config) });
    return new Run(this, safe(reply.run));
  }

  /**
   * Subscribes to events: of every run (`"all"`) or of one run (`{ run }`).
   * @param {"all" | { run: number | bigint }} [target="all"]
   * @param {"all" | "fault" | "telemetry"} [kinds="all"]
   * @returns {Promise<Watch>}
   */
  async watch(target = "all", kinds = "all") {
    if (!(kinds in KIND_BITS)) throw new ClientError("bad_kinds", `watch kinds are fault, telemetry or all, not ${kinds}`);
    const link = this.link;
    const id = link.nextWatch++;
    const watch = new Watch(this, id, target === "all" ? "all" : { run: target.run });
    link.watches.set(id, watch);
    const wireTarget = target === "all" ? "all" : { run: BigInt(target.run) };
    try {
      return await this.callWatching(wireTarget, KIND_BITS[kinds], watch);
    } catch (error) {
      link.watches.delete(id);
      throw error;
    }
  }

  callWatching(target, kinds, watch) {
    const link = this.link;
    if (link.closed) return Promise.reject(new ClientError("disconnected", "disconnected"));
    const id = link.nextId++;
    const body = svc.encodeClient({ kind: "request", request: { id, session: this.sessionName, op: { op: "watch", target, kinds } } }, link.codecs);
    if (body.length > link.cap) return Promise.reject(new ClientError("too_large", "the watch request is over the server's cap"));
    return new Promise((resolve, reject) => {
      link.pending.set(id, { resolve: () => resolve(watch), reject, readOnly: false, watch });
      link.socket.write(packet(body));
    });
  }

  /** What the host holds for run `run`. */
  async inspectRun(run) {
    const reply = await this.call({ op: "inspect", target: { kind: "run", run: BigInt(run) } }, { readOnly: true });
    const info = reply.inspection;
    return {
      run: safe(info.info.run),
      session: info.info.session,
      graph: info.info.graph,
      state: info.info.state,
      tick: safe(info.info.tick),
      faults: safe(info.info.faults),
      ports: info.info.ports.map((port) => ({ name: port.name, input: port.input, typeKey: port.typeKey })),
      nodes: info.info.nodes,
    };
  }

  /** The stored graph `name`, as its document text. */
  async inspectGraph(name) {
    const reply = await this.call({ op: "inspect", target: { kind: "graph", name } }, { readOnly: true });
    return { name: reply.inspection.name, json: reply.inspection.json };
  }

  /** The plan cache's counters. */
  async inspectCache() {
    const reply = await this.call({ op: "inspect", target: { kind: "cache" } }, { readOnly: true });
    const info = reply.inspection;
    return { entries: info.entries, hits: safe(info.hits), misses: safe(info.misses) };
  }

  /** A run's telemetry: one row per step below the detailed level, none. */
  async inspectTelemetry(run) {
    const reply = await this.call({ op: "inspect", target: { kind: "telemetry", run: BigInt(run) } }, { readOnly: true });
    const info = reply.inspection;
    return {
      run: safe(info.run),
      level: info.level,
      tick: safe(info.tick),
      stepCalls: safe(info.stepCalls),
      faults: safe(info.faults),
      skipped: safe(info.skipped),
      rows: info.rows.map((row) => ({
        step: row.step,
        nodes: row.nodes,
        calls: row.calls,
        failures: row.failures,
        backingOff: row.backingOff,
        disabled: row.disabled,
        autoOff: row.autoOff,
        timedRuns: row.timedRuns,
        timeNs: safe(row.timeNs),
        bytes: safe(row.bytes),
      })),
    };
  }

  /** The host as a whole: its sessions, the graphs of this session, its runs and plugins. */
  async inspectHost() {
    const reply = await this.call({ op: "inspect", target: { kind: "host" } }, { readOnly: true });
    const info = reply.inspection;
    return {
      sessions: info.sessions,
      graphs: info.graphs,
      runs: info.runs.map((run) => ({ run: safe(run.run), session: run.session, graph: run.graph, state: run.state })),
      plugins: info.plugins,
    };
  }

  /** Empties the plan cache. */
  async cacheClear() {
    await this.call({ op: "cache", action: "clear" });
  }

  /** The newest `limit` served requests, oldest first. Zero is all the server keeps. */
  async trace(limit = 0) {
    const reply = await this.call({ op: "trace", limit }, { readOnly: true });
    return reply.entries.map((entry) => ({ seq: safe(entry.seq), id: safe(entry.id), session: entry.session, op: entry.op, target: entry.target, error: entry.error }));
  }

  /** Closes the connection for every session view of it. Calls after this fail with `disconnected`. */
  close() {
    const link = this.link;
    if (link.closed) return;
    link.socket.end();
    link.socket.destroy();
    closeLink(link);
  }
}

function startConfig(config) {
  return {
    fifoCapacity: config.fifoCapacity ?? 0,
    faultReports: config.faultReports ?? 0,
    telemetry: config.telemetry ?? "basic",
    unmerged: config.unmerged ?? false,
    priority: config.priority ?? [],
  };
}
