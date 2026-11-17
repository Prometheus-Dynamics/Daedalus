// The service protocol: the messages a client and the daemon exchange, as JavaScript values.
//
// This is a second implementation of `core/wire/src/service`, held to the same rules. The rules
// are the ones at the top of that module:
//
// - A record (one of the nested groups that carry their length) is read field by field. A reader
//   takes the fields it knows, skips what follows, and reads a field added at the end only when
//   there are bytes left. Trailing bytes after a whole message are ignored.
// - An op, event kind or error code this side does not have is kept as the name `other` (for an
//   op in a hello, it is dropped). A reply tag, or a hello's layout, is never guessed at.
// - Anything else is a breaking change, and moves SERVICE_LAYOUT_HASH.
//
// Values are the worker wire's values (wire.js). A u64 in a message is a BigInt here, so every
// value round-trips exactly; `client.js` turns the counters it hands out into numbers.

import * as wire from "./wire.js";

export const SERVICE_VERSION = 1;
export const SESSION_CAP = 64;
export const DEFAULT_SESSION = "default";

/** The packet kinds of the service. They share no value with the worker protocol's. */
export const KIND = { CLIENT_HELLO: 0x80, REQUEST: 0x81, SERVER_HELLO: 0xc0, RESPONSE: 0xc1, EVENT: 0xc2 };

/** What this side knows of the things a peer may rely on. Empty: nothing is named yet. */
export const KNOWN = [];

/** The layout words, as core/wire/src/service.rs lists them. */
const LAYOUT_WORDS = [
  SERVICE_VERSION, wire.LAYOUT_HASH, SESSION_CAP,
  KIND.CLIENT_HELLO, KIND.REQUEST, KIND.SERVER_HELLO, KIND.RESPONSE, KIND.EVENT,
  0xf2a3, // A record has its length before it.
];
export const SERVICE_LAYOUT_HASH = wire.layoutHash(LAYOUT_WORDS);

/** The ops by kind byte: byte N is `OP_NAMES[N - 1]`. */
export const OP_NAMES = [
  "load_plugin", "put_graph", "patch_graph", "plan", "start", "feed", "step", "take", "watch",
  "unwatch", "reload", "cancel", "stop", "inspect", "cache", "trace", "ping", "make_plan", "put_plan",
];
const OP_BYTE = new Map(OP_NAMES.map((name, index) => [name, index + 1]));

/** The error codes by number. A code not listed here is `other`. */
export const ERROR_NAMES = [
  "invalid_request", "unknown_graph", "unknown_run", "unknown_port", "value_type", "plugin_error",
  "planning_failed", "busy", "cancelled", "stopped", "frame_too_large", "unsupported",
];
const OTHER_CODE = 0xffff;

const FORMS = ["steps", "blob", "source"];
const LEVELS = ["off", "basic", "detailed", "trace"];
const RUN_STATES = ["running", "cancelled", "stopped"];
const END_REASONS = ["stopped", "cancelled", "unwatched", "shutdown"];
const INSPECT_TARGETS = ["run", "graph", "cache", "telemetry", "host"];

// Helpers.

function badTag(what, tag) {
  return new wire.WireError(`unknown ${what} tag ${tag}`);
}

function flag(input, what) {
  const tag = input.u8();
  if (tag === 0) return false;
  if (tag === 1) return true;
  throw badTag(what, tag);
}

function putStrs(out, items) {
  out.u32(items.length);
  for (const item of items) out.str(item);
}

function getStrs(input) {
  const count = input.count();
  const items = [];
  for (let i = 0; i < count; i++) items.push(input.str());
  return items;
}

function putOptStr(out, text) {
  if (text == null) out.u8(0);
  else { out.u8(1); out.str(text); }
}

function getOptStr(input) {
  return flag(input, "optional text") ? input.str() : null;
}

function putList(out, items, each) {
  out.u32(items.length);
  for (const item of items) each(out, item);
}

function getList(input, each) {
  const count = input.count();
  const items = [];
  for (let i = 0; i < count; i++) items.push(each(input));
  return items;
}

function indexOf(names, name, what) {
  const at = names.indexOf(name);
  if (at < 0) throw new wire.WireError(`no ${what} named ${name}`);
  return at;
}

function errorCode(name) {
  const at = ERROR_NAMES.indexOf(name);
  return at < 0 ? OTHER_CODE : at + 1;
}

function errorName(code) {
  return code >= 1 && code <= ERROR_NAMES.length ? ERROR_NAMES[code - 1] : "other";
}

// The hello.

/**
 * A hello, as the peer sends it. `capabilities` lists the op names the server serves (unknown ops
 * are dropped), its `codecs` the opaque type keys it has codecs for, and `maxRuns` its run limit.
 * @typedef {{ version: number, layout: bigint, packetCap: number,
 *   capabilities: { ops: string[], codecs: string[], maxRuns: number }, knows: string[], needs: string[] }} Hello
 */

export function encodeHello(out, hello) {
  out.u32(hello.version);
  out.u64(hello.layout);
  out.u32(hello.packetCap);
  out.u32(hello.capabilities.ops.length);
  for (const op of hello.capabilities.ops) out.u8(OP_BYTE.get(op) ?? indexOf(OP_NAMES, op, "op"));
  putStrs(out, hello.capabilities.codecs);
  out.u32(hello.capabilities.maxRuns);
  putStrs(out, hello.knows);
  putStrs(out, hello.needs);
}

export function decodeHello(input) {
  const version = input.u32();
  const layout = input.u64();
  const packetCap = input.u32();
  const count = input.count();
  const ops = [];
  for (let i = 0; i < count; i++) {
    // A newer server serves ops this side doesn't have: they are no use here, and no reason to refuse.
    const name = OP_NAMES[input.u8() - 1];
    if (name !== undefined) ops.push(name);
  }
  const codecs = getStrs(input);
  const maxRuns = input.u32();
  const knows = getStrs(input);
  const needs = getStrs(input);
  return { version, layout, packetCap, capabilities: { ops, codecs, maxRuns }, knows, needs };
}

/** Refuses a peer's hello with another version or layout, or one that relies on something unknown. */
export function checkHello(hello) {
  if (hello.version !== SERVICE_VERSION) {
    throw new wire.WireError(`protocol version ${hello.version}, this side speaks ${SERVICE_VERSION}`);
  }
  if (hello.layout !== SERVICE_LAYOUT_HASH) {
    throw new wire.WireError(
      `the other side was built for another service layout: its layout hash is ` +
        `${hello.layout.toString(16)}, this side has ${SERVICE_LAYOUT_HASH.toString(16)}`,
    );
  }
  const unknown = hello.needs.find((name) => !KNOWN.includes(name));
  if (unknown !== undefined) {
    throw new wire.WireError(`the other side needs \`${unknown}\`, which this side doesn't have`);
  }
}

// Values with their types.

/**
 * A value with its type: `{ ty, value }`, where `ty` is a wire type (`{ scalar: "u32" }`) and
 * `value` is the value as wire.js reads and writes it.
 * @typedef {{ ty: object, value: any }} TypedValue
 */

function encodeTyped(out, typed, codecs) {
  wire.encodeType(typed.ty, out);
  wire.encodeValue(typed.ty, typed.value, codecs, out);
}

function decodeTyped(input, codecs) {
  const ty = wire.decodeType(input);
  return { ty, value: wire.decodeValue(ty, input, codecs) };
}

// Requests.

function encodeStartConfig(out, config) {
  out.record((r) => {
    r.u32(config.fifoCapacity);
    r.u32(config.faultReports);
    r.u8(indexOf(LEVELS, config.telemetry, "telemetry level"));
    r.bool(config.unmerged);
    putStrs(r, config.priority);
  });
}

function decodeStartConfig(input) {
  return input.record((r) => {
    const fifoCapacity = r.u32();
    const faultReports = r.u32();
    const level = r.u8();
    if (level >= LEVELS.length) throw badTag("telemetry level", level);
    const unmerged = flag(r, "unmerged flag");
    return { fifoCapacity, faultReports, telemetry: LEVELS[level], unmerged, priority: getStrs(r) };
  });
}

function encodePlanOptions(out, options) {
  out.record((r) => {
    r.u8(indexOf(FORMS, options.form, "plan form"));
    r.bool(options.inspect);
    putStrs(r, options.priority);
  });
}

function decodePlanOptions(input) {
  return input.record((r) => {
    const form = r.u8();
    if (form >= FORMS.length) throw badTag("plan form", form);
    const inspect = flag(r, "inspect flag");
    return { form: FORMS[form], inspect, priority: getStrs(r) };
  });
}

/**
 * One request, in its envelope. `op` is a plain object with an `op` name and that op's fields:
 * `{ op: "feed", run, port, value }`, `{ op: "watch", target: "all" | { run }, kinds }`, and so on.
 * @typedef {{ id: bigint, session: string, op: object }} Request
 */

export function encodeRequest(out, request, codecs) {
  if (Buffer.byteLength(request.session) > SESSION_CAP) {
    throw new wire.WireError(`a session name of ${Buffer.byteLength(request.session)} bytes is over the cap`);
  }
  out.u8(KIND.REQUEST);
  out.u64(request.id);
  out.str(request.session);
  encodeOp(out, request.op, codecs);
}

function encodeOp(out, op, codecs) {
  const byte = OP_BYTE.get(op.op);
  if (byte === undefined) throw new wire.WireError(`no op named ${op.op}`);
  out.u8(byte);
  switch (op.op) {
    case "load_plugin": out.str(op.path); break;
    case "put_graph": out.str(op.name); out.str(op.json); break;
    case "patch_graph": out.str(op.graph); out.str(op.patch); break;
    case "plan": out.str(op.graph); encodePlanOptions(out, op.options); break;
    case "make_plan": out.str(op.graph); encodeStartConfig(out, op.options); break;
    case "put_plan": out.str(op.name); out.bytes(op.bytes); break;
    case "start": out.str(op.graph); encodeStartConfig(out, op.config); break;
    case "feed": out.u64(op.run); out.str(op.port); encodeTyped(out, op.value, codecs); break;
    case "step": out.u64(op.run); out.u32(op.count); break;
    case "take": out.u64(op.run); out.str(op.port); break;
    case "watch":
      if (op.target === "all") out.u8(0);
      else { out.u8(1); out.u64(op.target.run); }
      out.u8(op.kinds);
      break;
    case "unwatch": out.u64(op.sub); break;
    case "reload": out.u64(op.run); out.str(op.graph); break;
    case "cancel": out.u64(op.run); break;
    case "stop": out.u64(op.run); out.u8(op.mode === "immediate" ? 1 : 0); break;
    case "inspect": encodeInspectTarget(out, op.target); break;
    case "cache": out.u8(0); break;
    case "trace": out.u32(op.limit); break;
    case "ping": break;
    default: throw new wire.WireError(`no op named ${op.op}`);
  }
}

function encodeInspectTarget(out, target) {
  switch (target.kind) {
    case "run": out.u8(0); out.u64(target.run); break;
    case "graph": out.u8(1); out.str(target.name); break;
    case "cache": out.u8(2); break;
    case "telemetry": out.u8(3); out.u64(target.run); break;
    case "host": out.u8(4); break;
    default: throw new wire.WireError(`no inspect target ${target.kind}`);
  }
}

function decodeInspectTarget(input) {
  const tag = input.u8();
  switch (tag) {
    case 0: return { kind: "run", run: input.u64() };
    case 1: return { kind: "graph", name: input.str() };
    case 2: return { kind: "cache" };
    case 3: return { kind: "telemetry", run: input.u64() };
    case 4: return { kind: "host" };
    default: throw badTag("inspect target", tag);
  }
}

function decodeOp(input, codecs) {
  const byte = input.u8();
  const name = OP_NAMES[byte - 1];
  if (name === undefined) throw new wire.WireError(`unknown op ${byte}`);
  switch (name) {
    case "load_plugin": return { op: name, path: input.str() };
    case "put_graph": return { op: name, name: input.str(), json: input.str() };
    case "patch_graph": return { op: name, graph: input.str(), patch: input.str() };
    case "plan": return { op: name, graph: input.str(), options: decodePlanOptions(input) };
    case "make_plan": return { op: name, graph: input.str(), options: decodeStartConfig(input) };
    case "put_plan": return { op: name, name: input.str(), bytes: input.bytes() };
    case "start": return { op: name, graph: input.str(), config: decodeStartConfig(input) };
    case "feed": {
      const run = input.u64();
      const port = input.str();
      return { op: name, run, port, value: decodeTyped(input, codecs) };
    }
    case "step": return { op: name, run: input.u64(), count: input.u32() };
    case "take": return { op: name, run: input.u64(), port: input.str() };
    case "watch": {
      const tag = input.u8();
      let target;
      if (tag === 0) target = "all";
      else if (tag === 1) target = { run: input.u64() };
      else throw badTag("watch target", tag);
      const kinds = input.u8();
      if ((kinds & ~3) !== 0) throw badTag("event kinds", kinds);
      return { op: name, target, kinds };
    }
    case "unwatch": return { op: name, sub: input.u64() };
    case "reload": return { op: name, run: input.u64(), graph: input.str() };
    case "cancel": return { op: name, run: input.u64() };
    case "stop": {
      const run = input.u64();
      const mode = input.u8();
      if (mode > 1) throw badTag("stop mode", mode);
      return { op: name, run, mode: mode === 1 ? "immediate" : "drain" };
    }
    case "inspect": return { op: name, target: decodeInspectTarget(input) };
    case "cache": {
      const action = input.u8();
      if (action !== 0) throw badTag("cache action", action);
      return { op: name, action: "clear" };
    }
    case "trace": return { op: name, limit: input.u32() };
    case "ping": return { op: name };
    default: throw new wire.WireError(`unknown op ${name}`);
  }
}

// Replies and errors.

/**
 * A diagnostic from the planner or a lint: `node` and `port` are null when absent.
 * @typedef {{ severity: "error" | "warning", code: string, message: string, node: string | null, port: string | null }} Diagnostic
 */

function encodeDiagnostic(out, item) {
  out.record((r) => {
    r.u8(item.severity === "warning" ? 1 : 0);
    r.str(item.code);
    r.str(item.message);
    putOptStr(r, item.node);
    putOptStr(r, item.port);
  });
}

function decodeDiagnostic(input) {
  return input.record((r) => {
    const severity = r.u8();
    if (severity > 1) throw badTag("severity", severity);
    const code = r.str();
    const message = r.str();
    return { severity: severity === 1 ? "warning" : "error", code, message, node: getOptStr(r), port: getOptStr(r) };
  });
}

function encodeNames(out, names) {
  out.record((r) => {
    putStrs(r, names.names);
    r.u32(names.total);
  });
}

function decodeNames(input) {
  return input.record((r) => ({ names: getStrs(r), total: r.u32() }));
}

/**
 * A refused request: `code` is the error's name (`other` for one this side does not have).
 * @typedef {{ code: string, message: string, diagnostics: Diagnostic[] }} ServiceErrorBody
 */

export function encodeServiceError(out, error) {
  out.u16(error.code === "other" ? OTHER_CODE : errorCode(error.code));
  out.str(error.message);
  putList(out, error.diagnostics ?? [], encodeDiagnostic);
}

export function decodeServiceError(input) {
  const code = errorName(input.u16());
  return { code, message: input.str(), diagnostics: getList(input, decodeDiagnostic) };
}

function encodeRunInfo(out, info) {
  out.record((r) => {
    r.u64(info.run);
    r.str(info.session);
    r.str(info.graph);
    r.u8(indexOf(RUN_STATES, info.state, "run state"));
    r.u64(info.tick);
    r.u64(info.faults);
    putList(r, info.ports, (w, port) => w.record((p) => {
      p.str(port.name);
      p.bool(port.input);
      p.str(port.typeKey);
    }));
    putStrs(r, info.nodes);
  });
}

function decodeRunInfo(input) {
  return input.record((r) => ({
    run: r.u64(),
    session: r.str(),
    graph: r.str(),
    state: decodeRunState(r),
    tick: r.u64(),
    faults: r.u64(),
    ports: getList(r, (w) => w.record((p) => ({ name: p.str(), input: flag(p, "port direction"), typeKey: p.str() }))),
    nodes: getStrs(r),
  }));
}

function decodeRunState(input) {
  const tag = input.u8();
  if (tag >= RUN_STATES.length) throw badTag("run state", tag);
  return RUN_STATES[tag];
}

function encodeInspection(out, inspection) {
  switch (inspection.kind) {
    case "run": out.u8(0); encodeRunInfo(out, inspection.info); break;
    case "graph": out.u8(1); out.str(inspection.name); out.str(inspection.json); break;
    case "cache": out.u8(2); out.u32(inspection.entries); out.u64(inspection.hits); out.u64(inspection.misses); break;
    case "telemetry":
      out.u8(3);
      out.u64(inspection.run);
      out.u8(indexOf(LEVELS, inspection.level, "telemetry level"));
      out.u64(inspection.tick);
      out.u64(inspection.stepCalls);
      out.u64(inspection.faults);
      out.u64(inspection.skipped);
      putList(out, inspection.rows, (w, row) => w.record((r) => {
        r.u32(row.step);
        putStrs(r, row.nodes);
        r.u32(row.calls);
        r.u32(row.failures);
        r.bool(row.backingOff);
        r.bool(row.disabled);
        r.bool(row.autoOff);
        r.u32(row.timedRuns);
        r.u64(row.timeNs);
        r.u64(row.bytes);
      }));
      break;
    case "host":
      out.u8(4);
      putStrs(out, inspection.sessions);
      putStrs(out, inspection.graphs);
      putList(out, inspection.runs, (w, run) => w.record((r) => {
        r.u64(run.run);
        r.str(run.session);
        r.str(run.graph);
        r.u8(indexOf(RUN_STATES, run.state, "run state"));
      }));
      putStrs(out, inspection.plugins);
      break;
    default: throw new wire.WireError(`no inspection ${inspection.kind}`);
  }
}

function decodeInspection(input) {
  const tag = input.u8();
  switch (tag) {
    case 0: return { kind: "run", info: decodeRunInfo(input) };
    case 1: return { kind: "graph", name: input.str(), json: input.str() };
    case 2: return { kind: "cache", entries: input.u32(), hits: input.u64(), misses: input.u64() };
    case 3: {
      const run = input.u64();
      const levelTag = input.u8();
      if (levelTag >= LEVELS.length) throw badTag("telemetry level", levelTag);
      const info = { kind: "telemetry", run, level: LEVELS[levelTag], tick: input.u64(), stepCalls: input.u64(), faults: input.u64(), skipped: input.u64() };
      info.rows = getList(input, (w) => w.record((r) => {
        const row = {
          step: r.u32(),
          nodes: getStrs(r),
          calls: r.u32(),
          failures: r.u32(),
          backingOff: flag(r, "backing off"),
          disabled: flag(r, "disabled"),
          autoOff: flag(r, "auto off"),
          timedRuns: 0,
          timeNs: 0n,
          bytes: 0n,
        };
        // Added together: all three are there, or none is.
        if (r.more()) {
          row.timedRuns = r.u32();
          row.timeNs = r.u64();
          row.bytes = r.u64();
        }
        return row;
      }));
      return info;
    }
    case 4: return {
      kind: "host",
      sessions: getStrs(input),
      graphs: getStrs(input),
      runs: getList(input, (w) => w.record((r) => ({ run: r.u64(), session: r.str(), graph: r.str(), state: decodeRunState(r) }))),
      plugins: getStrs(input),
    };
    default: throw badTag("inspection", tag);
  }
}

function encodeTraceEntry(out, entry) {
  out.record((r) => {
    r.u64(entry.seq);
    r.u64(entry.id);
    r.str(entry.session);
    r.u8(OP_BYTE.get(entry.op) ?? 255);
    r.str(entry.target);
    r.u16(entry.error == null ? 0 : errorCode(entry.error));
  });
}

function decodeTraceEntry(input) {
  return input.record((r) => {
    const seq = r.u64();
    const id = r.u64();
    const session = r.str();
    const byte = r.u8();
    const target = r.str();
    const code = r.u16();
    return {
      seq,
      id,
      session,
      op: OP_NAMES[byte - 1] ?? "other",
      target,
      error: code === 0 ? null : errorName(code),
    };
  });
}

function encodeReply(out, reply, codecs) {
  switch (reply.reply) {
    case "pong": out.u8(0); out.u32(reply.version); break;
    case "ack": out.u8(1); break;
    case "plugin":
      out.u8(2); out.str(reply.name); out.u32(reply.generation); putStrs(out, reply.nodes); putStrs(out, reply.backends);
      break;
    case "graph": out.u8(3); out.str(reply.name); putList(out, reply.diagnostics, encodeDiagnostic); break;
    case "plan":
      out.u8(4);
      out.str(reply.graph);
      out.u64(reply.hash);
      out.u8(indexOf(FORMS, reply.form, "plan form"));
      out.u32(reply.nodes);
      out.u32(reply.edges);
      out.u32(reply.regions);
      out.u32(reply.steps);
      out.bool(reply.cached);
      putList(out, reply.diagnostics, encodeDiagnostic);
      putOptStr(out, reply.explain);
      break;
    case "started": out.u8(5); out.u64(reply.run); break;
    case "fed": out.u8(6); out.u32(reply.woke); out.bool(reply.dropped); break;
    case "stepped":
      out.u8(7);
      out.u32(reply.ticks);
      out.u64(reply.tick);
      out.u64(reply.stepCalls);
      out.u64(reply.faults);
      out.u64(reply.skipped);
      out.bool(reply.idle);
      break;
    case "value":
      out.u8(8);
      if (reply.value == null) out.u8(0);
      else { out.u8(1); encodeTyped(out, reply.value, codecs); }
      break;
    case "watching": out.u8(9); out.u64(reply.sub); break;
    case "reloaded":
      out.u8(10);
      encodeNames(out, reply.carried);
      encodeNames(out, reply.fresh);
      encodeNames(out, reply.added);
      encodeNames(out, reply.removed);
      encodeNames(out, reply.removedPorts);
      out.u32(reply.droppedValues);
      break;
    case "stopped": out.u8(11); out.u64(reply.run); out.u64(reply.tick); out.u32(reply.dropped); break;
    case "inspection": out.u8(12); encodeInspection(out, reply.inspection); break;
    case "trace": out.u8(13); putList(out, reply.entries, encodeTraceEntry); break;
    case "made": out.u8(14); out.u64(reply.hash); out.bytes(reply.bytes); break;
    default: throw new wire.WireError(`no reply ${reply.reply}`);
  }
}

function decodeReply(input, codecs) {
  const tag = input.u8();
  switch (tag) {
    case 0: return { reply: "pong", version: input.u32() };
    case 1: return { reply: "ack" };
    case 2: return { reply: "plugin", name: input.str(), generation: input.u32(), nodes: getStrs(input), backends: getStrs(input) };
    case 3: return { reply: "graph", name: input.str(), diagnostics: getList(input, decodeDiagnostic) };
    case 4: {
      const graph = input.str();
      const hash = input.u64();
      const form = input.u8();
      if (form >= FORMS.length) throw badTag("plan form", form);
      return {
        reply: "plan",
        graph,
        hash,
        form: FORMS[form],
        nodes: input.u32(),
        edges: input.u32(),
        regions: input.u32(),
        steps: input.u32(),
        cached: flag(input, "cached flag"),
        diagnostics: getList(input, decodeDiagnostic),
        explain: getOptStr(input),
      };
    }
    case 5: return { reply: "started", run: input.u64() };
    case 6: return { reply: "fed", woke: input.u32(), dropped: flag(input, "dropped flag") };
    case 7: return {
      reply: "stepped",
      ticks: input.u32(),
      tick: input.u64(),
      stepCalls: input.u64(),
      faults: input.u64(),
      skipped: input.u64(),
      idle: flag(input, "idle flag"),
    };
    case 8: return { reply: "value", value: flag(input, "value flag") ? decodeTyped(input, codecs) : null };
    case 9: return { reply: "watching", sub: input.u64() };
    case 10: return {
      reply: "reloaded",
      carried: decodeNames(input),
      fresh: decodeNames(input),
      added: decodeNames(input),
      removed: decodeNames(input),
      removedPorts: decodeNames(input),
      droppedValues: input.u32(),
    };
    case 11: return { reply: "stopped", run: input.u64(), tick: input.u64(), dropped: input.u32() };
    case 12: return { reply: "inspection", inspection: decodeInspection(input) };
    case 13: return { reply: "trace", entries: getList(input, decodeTraceEntry) };
    case 14: return { reply: "made", hash: input.u64(), bytes: input.bytes() };
    default: throw badTag("reply", tag);
  }
}

// Events.

function encodeEvent(out, body) {
  switch (body.type) {
    case "fault":
      out.u8(0);
      out.u64(body.run);
      out.u32(body.tick);
      out.u32(body.step);
      putOptStr(out, body.node);
      putStrs(out, body.covers);
      out.u32(body.consecutive);
      out.str(body.reason);
      out.bool(body.reasonCut);
      out.str(body.detail);
      out.bool(body.detailCut);
      out.u32(body.code);
      out.bool(body.ownCode);
      break;
    case "telemetry":
      out.u8(1); out.u64(body.run); out.u64(body.tick); out.u64(body.stepCalls); out.u64(body.faults); out.u64(body.skipped);
      break;
    case "lagged": out.u8(2); out.u64(body.dropped); break;
    case "ended": out.u8(3); out.u64(body.run); out.u8(indexOf(END_REASONS, body.reason, "end reason")); break;
    case "unknown": out.u8(body.eventKind); break;
    default: throw new wire.WireError(`no event ${body.type}`);
  }
}

function decodeEvent(input) {
  const kind = input.u8();
  switch (kind) {
    case 0: {
      const fault = {
        type: "fault",
        run: input.u64(),
        tick: input.u32(),
        step: input.u32(),
        node: getOptStr(input),
        covers: getStrs(input),
        consecutive: input.u32(),
        reason: input.str(),
        reasonCut: flag(input, "reason cut"),
        detail: input.str(),
        detailCut: flag(input, "detail cut"),
        code: 0,
        ownCode: false,
      };
      // Added together, after the first version: both are there, or neither is.
      if (input.more()) {
        fault.code = input.u32();
        fault.ownCode = flag(input, "own code");
      }
      return fault;
    }
    case 1: return { type: "telemetry", run: input.u64(), tick: input.u64(), stepCalls: input.u64(), faults: input.u64(), skipped: input.u64() };
    case 2: return { type: "lagged", dropped: input.u64() };
    case 3: {
      const run = input.u64();
      const tag = input.u8();
      if (tag >= END_REASONS.length) throw badTag("end reason", tag);
      return { type: "ended", run, reason: END_REASONS[tag] };
    }
    default: return { type: "unknown", eventKind: kind };
  }
}

// The messages.

/**
 * What a client sends. `kind` is `hello` (with `hello`) or `request` (with `request`).
 * @typedef {{ kind: "hello", hello: Hello } | { kind: "request", request: Request }} ClientMessage
 */

/** Encodes a client message. Codecs are the server's opaque type keys, from its hello. */
export function encodeClient(message, codecs = new wire.Schema()) {
  const out = new wire.Writer();
  if (message.kind === "hello") {
    out.u8(KIND.CLIENT_HELLO);
    encodeHello(out, message.hello);
  } else {
    encodeRequest(out, message.request, codecs);
  }
  return out.finish();
}

/** Decodes a client message: the request's op is read in full. */
export function decodeClient(body, codecs = new wire.Schema()) {
  const input = new wire.Reader(body);
  const kind = input.u8();
  if (kind === KIND.CLIENT_HELLO) return { kind: "hello", hello: decodeHello(input) };
  if (kind === KIND.REQUEST) {
    const id = input.u64();
    const session = input.str();
    if (Buffer.byteLength(session) > SESSION_CAP) throw new wire.WireError(`a session name is over ${SESSION_CAP} bytes`);
    return { kind: "request", request: { id, session, op: decodeOp(input, codecs) } };
  }
  throw new wire.WireError(`unknown message kind ${kind.toString(16)}`);
}

/**
 * What a server sends: `hello`, a `response` (with `id` and either `reply` or `error`), or an
 * `event` (with `sub` and `body`).
 * @typedef {{ kind: "hello", hello: Hello }
 *   | { kind: "response", id: bigint, reply: object } | { kind: "response", id: bigint, error: ServiceErrorBody }
 *   | { kind: "event", sub: bigint, body: object }} ServerMessage
 */

/** Encodes a server message. */
export function encodeServer(message, codecs = new wire.Schema()) {
  const out = new wire.Writer();
  if (message.kind === "hello") {
    out.u8(KIND.SERVER_HELLO);
    encodeHello(out, message.hello);
  } else if (message.kind === "response") {
    out.u8(KIND.RESPONSE);
    out.u64(message.id);
    if (message.error) {
      out.u8(1);
      encodeServiceError(out, message.error);
    } else {
      out.u8(0);
      encodeReply(out, message.reply, codecs);
    }
  } else {
    out.u8(KIND.EVENT);
    out.u64(message.sub);
    encodeEvent(out, message.body);
  }
  return out.finish();
}

/** Decodes a server message. Trailing bytes after a whole message are ignored. */
export function decodeServer(body, codecs = new wire.Schema()) {
  const input = new wire.Reader(body);
  const kind = input.u8();
  if (kind === KIND.SERVER_HELLO) return { kind: "hello", hello: decodeHello(input) };
  if (kind === KIND.RESPONSE) {
    const id = input.u64();
    if (flag(input, "result flag")) return { kind: "response", id, error: decodeServiceError(input) };
    return { kind: "response", id, reply: decodeReply(input, codecs) };
  }
  if (kind === KIND.EVENT) return { kind: "event", sub: input.u64(), body: decodeEvent(input) };
  throw new wire.WireError(`unknown message kind ${kind.toString(16)}`);
}
