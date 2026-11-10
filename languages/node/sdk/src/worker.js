// The worker loop: reads host packets from fd 0, runs node bodies, and answers on fd 1.
//
// Node bodies see console.log and console.info go to stderr, so stdout carries packets only.
// A body throws NodeError(code, message) for a fault with that code; any other throw is a
// node_error whose reason is the stack, cut to 512 bytes. Events a body reports go out, in order,
// before the run's outputs. A cancel is read after the run it names, since the loop reads one
// packet at a time.

import fs from "node:fs";
import util from "node:util";
import * as wire from "./wire.js";

export const NODES = [];

/** Declares a node. `spec` has `inputs`, `outputs` (lists of [name, type]), and optionally `state`. */
export function node(id, spec, fn) {
  for (const [, ty] of [...spec.inputs, ...spec.outputs]) wire.checkType(ty);
  if (spec.state) wire.checkType(spec.state);
  NODES.push({ id, inputs: spec.inputs, outputs: spec.outputs, state: spec.state ?? null, fn, stateValue: undefined });
  return fn;
}

function fault(error) {
  if (error instanceof wire.NodeError) return [error.code, error.message];
  return ["node_error", String(error && error.stack ? error.stack : error)];
}

function writeOut(packet) {
  wire.writePacket(1, packet, fs.writeSync);
}

function flushEvents(run, events) {
  for (const event of events) {
    if (event.kind === "progress") writeOut(wire.encodeProgress(run, 0, event.message, event.fraction));
    else writeOut(wire.encodeLog(event.level, event.text));
  }
}

export function serve(nodes = NODES, { worker = "daedalus-node-worker" } = {}) {
  console.log = (...args) => process.stderr.write(util.format(...args) + "\n");
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

  for (;;) {
    let body;
    try {
      body = wire.readPacket(0, wire.DEFAULT_CAP, fs.readSync);
    } catch (error) {
      return;
    }
    if (body === null) return;
    let msg;
    try {
      msg = wire.decodeHost(body, schema);
    } catch (error) {
      writeOut(wire.encodeFault(0, null, "malformed_packet", String(error.message)));
      continue;
    }
    if (msg.kind === "run") runRun(msg, byId, schema);
    else if (msg.kind === "ping") writeOut(wire.encodePong());
    else if (msg.kind === "state_export") {
      const declared = byId.get(msg.node);
      if (declared && declared.state) {
        writeOut(wire.encodeStateValue(msg.node, declared.state, declared.stateValue ?? 0, schema));
      }
    } else if (msg.kind === "state_import") {
      const declared = byId.get(msg.node);
      if (declared) declared.stateValue = msg.value;
    } else if (msg.kind === "shutdown") return;
    // A cancel between runs has nothing to stop.
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

function runRun(msg, byId, schema) {
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
      cancelled: () => false,
      state: declared.stateValue,
      setState(value) { cx.newState = value; },
      progress(fraction, message = "") { events.push({ kind: "progress", fraction, message }); },
      log(level, text) { events.push({ kind: "log", level, text }); },
    };
    let result;
    try {
      result = declared.fn(inputs, cx);
    } catch (error) {
      const [code, reason] = fault(error);
      flushEvents(msg.run, events);
      writeOut(wire.encodeFault(msg.run, id, code, reason));
      status = "fault";
      break;
    }
    flushEvents(msg.run, events);
    if (declared.state && cx.newState !== undefined) declared.stateValue = cx.newState;
    const values = declared.outputs.length === 1 ? [result] : declared.outputs.length === 0 ? [] : result;
    if (!Array.isArray(values) || values.length !== declared.outputs.length) {
      writeOut(wire.encodeFault(msg.run, id, "internal", `${id} returned the wrong number of values`));
      status = "fault";
      break;
    }
    declared.outputs.forEach(([port], i) => outputs.push([id, port, values[i]]));
  }
  if (status === "fault") outputs.length = 0;
  writeOut(wire.encodeOutputs(msg.run, status, outputs, schema));
}
