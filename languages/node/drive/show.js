// The canonical text of each answer: the bytes the contract (testing/crates/drive-conformance)
// states for each answer, so a driver in any language prints the same line for the same answer.

const bool = (flag) => (flag ? "true" : "false");

/** A string in quotes. Inside: `\"`, `\\`, `\n`, `\r`, `\t`; other code points outside 0x20 to 0x7e as `\u{H}`. */
export function quote(text) {
  let out = '"';
  for (const ch of text) {
    const code = ch.codePointAt(0);
    if (ch === '"') out += '\\"';
    else if (ch === "\\") out += "\\\\";
    else if (ch === "\n") out += "\\n";
    else if (ch === "\r") out += "\\r";
    else if (ch === "\t") out += "\\t";
    else if (code >= 0x20 && code <= 0x7e) out += ch;
    else out += `\\u{${code.toString(16)}}`;
  }
  return `${out}"`;
}

/** A list: `[`, the quoted items joined by commas, `]`. */
export function list(items) {
  return `[${items.map((item) => quote(String(item))).join(",")}]`;
}

const SCALAR_NAMES = new Set([
  "bool", "i8", "i16", "i32", "i64", "isize", "u8", "u16", "u32", "u64", "usize", "string", "bytes", "unit",
]);

/** A value as `TYPE:VALUE`. A float or a compound value is `other`: the suite has none. */
export function value(typed) {
  const name = typed.ty.scalar;
  if (!SCALAR_NAMES.has(name)) return "other";
  const v = typed.value;
  switch (name) {
    case "unit": return "unit:";
    case "bool": return `bool:${bool(v)}`;
    case "string": return `string:${v}`;
    case "bytes": return `bytes:${Buffer.from(v).toString("hex")}`;
    default: return `${name}:${v}`;
  }
}

const SEVERITY = { error: "error", warning: "warning" };

function diagnostics(found) {
  return list(found.map((item) => `${SEVERITY[item.severity]}|${item.code}`));
}

export const error = (code) => `error ${code}`;
export const pong = (version) => `pong version=${version}`;
export const session = (name) => `session name=${quote(name)}`;
export const ack = () => "ack";
export const watching = (id) => `watching ${id}`;
export const unwatched = (id) => `unwatched ${id}`;
export const streamEnded = (id) => `stream ${id} ended`;
export const blob = (name, length) => `blob name=${quote(name)} bytes=${length}`;
export const made = (hash, length) => `made hash=${hash} bytes=${length}`;
export const started = (run) => `started run=${run}`;
export const fed = (woke, dropped) => `fed woke=${woke} dropped=${bool(dropped)}`;
export const stopped = (tick, dropped) => `stopped tick=${tick} dropped=${dropped}`;

export const plugin = (info) =>
  `plugin name=${quote(info.name)} generation=${info.generation} nodes=${list(info.nodes)} backends=${list(info.backends)}`;

export const graph = (name, found) => `graph name=${quote(name)} diagnostics=${diagnostics(found)}`;

export function plan(summary) {
  const explain = summary.explain == null ? "none" : quote(summary.explain);
  return (
    `plan graph=${quote(summary.graph)} hash=${summary.hash} form=${summary.form} nodes=${summary.nodes} ` +
    `edges=${summary.edges} regions=${summary.regions} steps=${summary.steps} cached=${bool(summary.cached)} ` +
    `diagnostics=${diagnostics(summary.diagnostics)} explain=${explain}`
  );
}

export function stepped(summary) {
  return (
    `stepped ticks=${summary.ticks} tick=${summary.tick} step_calls=${summary.stepCalls} ` +
    `faults=${summary.faults} skipped=${summary.skipped} idle=${bool(summary.idle)}`
  );
}

export function taken(typed) {
  return typed === null ? "value none" : `value ${value(typed)}`;
}

function names(found) {
  return `${list(found.names)}/${found.total}`;
}

export function reloaded(report) {
  return (
    `reloaded carried=${names(report.carried)} fresh=${names(report.fresh)} added=${names(report.added)} ` +
    `removed=${names(report.removed)} removed_ports=${names(report.removedPorts)} dropped_values=${report.droppedValues}`
  );
}

export function inspectRun(info) {
  const ports = info.ports.map((port) => `${port.name}|${port.input ? "input" : "output"}|${port.typeKey}`);
  return (
    `inspect run run=${info.run} session=${quote(info.session)} graph=${quote(info.graph)} state=${info.state} ` +
    `tick=${info.tick} faults=${info.faults} ports=${list(ports)} nodes=${list(info.nodes)}`
  );
}

export function inspectGraph(name, json) {
  return `inspect graph name=${quote(name)} json=${quote(json)}`;
}

export function inspectCache(info) {
  return `inspect cache entries=${info.entries} hits=${info.hits} misses=${info.misses}`;
}

export function inspectTelemetry(info) {
  const rows = info.rows.map(
    (row) =>
      `${row.step}|${row.nodes.join("+")}|${row.calls}|${row.failures}|${bool(row.backingOff)}|` +
      `${bool(row.disabled)}|${bool(row.autoOff)}`,
  );
  return (
    `inspect telemetry run=${info.run} level=${info.level} tick=${info.tick} step_calls=${info.stepCalls} ` +
    `faults=${info.faults} skipped=${info.skipped} rows=${list(rows)}`
  );
}

export function inspectHost(info) {
  const runs = info.runs.map((run) => `${run.run}|${run.session}|${run.graph}|${run.state}`);
  return (
    `inspect host sessions=${list(info.sessions)} graphs=${list(info.graphs)} runs=${list(runs)} ` +
    `plugins=${list(info.plugins)}`
  );
}

export function trace(entries) {
  const items = entries.map((entry) => `${entry.seq}|${entry.op}|${entry.target}|${entry.error ?? "none"}`);
  return `trace entries=${list(items)}`;
}

/** A line for an event read from watch `id`. */
export function event(id, body) {
  switch (body.type) {
    case "fault": {
      const node = body.node == null ? "none" : quote(body.node);
      return (
        `event ${id} fault run=${body.run} tick=${body.tick} step=${body.step} node=${node} ` +
        `covers=${list(body.covers)} consecutive=${body.consecutive} reason=${quote(body.reason)} ` +
        `reason_cut=${bool(body.reasonCut)} detail=${quote(body.detail)} detail_cut=${bool(body.detailCut)} ` +
        `code=${body.code} own_code=${bool(body.ownCode)}`
      );
    }
    case "telemetry":
      return (
        `event ${id} telemetry run=${body.run} tick=${body.tick} step_calls=${body.stepCalls} ` +
        `faults=${body.faults} skipped=${body.skipped}`
      );
    case "lagged": return `event ${id} lagged dropped=${body.dropped}`;
    case "ended": return `event ${id} ended run=${body.run} reason=${body.reason}`;
    default: return `event ${id} unknown kind=${body.eventKind}`;
  }
}
