// One command line of a scenario, as the drive contract (testing/crates/drive-conformance) states
// it. Tokens are separated by one space. The last token of `feed` is the rest of the line.

/** A line that is not a command the contract has, or names what the scenario never made. */
export class Usage extends Error {}

const U64_MAX = (1n << 64n) - 1n;
const INT = /^[+-]?\d+$/;

function unsigned(text, max) {
  if (!/^\+?\d+$/.test(text)) throw new Usage(text);
  const value = BigInt(text.replace(/^\+/, ""));
  if (value > max) throw new Usage(text);
  return value;
}

function u32(text) {
  return Number(unsigned(text, 0xffffffffn));
}

function u64(text) {
  return unsigned(text, U64_MAX);
}

function priority(word) {
  if (!word.startsWith("priority=")) throw new Usage(word);
  return word.slice("priority=".length).split(",");
}

const TELEMETRY = { off: "off", basic: "basic", detailed: "detailed", trace: "trace" };

/** Start options: `fifo=N`, `faults=N`, `telemetry=LEVEL`, `unmerged`, `priority=A,B`. */
function startConfig(words) {
  const config = { fifoCapacity: 0, faultReports: 0, telemetry: "basic", unmerged: false, priority: [] };
  for (const word of words) {
    if (word === "unmerged") config.unmerged = true;
    else if (word.startsWith("fifo=")) config.fifoCapacity = u32(word.slice(5));
    else if (word.startsWith("faults=")) config.faultReports = u32(word.slice(7));
    else if (word.startsWith("telemetry=")) {
      const level = TELEMETRY[word.slice(10)];
      if (level === undefined) throw new Usage(word);
      config.telemetry = level;
    } else if (word.startsWith("priority=")) config.priority = priority(word);
    else throw new Usage(word);
  }
  return config;
}

function planOptions(form, words) {
  if (!["steps", "blob", "source"].includes(form)) throw new Usage(form);
  const options = { form, inspect: false, priority: [] };
  for (const word of words) {
    if (word === "inspect") options.inspect = true;
    else options.priority = priority(word);
  }
  return options;
}

function eventKinds(words) {
  if (words.length === 0 || (words.length === 1 && words[0] === "all")) return "all";
  if (words.length === 1 && (words[0] === "fault" || words[0] === "telemetry")) return words[0];
  throw new Usage("watch kinds are fault, telemetry or all");
}

const TYPED_INT = {
  i8: ["i8", -128n, 127n], i16: ["i16", -32768n, 32767n], i32: ["i32", -2147483648n, 2147483647n],
  i64: ["i64", -(1n << 63n), (1n << 63n) - 1n], isize: ["isize", -(1n << 63n), (1n << 63n) - 1n],
  u8: ["u8", 0n, 255n], u16: ["u16", 0n, 65535n], u32: ["u32", 0n, 0xffffffffn],
  u64: ["u64", 0n, U64_MAX], usize: ["usize", 0n, U64_MAX],
};

function int(text, min, max) {
  if (!INT.test(text)) throw new Usage(text);
  const value = BigInt(text.replace(/^\+/, ""));
  if (value < min || value > max) throw new Usage(text);
  return value;
}

function unhex(text) {
  if (text.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(text)) return null;
  return Buffer.from(text, "hex");
}

/**
 * A typed value of the suite: `TYPE:VALUE`, as `daedalus-ctl` reads it. Floats are not part of the
 * suite. A 64-bit integer is a BigInt, and a smaller one a number, as the wire codec takes them.
 */
export function parseValue(text) {
  const at = text.indexOf(":");
  const name = at < 0 ? text : text.slice(0, at);
  const rest = at < 0 ? "" : text.slice(at + 1);
  if (name === "unit") return { ty: { scalar: "unit" }, value: null };
  if (name === "bool") {
    if (rest !== "true" && rest !== "false") throw new Usage(text);
    return { ty: { scalar: "bool" }, value: rest === "true" };
  }
  if (name === "string") return { ty: { scalar: "string" }, value: rest };
  if (name === "bytes") {
    const bytes = unhex(rest);
    if (bytes === null) throw new Usage(text);
    return { ty: { scalar: "bytes" }, value: bytes };
  }
  const spec = TYPED_INT[name];
  if (spec === undefined) throw new Usage(text);
  const [scalar, min, max] = spec;
  const value = int(rest, min, max);
  const wide = scalar === "i64" || scalar === "isize" || scalar === "u64" || scalar === "usize";
  return { ty: { scalar }, value: wide ? value : Number(value) };
}

/**
 * Reads one command line. Returns `{ cmd, ... }`, where `cmd` names the command and the other
 * fields are its arguments. Throws `Usage` for a line that is not a command.
 */
export function parse(line) {
  const words = line.split(" ");
  const [head, ...rest] = words;
  switch (head) {
    case "ping": if (words.length === 1) return { cmd: "ping" }; break;
    case "session":
      if (words.length === 1) return { cmd: "session", name: "" };
      if (words.length === 2) return { cmd: "session", name: rest[0] };
      break;
    case "load_plugin": if (words.length === 2) return { cmd: "load_plugin", path: rest[0] }; break;
    case "put_graph": if (words.length === 3) return { cmd: "put_graph", name: rest[0], file: rest[1] }; break;
    case "patch_graph": if (words.length === 3) return { cmd: "patch_graph", name: rest[0], file: rest[1] }; break;
    case "plan":
      if (words.length >= 3) return { cmd: "plan", graph: rest[0], options: planOptions(rest[1], rest.slice(2)) };
      break;
    case "blob":
      if (words.length === 3) {
        const bytes = unhex(rest[1]);
        if (bytes === null) throw new Usage(line);
        return { cmd: "blob", name: rest[0], bytes };
      }
      break;
    case "make_plan":
      if (words.length >= 3) return { cmd: "make_plan", graph: rest[0], blob: rest[1], config: startConfig(rest.slice(2)) };
      break;
    case "put_plan": if (words.length === 3) return { cmd: "put_plan", name: rest[0], blob: rest[1] }; break;
    case "start":
      if (words.length >= 2) return { cmd: "start", graph: rest[0], config: startConfig(rest.slice(1)) };
      break;
    case "feed": {
      if (words.length < 4) break;
      const run = u64(rest[0]);
      const typed = words.slice(3).join(" ");
      return { cmd: "feed", run, port: rest[1], value: parseValue(typed) };
    }
    case "step": if (words.length === 3) return { cmd: "step", run: u64(rest[0]), count: u32(rest[1]) }; break;
    case "take": if (words.length === 3) return { cmd: "take", run: u64(rest[0]), port: rest[1] }; break;
    case "watch": {
      if (words.length < 3) break;
      const id = u64(rest[0]);
      const target = rest[1] === "all" ? "all" : { run: u64(rest[1]) };
      return { cmd: "watch", id, target, kinds: eventKinds(rest.slice(2)) };
    }
    case "next": if (words.length === 2) return { cmd: "next", id: u64(rest[0]) }; break;
    case "unwatch": if (words.length === 2) return { cmd: "unwatch", id: u64(rest[0]) }; break;
    case "reload": if (words.length === 3) return { cmd: "reload", run: u64(rest[0]), graph: rest[1] }; break;
    case "cancel": if (words.length === 2) return { cmd: "cancel", run: u64(rest[0]) }; break;
    case "stop":
      if (words.length === 3 && (rest[1] === "drain" || rest[1] === "immediate")) {
        return { cmd: "stop", run: u64(rest[0]), mode: rest[1] };
      }
      break;
    case "inspect":
      if (words[1] === "run" && words.length === 3) return { cmd: "inspect", target: { kind: "run", run: u64(words[2]) } };
      if (words[1] === "telemetry" && words.length === 3) return { cmd: "inspect", target: { kind: "telemetry", run: u64(words[2]) } };
      if (words[1] === "graph" && words.length === 3) return { cmd: "inspect", target: { kind: "graph", name: words[2] } };
      if (words[1] === "cache" && words.length === 2) return { cmd: "inspect", target: { kind: "cache" } };
      if (words[1] === "host" && words.length === 2) return { cmd: "inspect", target: { kind: "host" } };
      break;
    case "cache": if (words.length === 2 && rest[0] === "clear") return { cmd: "cache_clear" }; break;
    case "trace": if (words.length === 2) return { cmd: "trace", limit: u32(rest[0]) }; break;
    default: break;
  }
  throw new Usage(line);
}
