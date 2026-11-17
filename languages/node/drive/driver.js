// The Node driver for the daemon's service client, under the drive contract
// (testing/crates/drive-conformance/src/lib.rs). It is run as `node driver.js ENDPOINT`, reads one
// command per line on stdin, and writes one answer line per command on stdout. The codec mode is
// `node driver.js --recode client|server FILE`: it decodes and re-encodes a corpus file's bodies.
//
// The client is `../sdk/src/client.js`, and the answers are the reference's text (`show.js`).

import fs from "node:fs";
import { pathToFileURL } from "node:url";
import * as client from "../sdk/src/client.js";
import * as svc from "../sdk/src/service.js";
import * as show from "./show.js";
import { Usage, parse } from "./syntax.js";

/** An endpoint: `tcp:ADDR` (a loopback address with a port) or `unix:PATH`. */
export function parseEndpoint(text) {
  if (text.startsWith("tcp:")) {
    const addr = text.slice(4);
    const at = addr.lastIndexOf(":");
    if (at < 0) throw new Error(`\`${addr}\` is no address`);
    const host = addr.slice(0, at).replace(/^\[(.*)\]$/, "$1");
    const port = Number(addr.slice(at + 1));
    if (!Number.isInteger(port)) throw new Error(`\`${addr}\` is no address`);
    return { host, port };
  }
  if (text.startsWith("unix:")) return { socket: text.slice(5) };
  throw new Error(`\`${text}\` is no endpoint`);
}

/** The answer for one command, and the state a scenario builds up across them. */
export class Driver {
  constructor(root) {
    this.root = root;
    this.current = root;
    this.watches = new Map();
    this.blobs = new Map();
  }

  /** The one output line for one input line. */
  async answer(line) {
    let cmd;
    try {
      cmd = parse(line);
    } catch (error) {
      if (error instanceof Usage) return show.error("usage");
      throw error;
    }
    try {
      return await this.run(cmd);
    } catch (error) {
      if (error instanceof Usage) return show.error("usage");
      if (error instanceof client.ServiceError) return show.error(error.code);
      return show.error("local");
    }
  }

  async run(cmd) {
    switch (cmd.cmd) {
      case "ping": return show.pong(await this.current.ping());
      case "session": {
        const name = cmd.name === "" ? svc.DEFAULT_SESSION : cmd.name;
        this.current = this.root.session(name);
        return show.session(name);
      }
      case "load_plugin": return show.plugin(await this.current.loadPlugin(cmd.path));
      case "put_graph": {
        const json = readText(cmd.file);
        return show.graph(cmd.name, await this.current.putGraph(cmd.name, json));
      }
      case "patch_graph": {
        const patch = readText(cmd.file);
        return show.graph(cmd.name, await this.current.patchGraph(cmd.name, patch));
      }
      case "plan": return show.plan(await this.current.plan(cmd.graph, cmd.options));
      case "blob":
        this.blobs.set(cmd.name, cmd.bytes);
        return show.blob(cmd.name, cmd.bytes.length);
      case "make_plan": {
        const made = await this.current.makePlan(cmd.graph, cmd.config);
        this.blobs.set(cmd.blob, Buffer.from(made.bytes));
        return show.made(made.hash, made.bytes.length);
      }
      case "put_plan": {
        const bytes = this.blobs.get(cmd.blob);
        if (bytes === undefined) throw new Usage(cmd.blob);
        await this.current.putPlan(cmd.name, bytes);
        return show.ack();
      }
      case "start": return show.started((await this.current.start(cmd.graph, cmd.config)).id);
      case "feed": {
        const fed = await this.runFor(cmd.run).feed(cmd.port, cmd.value);
        return show.fed(fed.woke, fed.dropped);
      }
      case "step": return show.stepped(await this.runFor(cmd.run).step(cmd.count));
      case "take": return show.taken(await this.runFor(cmd.run).take(cmd.port));
      case "watch": {
        const previous = this.watches.get(String(cmd.id));
        const target = cmd.target === "all" ? "all" : { run: cmd.target.run };
        const watch = await this.current.watch(target, cmd.kinds);
        if (previous) previous.close();
        this.watches.set(String(cmd.id), watch);
        return show.watching(cmd.id);
      }
      case "next": {
        const event = await this.watchFor(cmd.id).next();
        return event === null ? show.streamEnded(cmd.id) : show.event(cmd.id, event);
      }
      case "unwatch":
        this.watchFor(cmd.id).close();
        return show.unwatched(cmd.id);
      case "reload": {
        const report = await this.runFor(cmd.run).reload(cmd.graph);
        return show.reloaded(report);
      }
      case "cancel":
        await this.runFor(cmd.run).cancel();
        return show.ack();
      case "stop": {
        const stopped = await this.runFor(cmd.run).stop(cmd.mode);
        return show.stopped(stopped.tick, stopped.dropped);
      }
      case "inspect": return this.inspect(cmd.target);
      case "cache_clear":
        await this.current.cacheClear();
        return show.ack();
      case "trace": return show.trace(await this.current.trace(cmd.limit));
      default: throw new Usage(cmd.cmd);
    }
  }

  /** A run handle for an id the scenario gave. The id stays exact, as a BigInt. */
  runFor(id) {
    return new client.Run(this.current, id);
  }

  watchFor(id) {
    const watch = this.watches.get(String(id));
    if (watch === undefined) throw new Usage(`watch ${id}`);
    return watch;
  }

  async inspect(target) {
    switch (target.kind) {
      case "run": return show.inspectRun(await this.current.inspectRun(target.run));
      case "telemetry": return show.inspectTelemetry(await this.current.inspectTelemetry(target.run));
      case "graph": {
        const found = await this.current.inspectGraph(target.name);
        return show.inspectGraph(found.name, found.json);
      }
      case "cache": return show.inspectCache(await this.current.inspectCache());
      case "host": return show.inspectHost(await this.current.inspectHost());
      default: throw new Usage(target.kind);
    }
  }
}

function readText(file) {
  try {
    return fs.readFileSync(file, "utf8");
  } catch {
    throw new client.ClientError("local", `cannot read ${file}`);
  }
}

/** Codec mode: each body of a corpus file, decoded as a client or server message and encoded again. */
export function recode(kind, path) {
  const bytes = fs.readFileSync(path);
  const lines = [];
  let rest = bytes;
  while (rest.length > 0) {
    if (rest.length < 4) throw new Error("a body length is cut short");
    const len = rest.readUInt32LE(0);
    if (rest.length < 4 + len) throw new Error("a body is cut short");
    const body = rest.subarray(4, 4 + len);
    rest = rest.subarray(4 + len);
    let line;
    try {
      const msg = kind === "client" ? svc.decodeClient(body) : svc.decodeServer(body);
      line = (kind === "client" ? svc.encodeClient(msg) : svc.encodeServer(msg)).toString("hex");
    } catch {
      line = "error decode";
    }
    lines.push(line);
  }
  return lines;
}

function readStdin() {
  return new Promise((resolve, reject) => {
    const chunks = [];
    process.stdin.on("data", (chunk) => chunks.push(chunk));
    process.stdin.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    process.stdin.on("error", reject);
  });
}

async function main(args) {
  if (args[0] === "--recode") {
    if (args.length !== 3) throw new Error("usage: --recode client|server FILE");
    const lines = recode(args[1], args[2]);
    process.stdout.write(lines.map((line) => `${line}\n`).join(""));
    return;
  }
  if (args.length !== 1) throw new Error("usage: ENDPOINT | --recode client|server FILE");
  const text = await readStdin();
  const endpoint = parseEndpoint(args[0]);
  const root = await client.connect(endpoint);
  const driver = new Driver(root);
  const out = [];
  try {
    for (const line of text.split("\n")) {
      if (line === "") continue;
      out.push(`${await driver.answer(line)}\n`);
    }
  } finally {
    root.close();
  }
  process.stdout.write(out.join(""));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error && error.stack ? error.stack : error}\n`);
    process.exitCode = 1;
  });
}
