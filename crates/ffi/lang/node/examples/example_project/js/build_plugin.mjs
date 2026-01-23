import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";

// Import the in-repo SDK directly.
const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "../../../../../../..");
const sdk = path.join(repoRoot, "crates", "ffi", "lang", "node", "daedalus_node", "index.mjs");
const { Plugin, NodeDef, t } = await import(`file://${sdk}`);

const out = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.join(os.tmpdir(), "example_node.so");
fs.mkdirSync(path.dirname(out), { recursive: true });

const plugin = new Plugin({ name: "example_node", version: "0.1.1", description: "Node example project" });

// Point to the local nodes module sitting next to this script.
const nodesPath = path.join(path.dirname(new URL(import.meta.url).pathname), "nodes.mjs");
plugin.runtime({ js_path: nodesPath });

plugin.register(
  new NodeDef({
    id: "example_node:add",
    js_function: "add",
    inputs: [
      { name: "a", ty: t.int() },
      { name: "b", ty: t.int() },
    ],
    outputs: [{ name: "out", ty: t.int() }],
    metadata: { lang: "node", kind: "stateless" },
  }),
);

plugin.register(
  new NodeDef({
    id: "example_node:split",
    js_function: "split",
    inputs: [{ name: "value", ty: t.int() }],
    outputs: [
      { name: "out0", ty: t.int() },
      { name: "out1", ty: t.int() },
    ],
    metadata: { lang: "node", kind: "multi_output" },
  }),
);

plugin.register(
  new NodeDef({
    id: "example_node:counter",
    js_function: "counter",
    inputs: [{ name: "inc", ty: t.int() }],
    outputs: [{ name: "out", ty: t.int() }],
    stateful: true,
    state: { start: 0 },
    metadata: { lang: "node", kind: "stateful" },
  }),
);

const artifact = plugin.build({ out_path: out, out_name: "example_node", bundle: true, bundle_deps: true, release: true });
process.stdout.write(artifact + "\n");
