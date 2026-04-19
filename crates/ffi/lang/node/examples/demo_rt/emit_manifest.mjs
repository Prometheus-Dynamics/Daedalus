import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { Plugin, node, outputs, port, t } from "../../daedalus_node/index.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const outPath = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.join(os.tmpdir(), `demo_node_${process.pid}.manifest.json`);

const modulePath = path.join(__dirname, "node_demo_module.mjs");

export const add = node({
  id: "demo_node:add",
  label: "Add",
  path: modulePath,
  func: "add_impl",
  inputs: [
    port("a", t.int(), { default: 2 }),
    port("b", t.int(), { default: 3 }),
  ],
  outputs: outputs(port("out", t.int())),
})(function add_impl(a, b) {
  return a + b;
});

const plugin = new Plugin({
  name: "demo_node",
  version: "1.0.0",
  description: "Demo Node nodes",
  metadata: { author: "example" },
});
plugin.register(add.__daedalus_node__);

plugin.emitManifest(outPath);
console.log(outPath);
