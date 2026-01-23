import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";

// Import the in-repo SDK directly.
const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "../../../../../../..");
const sdk = path.join(repoRoot, "crates", "ffi", "lang", "node", "daedalus_node", "index.mjs");
const { Plugin, NodeDef, t } = await import(`file://${sdk}`);

function copyShaders(outDir) {
  const srcDir = path.join(path.dirname(new URL(import.meta.url).pathname), "shaders");
  const dstDir = path.join(outDir, "shaders");
  fs.mkdirSync(dstDir, { recursive: true });
  for (const name of fs.readdirSync(srcDir)) {
    if (!name.endsWith(".wgsl")) continue;
    fs.copyFileSync(path.join(srcDir, name), path.join(dstDir, name));
  }
}

const out = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.join(os.tmpdir(), `example_node_${process.pid}.manifest.json`);
fs.mkdirSync(path.dirname(out), { recursive: true });
copyShaders(path.dirname(out));

const plugin = new Plugin({ name: "example_node", version: "0.1.1", description: "Node example project" });

// Point to the local nodes module sitting next to this emitter.
const nodesPath = path.join(path.dirname(new URL(import.meta.url).pathname), "nodes.mjs");

plugin.register(
  new NodeDef({
    id: "example_node:add",
    js_path: nodesPath,
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
    js_path: nodesPath,
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
    js_path: nodesPath,
    js_function: "counter",
    inputs: [{ name: "inc", ty: t.int() }],
    outputs: [{ name: "out", ty: t.int() }],
    stateful: true,
    state: { start: 0 },
    metadata: { lang: "node", kind: "stateful" },
  }),
);

// Optional: shader wiring example (leave disabled by default).
// Keep it file-backed: src_path should be relative to the manifest directory.
//
// plugin.register(new NodeDef({
//   id: "example_node:invert_first_u32",
//   js_path: nodesPath,
//   js_function: "add", // placeholder: the node won't call JS when shader is present
//   inputs: [{ name: "in", ty: t.bytes() }],
//   outputs: [{ name: "out", ty: t.bytes() }],
//   shader: {
//     src_path: "shaders/invert.wgsl",
//     entry: "main",
//     name: "invert",
//     invocations: [1, 1, 1],
//     bindings: [
//       { binding: 0, kind: "storage_buffer", access: "read_write", readback: true, to_port: "out", size_bytes: 4 },
//     ],
//   },
// }));

plugin.emitManifest(out);
process.stdout.write(out + "\n");
