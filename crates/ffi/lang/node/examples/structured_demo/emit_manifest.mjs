import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { Plugin, node, outputs, port, t } from "../../daedalus_node/index.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const outPath = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.join(os.tmpdir(), `demo_node_struct_${process.pid}.manifest.json`);

const modulePath = path.join(__dirname, "runtime.mjs");

const pointTy = t.struct([
  { name: "x", ty: t.int() },
  { name: "y", ty: t.int() },
]);

const modeTy = t.enum([
  { name: "A", ty: t.int() },
  { name: "B", ty: pointTy },
]);

export const translate_point = node({
  id: "demo_node_struct:translate_point",
  label: "TranslatePoint",
  path: modulePath,
  func: "translate_point",
  inputs: [
    port("pt", pointTy),
    port("dx", t.int(), { default: 1 }),
    port("dy", t.int(), { default: -1 }),
  ],
  outputs: outputs(port("out", pointTy)),
})(function translate_point_impl(_pt, _dx, _dy) {});

export const flip_mode = node({
  id: "demo_node_struct:flip_mode",
  label: "FlipMode",
  path: modulePath,
  func: "flip_mode",
  inputs: [port("mode", modeTy)],
  outputs: outputs(port("out", modeTy)),
})(function flip_mode_impl(_mode) {});

export const map_len = node({
  id: "demo_node_struct:map_len",
  label: "MapLen",
  path: modulePath,
  func: "map_len",
  inputs: [port("m", t.map(t.string(), t.int()))],
  outputs: outputs(port("out", t.int())),
})(function map_len_impl(_m) {});

export const list_sum = node({
  id: "demo_node_struct:list_sum",
  label: "ListSum",
  path: modulePath,
  func: "list_sum",
  inputs: [port("items", t.list(t.int()))],
  outputs: outputs(port("out", t.int())),
})(function list_sum_impl(_items) {});

const plugin = new Plugin({
  name: "demo_node_struct",
  version: "0.1.0",
  description: "Structured demo",
});
plugin.register(translate_point.__daedalus_node__);
plugin.register(flip_mode.__daedalus_node__);
plugin.register(map_len.__daedalus_node__);
plugin.register(list_sum.__daedalus_node__);

plugin.emitManifest(outPath);
console.log(outPath);
