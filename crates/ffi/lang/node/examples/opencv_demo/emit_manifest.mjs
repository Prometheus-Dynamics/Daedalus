/**
 * Emit a manifest that demonstrates passing an image-like payload into Node, processing it with
 * OpenCV, and returning it back to Rust.
 *
 * Notes:
 * - Requires an OpenCV binding such as `opencv4nodejs` available at runtime.
 * - The image carrier is a struct `{data_b64,width,height,channels,dtype,layout,encoding}` where
 *   `data_b64` carries bytes; when `encoding=="raw"`, it is raw pixels (HWC, u8).
 */
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { Plugin, node, outputs, port, t } from "../../daedalus_node/index.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const outPath = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.join(os.tmpdir(), `demo_node_opencv_${process.pid}.manifest.json`);

const imageTy = t.struct([
  { name: "data_b64", ty: t.string() },
  { name: "width", ty: t.int() },
  { name: "height", ty: t.int() },
  { name: "channels", ty: t.int() },
  { name: "dtype", ty: t.string() },
  { name: "layout", ty: t.string() },
  { name: "encoding", ty: t.string() },
]);

const modulePath = path.join(__dirname, "runtime.mjs");

export const blur = node({
  id: "demo_node_opencv:blur",
  label: "OpenCV Blur",
  path: modulePath,
  func: "blur",
  inputs: [port("img", imageTy)],
  outputs: outputs(port("out", imageTy)),
})(function blur_impl(_img) {});

const plugin = new Plugin({
  name: "demo_node_opencv",
  version: "0.1.0",
  description: "OpenCV image demo",
});
plugin.register(blur.__daedalus_node__);
plugin.emitManifest(outPath);
console.log(outPath);
