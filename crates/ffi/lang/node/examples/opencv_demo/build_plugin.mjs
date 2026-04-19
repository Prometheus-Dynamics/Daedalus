import os from "node:os";
import path from "node:path";
import process from "node:process";

// Import the in-repo SDK directly.
const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "../../../../../../..");
const sdk = path.join(repoRoot, "crates", "ffi", "lang", "node", "daedalus_node", "index.mjs");
const { Plugin, NodeDef, t } = await import(`file://${sdk}`);

const out = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.join(os.tmpdir(), "demo_node_opencv.so");

const plugin = new Plugin({
  name: "demo_node_opencv",
  version: "1.0.0",
  description: "OpenCV image demo",
});

const rtPath = path.join(path.dirname(new URL(import.meta.url).pathname), "runtime.mjs");
plugin.runtime({ js_path: rtPath });

const imageTy = t.struct([
  { name: "data_b64", ty: t.string() },
  { name: "width", ty: t.int() },
  { name: "height", ty: t.int() },
  { name: "channels", ty: t.int() },
  { name: "dtype", ty: t.string() },
  { name: "layout", ty: t.string() },
  { name: "encoding", ty: t.string() },
]);

plugin.register(
  new NodeDef({
    id: "demo_node_opencv:blur",
    js_function: "blur",
    inputs: [{ name: "img", ty: imageTy }],
    outputs: [{ name: "out", ty: imageTy }],
    metadata: { lang: "node", kind: "opencv" },
  }),
);

const artifact = plugin.build({
  out_path: out,
  out_name: "demo_node_opencv",
  bundle: true,
  bundle_deps: true,
  release: true,
});
process.stdout.write(artifact + "\n");

