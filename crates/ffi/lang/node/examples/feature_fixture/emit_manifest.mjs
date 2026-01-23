import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { Plugin, NodeDef, port, t, syncGroup, SyncPolicy, BackpressureStrategy, shaderImagePath } from "../../daedalus_node/index.mjs";

const outPath = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.join(os.tmpdir(), `demo_node_feat_${process.pid}.manifest.json`);

const outDir = path.dirname(outPath);
fs.mkdirSync(outDir, { recursive: true });

const modulePath = fileURLToPath(new URL("./runtime.mjs", import.meta.url));

const cfgTy = t.struct([{ name: "factor", ty: t.int() }]);
const pointTy = t.struct([
  { name: "x", ty: t.int() },
  { name: "y", ty: t.int() },
]);
const modeTy = t.enum([
  { name: "A", ty: t.int() },
  { name: "B", ty: pointTy },
]);

const imgTy = t.struct([
  { name: "data_b64", ty: t.string() },
  { name: "width", ty: t.int() },
  { name: "height", ty: t.int() },
  { name: "channels", ty: t.int() },
  { name: "dtype", ty: t.string() },
  { name: "layout", ty: t.string() },
]);

// Copy shared WGSL fixtures alongside the manifest so all shaders use `src_path`.
const langRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../.."); // crates/ffi/lang/node
const wgslDir = path.resolve(langRoot, "..", "shaders"); // crates/ffi/lang/shaders
for (const name of ["invert.wgsl", "write_u32.wgsl", "counter.wgsl", "write_one.wgsl", "write_two.wgsl"]) {
  fs.copyFileSync(path.join(wgslDir, name), path.join(outDir, name));
}

const plugin = new Plugin({
  name: "demo_node_feat",
  version: "0.1.1",
  description: "Node feature fixture",
  metadata: { category: "tests" },
});

const nodes = [
  new NodeDef({
    id: "demo_node_feat:add_defaults",
    label: "AddDefaults",
    js_path: modulePath,
    js_function: "add_defaults",
    metadata: { category: "math", lang: "node" },
    inputs: [
      { name: "a", ty: t.int(), const_value: 2 },
      { name: "b", ty: t.int(), const_value: 3 },
    ],
    outputs: [{ name: "out", ty: t.int() }],
  }),
  new NodeDef({
    id: "demo_node_feat:split",
    label: "Split",
    js_path: modulePath,
    js_function: "split",
    feature_flags: ["cpu"],
    metadata: { category: "math" },
    inputs: [{ name: "value", ty: t.int() }],
    outputs: [
      { name: "out0", ty: t.int() },
      { name: "out1", ty: t.int() },
    ],
  }),
  new NodeDef({
    id: "demo_node_feat:scale_cfg",
    label: "ScaleCfg",
    js_path: modulePath,
    js_function: "scale_cfg",
    metadata: { category: "config" },
    inputs: [
      { name: "value", ty: t.int() },
      { name: "cfg", ty: cfgTy },
    ],
    outputs: [{ name: "out", ty: t.int() }],
  }),
  new NodeDef({
    id: "demo_node_feat:make_point",
    label: "MakePoint",
    js_path: modulePath,
    js_function: "make_point",
    metadata: { category: "struct" },
    inputs: [
      { name: "x", ty: t.int() },
      { name: "y", ty: t.int() },
    ],
    outputs: [{ name: "out", ty: pointTy }],
  }),
  new NodeDef({
    id: "demo_node_feat:enum_mode",
    label: "EnumMode",
    js_path: modulePath,
    js_function: "enum_mode",
    metadata: { category: "enum" },
    inputs: [{ name: "value", ty: t.int() }],
    outputs: [{ name: "out", ty: modeTy }],
  }),
  new NodeDef({
    id: "demo_node_feat:ctx_echo",
    label: "CtxEcho",
    js_path: modulePath,
    js_function: "ctx_echo",
    metadata: { category: "ctx" },
    inputs: [{ name: "text", ty: t.string() }],
    outputs: [{ name: "out", ty: t.string() }],
  }),
  new NodeDef({
    id: "demo_node_feat:choose_mode_meta",
    label: "ChooseModeMeta",
    js_path: modulePath,
    js_function: "choose_mode_meta",
    metadata: { category: "meta" },
    inputs: [port("mode", t.string(), { source: "modes", default: "quality" })],
    outputs: [{ name: "out", ty: t.string() }],
  }),
  new NodeDef({
    id: "demo_node_feat:sync_a_only",
    label: "SyncAOnly",
    js_path: modulePath,
    js_function: "sync_a_only",
    sync_groups: [["a"]],
    metadata: { category: "sync" },
    inputs: [
      { name: "a", ty: t.int() },
      { name: "b", ty: t.optional(t.int()) },
    ],
    outputs: [{ name: "out", ty: t.int() }],
  }),
  new NodeDef({
    id: "demo_node_feat:sync_a_only_obj",
    label: "SyncAOnlyObj",
    js_path: modulePath,
    js_function: "sync_a_only_obj",
    sync_groups: [
      syncGroup({
        name: "a_only",
        ports: ["a"],
        policy: SyncPolicy.Latest,
        backpressure: BackpressureStrategy.ErrorOnOverflow,
        capacity: 2,
      }),
    ],
    metadata: { category: "sync" },
    inputs: [
      { name: "a", ty: t.int() },
      { name: "b", ty: t.optional(t.int()) },
    ],
    outputs: [{ name: "out", ty: t.int() }],
  }),
  new NodeDef({
    id: "demo_node_feat:accum",
    label: "Accum",
    js_path: modulePath,
    js_function: "accum",
    stateful: true,
    state: { ty: t.struct([{ name: "total", ty: t.int() }]) },
    metadata: { category: "state" },
    inputs: [{ name: "value", ty: t.int() }],
    outputs: [{ name: "out", ty: t.int() }],
  }),
  new NodeDef({
    id: "demo_node_feat:gpu_required_placeholder",
    label: "GpuRequiredPlaceholder",
    js_path: modulePath,
    js_function: "add_defaults",
    default_compute: "GpuRequired",
    metadata: { category: "gpu" },
    inputs: [{ name: "x", ty: t.int(), const_value: 1 }],
    outputs: [{ name: "out", ty: t.int() }],
  }),
  new NodeDef({
    id: "demo_node_feat:shader_invert",
    label: "ShaderInvert",
    js_path: modulePath,
    js_function: "shader_invert",
    shader: shaderImagePath("invert.wgsl", { name: "invert" }),
    metadata: { category: "gpu" },
    inputs: [{ name: "img", ty: imgTy }],
    outputs: [{ name: "img", ty: imgTy }],
  }),
  new NodeDef({
    id: "demo_node_feat:shader_write_u32",
    label: "ShaderWriteU32",
    js_path: modulePath,
    js_function: "shader_write_u32",
    shader: {
      src_path: "write_u32.wgsl",
      entry: "main",
      name: "write_u32",
      invocations: [1, 1, 1],
      bindings: [
        {
          binding: 0,
          kind: "storage_buffer",
          access: "read_write",
          readback: true,
          to_port: "out",
          size_bytes: 4,
        },
      ],
    },
    metadata: { category: "gpu" },
    inputs: [],
    outputs: [{ name: "out", ty: t.bytes() }],
  }),
  new NodeDef({
    id: "demo_node_feat:shader_counter",
    label: "ShaderCounter",
    js_path: modulePath,
    js_function: "shader_counter",
    stateful: true,
    shader: {
      src_path: "counter.wgsl",
      entry: "main",
      name: "counter",
      invocations: [1, 1, 1],
      bindings: [
        {
          binding: 0,
          kind: "storage_buffer",
          access: "read_write",
          from_state: "counter",
          to_state: "counter",
          readback: true,
          to_port: "out",
          size_bytes: 4,
        },
      ],
    },
    metadata: { category: "gpu" },
    inputs: [],
    outputs: [{ name: "out", ty: t.bytes() }],
  }),
  new NodeDef({
    id: "demo_node_feat:shader_counter_cpu",
    label: "ShaderCounterCpu",
    js_path: modulePath,
    js_function: "shader_counter_cpu",
    stateful: true,
    shader: {
      src_path: "counter.wgsl",
      entry: "main",
      name: "counter_cpu",
      invocations: [1, 1, 1],
      bindings: [
        {
          binding: 0,
          kind: "storage_buffer",
          access: "read_write",
          from_state: "counter_cpu",
          to_state: "counter_cpu",
          readback: true,
          to_port: "out",
          size_bytes: 4,
        },
      ],
    },
    metadata: { category: "gpu" },
    inputs: [],
    outputs: [{ name: "out", ty: t.bytes() }],
  }),
  new NodeDef({
    id: "demo_node_feat:shader_counter_gpu",
    label: "ShaderCounterGpu",
    js_path: modulePath,
    js_function: "shader_counter_gpu",
    stateful: true,
    shader: {
      src_path: "counter.wgsl",
      entry: "main",
      name: "counter_gpu",
      invocations: [1, 1, 1],
      bindings: [
        {
          binding: 0,
          kind: "storage_buffer",
          access: "read_write",
          state_backend: "gpu",
          from_state: "counter_gpu",
          readback: true,
          to_port: "out",
          size_bytes: 4,
        },
      ],
    },
    metadata: { category: "gpu" },
    inputs: [],
    outputs: [{ name: "out", ty: t.bytes() }],
  }),
  new NodeDef({
    id: "demo_node_feat:shader_multi_write",
    label: "ShaderMultiWrite",
    js_path: modulePath,
    js_function: "shader_multi_write",
    shader: {
      shaders: [
        { name: "one", src_path: "write_one.wgsl", entry: "main" },
        { name: "two", src_path: "write_two.wgsl", entry: "main" },
      ],
      dispatch_from_port: "which",
      invocations: [1, 1, 1],
      bindings: [
        {
          binding: 0,
          kind: "storage_buffer",
          access: "read_write",
          readback: true,
          to_port: "out",
          size_bytes: 4,
        },
      ],
    },
    metadata: { category: "gpu" },
    inputs: [{ name: "which", ty: t.string() }],
    outputs: [{ name: "out", ty: t.bytes() }],
  }),
  new NodeDef({
    id: "demo_node_feat:multi_emit",
    label: "MultiEmit",
    js_path: modulePath,
    js_function: "multi_emit",
    raw_io: true,
    metadata: { category: "raw_io" },
    inputs: [],
    outputs: [{ name: "out", ty: t.int() }],
  }),
  new NodeDef({
    id: "demo_node_feat:cap_add",
    label: "CapAdd",
    capability: "Add",
    metadata: { category: "capability" },
    inputs: [
      { name: "a", ty: t.int() },
      { name: "b", ty: t.int() },
    ],
    outputs: [{ name: "out", ty: t.int() }],
  }),
];

for (const n of nodes) plugin.register(n);

plugin.emitManifest(outPath);
process.stdout.write(outPath + "\n");
