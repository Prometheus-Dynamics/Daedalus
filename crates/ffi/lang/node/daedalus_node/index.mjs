import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";

export const t = {
  int: () => ({ Scalar: "Int" }),
  float: () => ({ Scalar: "Float" }),
  bool: () => ({ Scalar: "Bool" }),
  string: () => ({ Scalar: "String" }),
  bytes: () => ({ Scalar: "Bytes" }),
  unit: () => ({ Scalar: "Unit" }),

  optional: (inner) => ({ Optional: inner }),
  list: (inner) => ({ List: inner }),
  tuple: (items) => ({ Tuple: items }),
  map: (k, v) => ({ Map: [k, v] }),
  struct: (fields) => ({ Struct: fields.map((f) => ({ name: f.name, ty: f.ty })) }),
  enum: (variants) => ({
    Enum: variants.map((v) => ({ name: v.name, ty: v.ty ?? null })),
  }),
};

export function port(name, ty, opts = {}) {
  const p = { name, ty };
  if (opts && Object.prototype.hasOwnProperty.call(opts, "default")) {
    p.const_value = opts.default;
  }
  if (opts && Object.prototype.hasOwnProperty.call(opts, "source")) {
    p.source = opts.source;
  }
  if (opts && Object.prototype.hasOwnProperty.call(opts, "const_value")) {
    p.const_value = opts.const_value;
  }
  return p;
}

export function inputs(...ports) {
  return ports.flat();
}

export function outputs(...ports) {
  return ports.flat();
}

export function wgsl(srcOrPath) {
  if (typeof srcOrPath !== "string") {
    throw new TypeError("wgsl() expects a string (path or source)");
  }
  try {
    if (fs.existsSync(srcOrPath) && fs.statSync(srcOrPath).isFile()) {
      return fs.readFileSync(srcOrPath, "utf8");
    }
  } catch {
    // fall through to treat as source
  }
  return srcOrPath;
}

export function shaderImage(srcOrPath, { entry = "main", name = null, workgroup_size = null, input_binding = 0, output_binding = 1 } = {}) {
  const s = {
    src: wgsl(srcOrPath),
    entry,
    input_binding: Number(input_binding),
    output_binding: Number(output_binding),
  };
  if (name != null) s.name = name;
  if (workgroup_size != null) s.workgroup_size = workgroup_size;
  return s;
}

export function shaderImagePath(srcPath, { entry = "main", name = null, workgroup_size = null, input_binding = 0, output_binding = 1 } = {}) {
  if (typeof srcPath !== "string") {
    throw new TypeError("shaderImagePath() expects a string path");
  }
  const s = {
    src_path: srcPath,
    entry,
    input_binding: Number(input_binding),
    output_binding: Number(output_binding),
  };
  if (name != null) s.name = name;
  if (workgroup_size != null) s.workgroup_size = workgroup_size;
  return s;
}

export const SyncPolicy = Object.freeze({
  AllReady: "AllReady",
  Latest: "Latest",
  ZipByTag: "ZipByTag",
});

export const BackpressureStrategy = Object.freeze({
  None: "None",
  BoundedQueues: "BoundedQueues",
  ErrorOnOverflow: "ErrorOnOverflow",
});

export function syncGroup({ ports, name = null, policy = SyncPolicy.AllReady, backpressure = null, capacity = null } = {}) {
  if (!Array.isArray(ports) || ports.length === 0) {
    throw new TypeError("syncGroup({ports:[...]}) requires a non-empty ports array");
  }
  const g = {
    ports: [...ports],
    policy,
  };
  if (name != null) g.name = name;
  if (backpressure != null) g.backpressure = backpressure;
  if (capacity != null) g.capacity = capacity;
  return g;
}

export class NodeDef {
  constructor({
    id,
    label = null,
    js_module = null,
    js_path = null,
    js_function = null,
    raw_io = false,
    stateful = false,
    state = null,
    capability = null,
    shader = null,
    feature_flags = [],
    default_compute = "CpuOnly",
    sync_groups = [],
    metadata = {},
    inputs = [],
    outputs = [],
  }) {
    this.id = id;
    this.label = label;
    this.js_module = js_module;
    this.js_path = js_path;
    this.js_function = js_function;
    this.raw_io = !!raw_io;
    this.stateful = !!stateful;
    this.state = state;
    this.capability = capability;
    this.shader = shader;
    this.feature_flags = feature_flags;
    this.default_compute = default_compute;
    this.sync_groups = sync_groups;
    this.metadata = metadata;
    this.inputs = inputs;
    this.outputs = outputs;
  }

  toManifest() {
    return {
      id: this.id,
      label: this.label,
      js_module: this.js_module,
      js_path: this.js_path,
      js_function: this.js_function,
      raw_io: this.raw_io,
      stateful: this.stateful,
      state: this.state,
      capability: this.capability,
      shader: this.shader,
      feature_flags: this.feature_flags,
      default_compute: this.default_compute,
      sync_groups: this.sync_groups,
      metadata: this.metadata,
      inputs: this.inputs,
      outputs: this.outputs,
    };
  }
}

export class Plugin {
  constructor({ name, version = null, description = null, metadata = {} }) {
    this.name = name;
    this.version = version;
    this.description = description;
    this.metadata = metadata;
    this.nodes = [];
    this.default_js_path = null;
    this.default_js_module = null;
  }

  runtime({ js_path = null, js_module = null } = {}) {
    if (js_path != null) this.default_js_path = js_path;
    if (js_module != null) this.default_js_module = js_module;
    return this;
  }

  register(node) {
    if (!node || !(node instanceof NodeDef)) {
      throw new TypeError("register() expects a NodeDef");
    }
    if (node.js_path == null && this.default_js_path != null) node.js_path = this.default_js_path;
    if (node.js_module == null && this.default_js_module != null) node.js_module = this.default_js_module;
    this.nodes.push(node);
  }

  emitManifest(filePath) {
    const doc = {
      manifest_version: "1",
      language: "node",
      plugin: {
        name: this.name,
        version: this.version,
        description: this.description,
        metadata: this.metadata,
      },
      nodes: this.nodes.map((n) => n.toManifest()),
    };
    const target = path.resolve(filePath);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, JSON.stringify(doc, null, 2), "utf8");
    return target;
  }

  packFromManifest({
    out_name = "generated_node_plugin",
    manifest_path,
    build = true,
    release = false,
    bundle = false,
    bundle_deps = bundle,
  } = {}) {
    const workspace = findWorkspaceRoot();
    const ffiRoot = path.join(workspace, "crates", "ffi");

    const examplePath = path.join(ffiRoot, "examples", `${out_name}.rs`);
    const written = path.resolve(manifest_path);
    const baseDir = path.dirname(written);

    let manifestIncludePath = written;
    let bundleEntries = [];

    if (bundle) {
      const bundleDir = path.join(ffiRoot, "examples", `${out_name}_bundle`);
      fs.mkdirSync(bundleDir, { recursive: true });

      const doc = JSON.parse(fs.readFileSync(written, "utf8"));
      const nodes = Array.isArray(doc.nodes) ? doc.nodes : [];

      const filesToCopyAbs = [];
      const addFile = (p) => {
        if (typeof p !== "string" || !p) return;
        const abs = path.isAbsolute(p) ? p : path.resolve(baseDir, p);
        if (fs.existsSync(abs) && fs.statSync(abs).isFile()) filesToCopyAbs.push(abs);
      };

      for (const n of nodes) {
        if (n && typeof n === "object") {
          addFile(n.js_path ?? n.js_module);
          const shader = n.shader;
          if (shader && typeof shader === "object") {
            addFile(shader.src_path);
            if (Array.isArray(shader.shaders)) {
              for (const s of shader.shaders) {
                if (s && typeof s === "object") addFile(s.src_path);
              }
            }
          }
        }
      }

      const uniqAbs = Array.from(new Set(filesToCopyAbs));
      const mapping = new Map();
      let extIdx = 0;
      for (const abs of uniqAbs) {
        const rel =
          abs.startsWith(baseDir + path.sep)
            ? path.relative(baseDir, abs)
            : path.join("_external", String(extIdx++), path.basename(abs));
        mapping.set(abs, rel);
        const dest = path.join(bundleDir, rel);
        fs.mkdirSync(path.dirname(dest), { recursive: true });
        fs.copyFileSync(abs, dest);
        bundleEntries.push({ rel, abs: dest });
      }

      if (bundle_deps) {
        const pkgDir = path.dirname(new URL(import.meta.url).pathname);
        const bin = process.platform === "win32" ? "esbuild.cmd" : "esbuild";
        const esbuildBin = path.join(pkgDir, "node_modules", ".bin", bin);
        if (!fs.existsSync(esbuildBin)) {
          throw new Error(`esbuild not found at ${esbuildBin}; run 'npm install' in ${pkgDir}`);
        }

        const bundledMap = new Map(); // absEntry -> relOut
        let idx = 0;
        const bundleEntry = (p) => {
          if (typeof p !== "string" || !p) return null;
          const abs = path.isAbsolute(p) ? p : path.resolve(baseDir, p);
          if (!fs.existsSync(abs) || !fs.statSync(abs).isFile()) {
            throw new Error(`bundle_deps requires a file path module, got: ${p}`);
          }
          if (bundledMap.has(abs)) return bundledMap.get(abs);
          const outRel = path.join("_bundle", "js", `${path.basename(abs).replace(/\\W+/g, "_")}.${idx++}.mjs`);
          const outAbs = path.join(bundleDir, outRel);
          fs.mkdirSync(path.dirname(outAbs), { recursive: true });
          const res = spawnSync(
            esbuildBin,
            [
              abs,
              "--bundle",
              "--platform=node",
              "--format=esm",
              "--target=es2022",
              `--outfile=${outAbs}`,
              "--log-level=error",
            ],
            { cwd: baseDir, stdio: "inherit" },
          );
          if (res.status !== 0) {
            throw new Error(`esbuild bundle failed for ${abs} (status=${res.status})`);
          }
          bundledMap.set(abs, outRel);
          bundleEntries.push({ rel: outRel, abs: outAbs });
          return outRel;
        };

        for (const n of nodes) {
          if (!n || typeof n !== "object") continue;
          const mod = n.js_path ?? n.js_module;
          if (!mod) continue;
          const outRel = bundleEntry(mod);
          if (outRel) {
            n.js_path = outRel;
            n.js_module = null;
          }
        }
      }

      for (const n of nodes) {
        if (!n || typeof n !== "object") continue;
        const jsMod = n.js_path ?? n.js_module;
        if (typeof jsMod === "string" && jsMod) {
          const abs = path.isAbsolute(jsMod) ? jsMod : path.resolve(baseDir, jsMod);
          const rel = mapping.get(abs);
          if (rel) {
            if (n.js_path != null) n.js_path = rel;
            else n.js_module = rel;
          }
        }
        const shader = n.shader;
        if (shader && typeof shader === "object") {
          const sp = shader.src_path;
          if (typeof sp === "string" && sp) {
            const abs = path.isAbsolute(sp) ? sp : path.resolve(baseDir, sp);
            const rel = mapping.get(abs);
            if (rel) shader.src_path = rel;
          }
          if (Array.isArray(shader.shaders)) {
            for (const s of shader.shaders) {
              const p = s && typeof s === "object" ? s.src_path : null;
              if (typeof p === "string" && p) {
                const abs = path.isAbsolute(p) ? p : path.resolve(baseDir, p);
                const rel = mapping.get(abs);
                if (rel) s.src_path = rel;
              }
            }
          }
        }
      }

      const bundledManifest = path.join(bundleDir, "manifest.json");
      fs.writeFileSync(bundledManifest, JSON.stringify(doc, null, 2), "utf8");
      manifestIncludePath = bundledManifest;
      bundleEntries = bundleEntries.map((e) => ({ rel: e.rel, abs: path.join(bundleDir, e.rel) }));
    }

    const bundleCode = bundle
      ? `
fn extract_bundle() -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("daedalus_node_bundle_${out_name}_{}_{}", std::process::id(), nanos));
    std::fs::create_dir_all(&dir).expect("create bundle temp dir");
    for (rel, bytes) in BUNDLE_FILES {
        let dest = dir.join(rel);
        if let Some(parent) = dest.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::write(&dest, bytes).expect("write bundled file");
    }
    dir
}

static BUNDLE_FILES: &[(&str, &[u8])] = &[
${bundleEntries
  .map((e) => `    (${JSON.stringify(e.rel)}, include_bytes!(r#\"${e.abs}\"#) as &[u8]),`)
  .join("\n")}
];
`
      : "";

    const exampleSrc = `#![crate_type = "cdylib"]
use daedalus_ffi::export_plugin;
use daedalus_ffi::{NodeManifest, NodeManifestPlugin};
use daedalus_runtime::plugins::{Plugin, PluginRegistry};
use serde_json;

static MANIFEST_JSON: &str = include_str!(r"${manifestIncludePath}");
static BASE_DIR: &str = r"${baseDir}";

${bundleCode}

pub struct GeneratedNodePlugin {
    inner: NodeManifestPlugin,
}

impl Default for GeneratedNodePlugin {
    fn default() -> Self {
        let manifest: NodeManifest = serde_json::from_str(MANIFEST_JSON).expect("invalid embedded manifest");
        let base = if ${bundle ? "true" : "false"} { extract_bundle() } else { std::path::PathBuf::from(BASE_DIR) };
        Self {
            inner: NodeManifestPlugin::from_manifest_with_base(manifest, Some(base)),
        }
    }
}

impl Plugin for GeneratedNodePlugin {
    fn id(&self) -> &'static str {
        self.inner.id()
    }

    fn install(&self, registry: &mut PluginRegistry) -> Result<(), &'static str> {
        self.inner.install(registry)
    }
}

export_plugin!(GeneratedNodePlugin);
`;
    fs.mkdirSync(path.dirname(examplePath), { recursive: true });
    fs.writeFileSync(examplePath, exampleSrc, "utf8");

    const profile = release ? "release" : process.env.PROFILE || "debug";
    const { prefix, ext } = libNaming();
    const artifact = path.join(workspace, "target", profile, "examples", `${prefix}${out_name}${ext}`);

    if (build) {
      const args = ["build", "-p", "daedalus-ffi", "--example", out_name];
      if (release) args.push("--release");
      const res = spawnSync("cargo", args, {
        cwd: workspace,
        stdio: "inherit",
      });
      if (res.status !== 0) throw new Error(`cargo build failed (status=${res.status})`);
      if (!fs.existsSync(artifact)) throw new Error(`expected artifact missing at ${artifact}`);
    }

    return artifact;
  }

  pack({
    out_name = "generated_node_plugin",
    manifest_path = null,
    build = true,
    release = false,
    bundle = false,
    bundle_deps = bundle,
  } = {}) {
    const manifestTarget =
      manifest_path != null ? path.resolve(manifest_path) : path.join(process.cwd(), `${this.name}.manifest.json`);
    const written = this.emitManifest(manifestTarget);
    return this.packFromManifest({ out_name, manifest_path: written, build, release, bundle, bundle_deps });
  }

  packTs({
    project = "tsconfig.json",
    emit_dir = null,
    out_name = "generated_node_plugin",
    manifest_path = null,
    build = true,
    release = false,
    bundle = true,
    bundle_deps = true,
    install = true,
  } = {}) {
    const pkgDir = path.dirname(new URL(import.meta.url).pathname);
    const nodeModules = path.join(pkgDir, "node_modules");
    if (install && !fs.existsSync(nodeModules)) {
      const npm = process.env.NPM || "npm";
      const res = spawnSync(npm, ["install", "--no-audit", "--no-fund"], { cwd: pkgDir, stdio: "inherit" });
      if (res.status !== 0) throw new Error(`npm install failed (status=${res.status})`);
    }

    const tool = path.join(pkgDir, "tools", "emit_manifest_ts.mjs");
    const outPath =
      manifest_path != null ? path.resolve(manifest_path) : path.join(process.cwd(), `${this.name}.manifest.json`);
    const args = [tool, "--project", project, "--out", outPath, "--plugin-name", this.name];
    if (this.version) args.push("--plugin-version", String(this.version));
    if (this.description) args.push("--plugin-description", String(this.description));
    if (this.metadata) args.push("--plugin-metadata", JSON.stringify(this.metadata));
    if (emit_dir) args.push("--emit-dir", emit_dir);

    const node = process.env.NODE || "node";
    const res = spawnSync(node, args, { cwd: process.cwd(), stdio: "inherit" });
    if (res.status !== 0) throw new Error(`daedalus-node-tsc failed (status=${res.status})`);
    return this.packFromManifest({ out_name, manifest_path: outPath, build, release, bundle, bundle_deps });
  }

  build({
    out_path = null,
    out_name = null,
    bundle = true,
    bundle_deps = true,
    release = true,
    keep_intermediates = false,
  } = {}) {
    const tmpBase = fs.mkdtempSync(path.join(os.tmpdir(), `daedalus_node_build_${this.name}_`));
    const manifestTmp = path.join(tmpBase, `${this.name}.manifest.json`);
    const unique = out_name ?? `${this.name}_${process.pid}`;
    const artifact = this.pack({
      out_name: unique,
      manifest_path: manifestTmp,
      build: true,
      release,
      bundle,
      bundle_deps,
    });

    let out = artifact;
    if (out_path != null) {
      fs.mkdirSync(path.dirname(out_path), { recursive: true });
      fs.copyFileSync(artifact, out_path);
      out = out_path;
    }

    if (!keep_intermediates) {
      const workspace = findWorkspaceRoot();
      try {
        fs.rmSync(path.join(workspace, "crates", "ffi", "examples", `${unique}.rs`));
      } catch {}
      try {
        fs.rmSync(path.join(workspace, "crates", "ffi", "examples", `${unique}_bundle`), { recursive: true, force: true });
      } catch {}
      try {
        fs.rmSync(tmpBase, { recursive: true, force: true });
      } catch {}
    }
    return out;
  }
}

function findWorkspaceRoot() {
  let cur = process.cwd();
  for (let i = 0; i < 10; i++) {
    const cand = path.join(cur, "Cargo.lock");
    if (fs.existsSync(cand)) return cur;
    const parent = path.dirname(cur);
    if (parent === cur) break;
    cur = parent;
  }
  throw new Error("failed to find workspace root (Cargo.lock)");
}

function libNaming() {
  const p = process.platform;
  if (p === "win32") return { prefix: "", ext: ".dll" };
  if (p === "darwin") return { prefix: "lib", ext: ".dylib" };
  return { prefix: "lib", ext: ".so" };
}

function normalizePorts(objOrArr) {
  if (Array.isArray(objOrArr)) {
    return objOrArr.map((p) => {
      if (typeof p === "string") return { name: p, ty: null };
      if (Array.isArray(p) && p.length === 2) return { name: p[0], ty: p[1] };
      return p;
    });
  }
  if (!objOrArr || typeof objOrArr !== "object") return [];
  return Object.entries(objOrArr).map(([name, ty]) => ({ name, ty }));
}

export function node({
  id,
  label,
  module,
  path: module_path,
  func,
  inputs,
  outputs,
  defaults,
  input_types,
  output_types,
  raw_io = false,
  stateful = false,
  state = null,
  feature_flags = [],
  default_compute = "CpuOnly",
  sync_groups = [],
  metadata = {},
} = {}) {
  return (fn) => {
    const js_function = func ?? fn?.name ?? null;
    const js_module = module ?? null;
    const js_path = module_path ?? null;
    const inPorts = normalizePorts(inputs);
    const outPorts = normalizePorts(outputs);

    // Rust-like sugar:
    // - `inputs: ["a","b"]` + `input_types: {a: t.int(), ...}`
    // - `outputs: ["out"]` + `output_types: {out: t.int()}`
    if (input_types && typeof input_types === "object") {
      for (const p of inPorts) {
        if (p && p.ty == null && Object.prototype.hasOwnProperty.call(input_types, p.name)) {
          p.ty = input_types[p.name];
        }
      }
    }
    if (output_types && typeof output_types === "object") {
      for (const p of outPorts) {
        if (p && p.ty == null && Object.prototype.hasOwnProperty.call(output_types, p.name)) {
          p.ty = output_types[p.name];
        }
      }
    }
    if (defaults && typeof defaults === "object") {
      for (const p of inPorts) {
        if (p && Object.prototype.hasOwnProperty.call(defaults, p.name)) {
          p.const_value = defaults[p.name];
        }
      }
    }
    for (const p of [...inPorts, ...outPorts]) {
      if (!p || typeof p !== "object" || !p.name) {
        throw new TypeError("ports must be {name,ty} or [name,ty] or string");
      }
      if (p.ty == null) {
        throw new TypeError(`missing type for port '${p.name}' on node '${id}'`);
      }
    }
    const def = new NodeDef({
      id,
      label,
      js_module,
      js_path,
      js_function,
      raw_io,
      stateful,
      state,
      feature_flags,
      default_compute,
      sync_groups,
      metadata,
      inputs: inPorts,
      outputs: outPorts,
    });
    fn.__daedalus_node__ = def;
    return fn;
  };
}

// Method decorator variant (usable from TypeScript on class methods).
export function nodeMethod(args = {}) {
  return (_target, propertyKey, descriptor) => {
    const fn = descriptor && descriptor.value ? descriptor.value : null;
    if (typeof fn !== "function") {
      throw new TypeError("@nodeMethod must decorate a function");
    }
    const ownerName =
      (_target && typeof _target === "function" && _target.name) ||
      (_target && _target.constructor && _target.constructor.name) ||
      null;
    const dotted = ownerName ? `${ownerName}.${propertyKey}` : propertyKey;
    node({ ...args, func: args.func ?? dotted })(fn);
    return descriptor;
  };
}
