use std::fs;
use std::path::Path;
use std::process::Command;

use daedalus_runtime::io::NodeIo;
use daedalus_runtime::{
    NodeError,
    plugins::{Plugin, PluginError, PluginInstallContext, PluginResult},
};
use thiserror::Error;

use crate::bridge::{inputs_to_json, json_to_output, manifest_node_to_decl, push_output};
use crate::manifest::Manifest;
#[cfg(feature = "gpu-wgpu")]
use crate::shader_manifest::install_shader_node;

pub type NodeManifest = Manifest;

#[derive(Debug, Error)]
pub enum NodeManifestError {
    #[error("failed to read manifest: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse manifest json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("registry error: {0}")]
    Registry(String),
    #[error("node error: {0}")]
    Node(String),
}

/// Load a Node-generated manifest from disk.
pub fn load_node_manifest(path: impl AsRef<Path>) -> Result<NodeManifest, NodeManifestError> {
    let text = fs::read_to_string(path)?;
    let manifest: NodeManifest = serde_json::from_str(&text)?;
    Ok(manifest)
}

pub struct NodeManifestPlugin {
    id: &'static str,
    manifest: NodeManifest,
    module_search: Option<std::path::PathBuf>,
}

impl NodeManifestPlugin {
    pub fn from_manifest(manifest: NodeManifest) -> Self {
        Self::from_manifest_with_base(manifest, None)
    }

    pub fn from_manifest_with_base(
        manifest: NodeManifest,
        base: Option<std::path::PathBuf>,
    ) -> Self {
        let leaked = Box::leak(manifest.plugin.name.clone().into_boxed_str());
        Self {
            id: leaked,
            manifest,
            module_search: base,
        }
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, NodeManifestError> {
        let manifest = load_node_manifest(&path)?;
        let base = path.as_ref().parent().map(|p| p.to_path_buf());
        Ok(Self::from_manifest_with_base(manifest, base))
    }

    pub fn id(&self) -> &'static str {
        self.id
    }

    fn install_inner(
        &self,
        registry: &mut PluginInstallContext<'_>,
    ) -> Result<(), NodeManifestError> {
        for node in &self.manifest.nodes {
            let desc = manifest_node_to_decl(node).map_err(|e| {
                NodeManifestError::Registry(format!("invalid node declaration: {e}"))
            })?;
            registry
                .register_node_decl(desc)
                .map_err(|e| NodeManifestError::Registry(e.to_string()))?;

            if node.shader.is_some() {
                #[cfg(feature = "gpu-wgpu")]
                {
                    install_shader_node(node, self.module_search.as_deref(), registry).map_err(
                        |e| NodeManifestError::Registry(format!("shader node {}: {e}", node.id)),
                    )?;
                }
                continue;
            }

            if let Some(cap) = node.capability.clone() {
                let inputs = node.inputs.clone();
                let outputs = node.outputs.clone();
                if outputs.len() != 1 {
                    return Err(NodeManifestError::Registry(format!(
                        "capability node {} must have exactly 1 output",
                        node.id
                    )));
                }
                let out_port = outputs[0].name.clone();
                registry.handlers.on(&node.id, move |_node_rt, ctx_rt, io| {
                    let mut args_any: Vec<&dyn std::any::Any> = Vec::with_capacity(inputs.len());
                    for p in &inputs {
                        let a = io
                            .get_payload(&p.name)
                            .and_then(|payload| payload.value_any())
                            .ok_or_else(|| {
                                NodeError::InvalidInput(format!("missing {}", p.name))
                            })?;
                        args_any.push(a);
                    }
                    let entries = ctx_rt.capabilities.get(&cap).ok_or_else(|| {
                        NodeError::InvalidInput("missing capability entries".into())
                    })?;
                    for entry in entries {
                        if args_any.len() == entry.type_ids.len()
                            && args_any
                                .iter()
                                .zip(entry.type_ids.iter())
                                .all(|(a, tid)| a.type_id() == *tid)
                        {
                            let out = (entry.func)(&args_any)?;
                            io.push_output(Some(&out_port), out);
                            return Ok(());
                        }
                    }
                    Err(NodeError::InvalidInput(
                        "unsupported capability type".into(),
                    ))
                });
                continue;
            }

            let module = node
                .js_path
                .clone()
                .or_else(|| node.js_module.clone())
                .ok_or_else(|| NodeManifestError::Node("missing js_module/js_path".into()))?;
            let function = node
                .js_function
                .clone()
                .ok_or_else(|| NodeManifestError::Node("missing js_function".into()))?;

            let inputs = node.inputs.clone();
            let outputs = node.outputs.clone();
            let module_search = self.module_search.clone();

            let is_stateful = node.stateful || node.state.is_some();
            let state_spec = node.state.clone();
            let raw_io = node.raw_io;
            if is_stateful {
                let mut state: Option<serde_json::Value> = None;
                registry
                    .handlers
                    .on_stateful(&node.id, move |node_rt, ctx_rt, io| {
                        let args = inputs_to_json(io, &inputs)
                            .map_err(|e| NodeError::Handler(e.to_string()))?;
                        let ctx_json = serde_json::to_value(&ctx_rt.metadata)
                            .unwrap_or(serde_json::Value::Null);
                        let node_json = serde_json::json!({
                            "id": node_rt.id,
                            "label": node_rt.label,
                            "bundle": node_rt.bundle,
                        });
                        let payload = serde_json::json!({
                            "module": module,
                            "function": function,
                            "args": args,
                            "stateful": true,
                            "raw_io": raw_io,
                            "state_spec": state_spec,
                            "state": state.take().unwrap_or(serde_json::Value::Null),
                            "ctx": { "metadata": ctx_json },
                            "node": node_json,
                        });
                        run_node_call(&module_search, payload).and_then(|value| {
                            if let Some(obj) = value.as_object()
                                && let Some(st) = obj.get("state")
                            {
                                state = Some(st.clone());
                            }
                            push_outputs(io, &outputs, value)
                        })
                    });
            } else {
                registry.handlers.on(&node.id, move |node_rt, ctx_rt, io| {
                    let args = inputs_to_json(io, &inputs)
                        .map_err(|e| NodeError::Handler(e.to_string()))?;
                    let ctx_json =
                        serde_json::to_value(&ctx_rt.metadata).unwrap_or(serde_json::Value::Null);
                    let node_json = serde_json::json!({
                        "id": node_rt.id,
                        "label": node_rt.label,
                        "bundle": node_rt.bundle,
                    });
                    let payload = serde_json::json!({
                        "module": module,
                        "function": function,
                        "args": args,
                        "stateful": false,
                        "raw_io": raw_io,
                        "ctx": { "metadata": ctx_json },
                        "node": node_json,
                    });
                    let value = run_node_call(&module_search, payload)?;
                    push_outputs(io, &outputs, value)
                });
            }
        }
        Ok(())
    }
}

fn push_outputs(
    io: &mut NodeIo,
    outputs: &[crate::manifest::ManifestPort],
    value: serde_json::Value,
) -> Result<(), NodeError> {
    fn push_events(
        io: &mut NodeIo,
        outputs: &[crate::manifest::ManifestPort],
        events: &serde_json::Value,
    ) -> Result<(), NodeError> {
        let arr = events
            .as_array()
            .ok_or_else(|| NodeError::Handler("expected events to be an array".into()))?;
        for ev in arr {
            let obj = ev
                .as_object()
                .ok_or_else(|| NodeError::Handler("expected event to be an object".into()))?;
            let port = obj
                .get("port")
                .and_then(|v| v.as_str())
                .ok_or_else(|| NodeError::Handler("event missing 'port'".into()))?;
            let val_json = obj.get("value").cloned().unwrap_or(serde_json::Value::Null);
            let out = outputs
                .iter()
                .find(|p| p.name == port)
                .ok_or_else(|| NodeError::Handler(format!("unknown output port '{port}'")))?;
            let val = json_to_output(val_json, &out.ty).map_err(NodeError::Handler)?;
            push_output(io, &out.name, val);
        }
        Ok(())
    }

    if outputs.is_empty() {
        return Ok(());
    }
    if let Some(obj) = value.as_object() {
        let mut pushed_events = false;
        if let Some(events) = obj.get("events") {
            push_events(io, outputs, events)?;
            pushed_events = true;
        }
        if let Some(outputs_val) = obj.get("outputs") {
            return push_outputs(io, outputs, outputs_val.clone());
        }
        if pushed_events {
            let has_named = outputs.iter().any(|p| obj.contains_key(&p.name));
            if !has_named {
                return Ok(());
            }
        }
        if outputs.len() == 1 {
            let out = &outputs[0];
            if let Some(v) = obj.get(&out.name) {
                let val = json_to_output(v.clone(), &out.ty).map_err(NodeError::Handler)?;
                push_output(io, &out.name, val);
                return Ok(());
            }
        }
        if outputs.len() > 1 {
            let mut pushed = 0usize;
            for out in outputs {
                if let Some(v) = obj.get(&out.name) {
                    let val = json_to_output(v.clone(), &out.ty).map_err(NodeError::Handler)?;
                    push_output(io, &out.name, val);
                    pushed += 1;
                }
            }
            if pushed == outputs.len() {
                return Ok(());
            }
        }
    }
    if outputs.len() == 1 {
        let val = json_to_output(value, &outputs[0].ty).map_err(NodeError::Handler)?;
        push_output(io, &outputs[0].name, val);
        return Ok(());
    }
    let arr = value
        .as_array()
        .ok_or_else(|| NodeError::Handler("expected array from node".into()))?;
    if arr.len() != outputs.len() {
        return Err(NodeError::Handler("node tuple arity mismatch".into()));
    }
    for (idx, out) in outputs.iter().enumerate() {
        let val = json_to_output(arr[idx].clone(), &out.ty).map_err(NodeError::Handler)?;
        push_output(io, &out.name, val);
    }
    Ok(())
}

fn run_node_call(
    module_search: &Option<std::path::PathBuf>,
    payload: serde_json::Value,
) -> Result<serde_json::Value, NodeError> {
    let script = NODE_BRIDGE;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let script_path = std::env::temp_dir().join(format!(
        "daedalus_node_bridge_{}_{}.mjs",
        std::process::id(),
        nanos
    ));
    std::fs::write(&script_path, script).map_err(|e| NodeError::Handler(e.to_string()))?;

    let mut cmd = Command::new(std::env::var("NODE").unwrap_or_else(|_| "node".to_string()));
    cmd.arg(&script_path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    if let Some(base) = module_search {
        cmd.current_dir(base);
        let mut paths: Vec<std::path::PathBuf> = std::env::var_os("NODE_PATH")
            .map(|p| std::env::split_paths(&p).collect())
            .unwrap_or_default();
        paths.insert(0, base.clone());
        if let Ok(joined) = std::env::join_paths(paths) {
            cmd.env("NODE_PATH", joined);
        }
    }
    let mut child = cmd.spawn().map_err(|e| NodeError::Handler(e.to_string()))?;
    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| NodeError::Handler("missing stdin".into()))?;
        serde_json::to_writer(&mut stdin, &payload)
            .map_err(|e| NodeError::Handler(e.to_string()))?;
        drop(stdin);
    }
    let output = child
        .wait_with_output()
        .map_err(|e| NodeError::Handler(e.to_string()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(NodeError::Handler(format!(
            "node exited with {}: {}",
            output.status, stderr
        )));
    }
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).map_err(|e| NodeError::Handler(e.to_string()))?;
    Ok(value)
}

const NODE_BRIDGE: &str = r#"import process from 'node:process';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);

function resolveExport(mod, path) {
  const parts = String(path || '').split('.').filter(Boolean);
  let cur = mod;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}

function loadModule(moduleId) {
  try {
    return require(moduleId);
  } catch (_) {
    // Fall back to ESM dynamic import (absolute/relative paths need file://).
    if (moduleId.startsWith('.') || moduleId.startsWith('/') || moduleId.includes(':')) {
      const url = new URL(moduleId, 'file://' + process.cwd() + '/');
      return import(url.href);
    }
    return import(moduleId);
  }
}

function toJson(x) {
  if (x === undefined) return null;
  if (x === null) return null;
  if (Array.isArray(x)) return x.map(toJson);
  if (typeof x === 'bigint') return Number(x);
  if (typeof x === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(x)) out[k] = toJson(v);
    return out;
  }
  return x;
}

export async function invoke(data) {
  const mod = await loadModule(data.module);
  const func =
    resolveExport(mod, data.function) ||
    (mod.default ? resolveExport(mod.default, data.function) : undefined);
  if (typeof func !== 'function') {
    throw new Error(`missing export '${data.function}' from module '${data.module}'`);
  }
  const args = Array.isArray(data.args) ? data.args : [];
  const stateful = !!data.stateful;
  const ctx = data.ctx ?? null;
  const node = data.node ?? null;
  const raw_io = !!data.raw_io;
  const io = raw_io
    ? {
        events: [],
        push: (port, value) => {
          if (typeof port !== "string" || !port) throw new TypeError("io.push(port,value) requires a port string");
          io.events.push({ port, value: toJson(value) });
        },
        pushMany: (port, values) => {
          if (!Array.isArray(values)) throw new TypeError("io.pushMany(port,values) expects an array");
          for (const v of values) io.push(port, v);
        },
      }
    : null;

  if (!stateful) {
    const res = await func(...args, { ctx, node, io });
    if (raw_io) {
      if (io.events.length) {
        return { events: io.events };
      }
      return { outputs: toJson(res) };
    }
    return toJson(res);
  }

  const state = data.state ?? null;
  const res = await func({ args, state, state_spec: data.state_spec ?? null, ctx, node, io });
  if (res && typeof res === 'object' && ('state' in res || 'outputs' in res)) {
    const out = { state: toJson(res.state), outputs: toJson(res.outputs) };
    if (raw_io) {
      if (io.events.length) {
        out.events = io.events;
        out.outputs = null;
      }
    }
    return out;
  }
  if (Array.isArray(res) && res.length === 2) {
    const out = { state: toJson(res[0]), outputs: toJson(res[1]) };
    if (raw_io) {
      if (io.events.length) {
        out.events = io.events;
        out.outputs = null;
      }
    }
    return out;
  }
  const out = { state: toJson(state), outputs: toJson(res) };
  if (raw_io) {
    if (io.events.length) {
      out.events = io.events;
      out.outputs = null;
    }
  }
  return out;
}

if (process.argv[1] && process.argv[1].includes('daedalus_node_bridge')) {
  let buf = '';
  process.stdin.setEncoding('utf8');
  process.stdin.on('data', (chunk) => { buf += chunk; });
  process.stdin.on('end', async () => {
    try {
      const data = JSON.parse(buf || '{}');
      const result = await invoke(data);
      process.stdout.write(JSON.stringify(result));
    } catch (err) {
      process.stderr.write(String(err && err.stack ? err.stack : err) + '\\n');
      process.exitCode = 1;
    }
  });
}
"#;

impl Plugin for NodeManifestPlugin {
    fn id(&self) -> &'static str {
        self.id
    }

    fn install(&self, registry: &mut PluginInstallContext<'_>) -> PluginResult<()> {
        self.install_inner(registry)
            .map_err(|error| PluginError::Install {
                message: error.to_string(),
            })
    }
}
