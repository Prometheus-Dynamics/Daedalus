use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use daedalus_runtime::NodeError;
use daedalus_runtime::io::NodeIo;
use daedalus_runtime::plugins::{Plugin, PluginError, PluginInstallContext, PluginResult};
use thiserror::Error;

use crate::bridge::{inputs_to_json, json_to_output, manifest_node_to_decl, push_output};
use crate::manifest::Manifest;
#[cfg(feature = "gpu-wgpu")]
use crate::shader_manifest::install_shader_node;

pub type JavaManifest = Manifest;

#[derive(Debug, Error)]
pub enum JavaManifestError {
    #[error("failed to read manifest: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse manifest json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("registry error: {0}")]
    Registry(String),
    #[error("java error: {0}")]
    Java(String),
}

pub fn load_java_manifest(path: impl AsRef<Path>) -> Result<JavaManifest, JavaManifestError> {
    let text = fs::read_to_string(path)?;
    let manifest: JavaManifest = serde_json::from_str(&text)?;
    Ok(manifest)
}

pub struct JavaManifestPlugin {
    id: &'static str,
    manifest: JavaManifest,
    module_search: Option<PathBuf>,
}

impl JavaManifestPlugin {
    pub fn from_manifest(manifest: JavaManifest) -> Self {
        Self::from_manifest_with_base(manifest, None)
    }

    pub fn from_manifest_with_base(manifest: JavaManifest, base: Option<PathBuf>) -> Self {
        let leaked = Box::leak(manifest.plugin.name.clone().into_boxed_str());
        Self {
            id: leaked,
            manifest,
            module_search: base,
        }
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, JavaManifestError> {
        let manifest = load_java_manifest(&path)?;
        let base = path.as_ref().parent().map(|p| p.to_path_buf());
        Ok(Self::from_manifest_with_base(manifest, base))
    }

    pub fn id(&self) -> &'static str {
        self.id
    }

    fn install_inner(
        &self,
        registry: &mut PluginInstallContext<'_>,
    ) -> Result<(), JavaManifestError> {
        for node in &self.manifest.nodes {
            let desc = manifest_node_to_decl(node).map_err(|e| {
                JavaManifestError::Registry(format!("invalid node declaration: {e}"))
            })?;
            registry
                .register_node_decl(desc)
                .map_err(|e| JavaManifestError::Registry(e.to_string()))?;

            if node.shader.is_some() {
                #[cfg(feature = "gpu-wgpu")]
                {
                    install_shader_node(node, self.module_search.as_deref(), registry).map_err(
                        |e| JavaManifestError::Registry(format!("shader node {}: {e}", node.id)),
                    )?;
                }
                continue;
            }

            if let Some(cap) = node.capability.clone() {
                let inputs = node.inputs.clone();
                let outputs = node.outputs.clone();
                if outputs.len() != 1 {
                    return Err(JavaManifestError::Registry(format!(
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

            let classpath = node
                .java_classpath
                .clone()
                .ok_or_else(|| JavaManifestError::Java("missing java_classpath".into()))?;
            let class = node
                .java_class
                .clone()
                .ok_or_else(|| JavaManifestError::Java("missing java_class".into()))?;
            let method = node
                .java_method
                .clone()
                .ok_or_else(|| JavaManifestError::Java("missing java_method".into()))?;

            let inputs = node.inputs.clone();
            let outputs = node.outputs.clone();
            let module_search = self.module_search.clone();
            let raw_io = node.raw_io;

            let is_stateful = node.stateful || node.state.is_some();
            let state_spec = node.state.clone();
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
                            "classpath": classpath,
                            "class": class,
                            "method": method,
                            "args": args,
                            "stateful": true,
                            "raw_io": raw_io,
                            "state_spec": state_spec,
                            "state": state.take().unwrap_or(serde_json::Value::Null),
                            "ctx": { "metadata": ctx_json },
                            "node": node_json,
                        });
                        run_java_call(&module_search, payload).and_then(|value| {
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
                        "classpath": classpath,
                        "class": class,
                        "method": method,
                        "args": args,
                        "stateful": false,
                        "raw_io": raw_io,
                        "ctx": { "metadata": ctx_json },
                        "node": node_json,
                    });
                    let value = run_java_call(&module_search, payload)?;
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
        .ok_or_else(|| NodeError::Handler("expected array from java".into()))?;
    if arr.len() != outputs.len() {
        return Err(NodeError::Handler("java tuple arity mismatch".into()));
    }
    for (idx, out) in outputs.iter().enumerate() {
        let val = json_to_output(arr[idx].clone(), &out.ty).map_err(NodeError::Handler)?;
        push_output(io, &out.name, val);
    }
    Ok(())
}

fn ensure_java_bridge() -> Result<PathBuf, NodeError> {
    use std::sync::OnceLock;
    static BRIDGE_DIR: OnceLock<PathBuf> = OnceLock::new();
    if let Some(dir) = BRIDGE_DIR.get() {
        return Ok(dir.clone());
    }

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "daedalus_java_bridge_{}_{}",
        std::process::id(),
        nanos
    ));
    std::fs::create_dir_all(&dir).map_err(|e| NodeError::Handler(e.to_string()))?;
    let src = dir.join("DaedalusJavaBridge.java");
    std::fs::write(&src, JAVA_BRIDGE).map_err(|e| NodeError::Handler(e.to_string()))?;
    let javac = std::env::var("JAVAC").unwrap_or_else(|_| "javac".to_string());
    let out = Command::new(javac)
        .current_dir(&dir)
        .arg(src.file_name().unwrap())
        .output()
        .map_err(|e| NodeError::Handler(e.to_string()))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(NodeError::Handler(format!("javac failed: {stderr}")));
    }

    let _ = BRIDGE_DIR.set(dir);
    Ok(BRIDGE_DIR
        .get()
        .expect("BRIDGE_DIR was set or concurrently initialized")
        .clone())
}

fn run_java_call(
    module_search: &Option<PathBuf>,
    payload: serde_json::Value,
) -> Result<serde_json::Value, NodeError> {
    let bridge_dir = ensure_java_bridge()?;

    let cp = payload
        .get("classpath")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let base = module_search
        .as_deref()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let cp_abs = if cp.is_empty() {
        base.clone()
    } else {
        let p = PathBuf::from(cp);
        if p.is_absolute() { p } else { base.join(p) }
    };

    let sep = if cfg!(windows) { ";" } else { ":" };
    let classpath = format!(
        "{}{sep}{}",
        bridge_dir.to_string_lossy(),
        cp_abs.to_string_lossy()
    );

    let java = std::env::var("JAVA").unwrap_or_else(|_| "java".to_string());
    let mut cmd = Command::new(java);
    cmd.arg("-cp")
        .arg(classpath)
        .arg("DaedalusJavaBridge")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    if let Some(base_dir) = module_search {
        cmd.current_dir(base_dir);
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
            "java exited with {}: {}",
            output.status, stderr
        )));
    }
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).map_err(|e| NodeError::Handler(e.to_string()))?;
    Ok(value)
}

const JAVA_BRIDGE: &str = include_str!("java/DaedalusJavaBridge.java");

impl Plugin for JavaManifestPlugin {
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
