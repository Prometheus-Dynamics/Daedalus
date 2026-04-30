use std::ffi::{CStr, CString};
use std::fs;
use std::path::{Path, PathBuf};

use daedalus_runtime::io::NodeIo;
use daedalus_runtime::{
    NodeError,
    plugins::{Plugin, PluginError, PluginInstallContext, PluginResult},
};
use libloading::Library;
use thiserror::Error;

use crate::bridge::{inputs_to_json, json_to_output, manifest_node_to_decl, push_output};
use crate::manifest::Manifest;
#[cfg(feature = "gpu-wgpu")]
use crate::shader_manifest::install_shader_node;

pub type CppManifest = Manifest;

#[derive(Debug, Error)]
pub enum CppManifestError {
    #[error("failed to read manifest: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse manifest json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("registry error: {0}")]
    Registry(String),
    #[error("c/c++ error: {0}")]
    Cpp(String),
}

/// Load a C/C++-generated manifest from disk.
pub fn load_cpp_manifest(path: impl AsRef<Path>) -> Result<CppManifest, CppManifestError> {
    let text = fs::read_to_string(path)?;
    let manifest: CppManifest = serde_json::from_str(&text)?;
    Ok(manifest)
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct DaedalusCppResult {
    json: *const std::os::raw::c_char,
    error: *const std::os::raw::c_char,
}

type CppNodeFn = unsafe extern "C" fn(*const std::os::raw::c_char) -> DaedalusCppResult;
type CppFreeFn = unsafe extern "C" fn(*mut std::os::raw::c_char);

pub struct CppManifestPlugin {
    id: &'static str,
    manifest: CppManifest,
    base_dir: Option<PathBuf>,
    default_lib: Option<PathBuf>,
}

impl CppManifestPlugin {
    pub fn from_manifest(manifest: CppManifest) -> Self {
        Self::from_manifest_with_base(manifest, None)
    }

    pub fn from_manifest_with_base(manifest: CppManifest, base: Option<PathBuf>) -> Self {
        Self::from_manifest_with_base_and_default_lib(manifest, base, None)
    }

    pub fn from_manifest_with_base_and_default_lib(
        manifest: CppManifest,
        base: Option<PathBuf>,
        default_lib: Option<PathBuf>,
    ) -> Self {
        let leaked = Box::leak(manifest.plugin.name.clone().into_boxed_str());
        Self {
            id: leaked,
            manifest,
            base_dir: base,
            default_lib,
        }
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, CppManifestError> {
        let manifest = load_cpp_manifest(&path)?;
        let base = path.as_ref().parent().map(|p| p.to_path_buf());
        Ok(Self::from_manifest_with_base(manifest, base))
    }

    pub fn id(&self) -> &'static str {
        self.id
    }

    fn resolve_path(&self, p: &str) -> PathBuf {
        let pb = PathBuf::from(p);
        if pb.is_absolute() {
            return pb;
        }
        if let Some(base) = &self.base_dir {
            base.join(pb)
        } else {
            pb
        }
    }

    fn install_inner(
        &self,
        registry: &mut PluginInstallContext<'_>,
    ) -> Result<(), CppManifestError> {
        use std::collections::HashMap;

        // Load each dylib path at most once and leak it for the lifetime of the process.
        let mut libs: HashMap<PathBuf, &'static Library> = HashMap::new();

        for node in &self.manifest.nodes {
            let desc = manifest_node_to_decl(node).map_err(|e| {
                CppManifestError::Registry(format!(
                    "invalid node declaration for {}: {}",
                    node.id, e
                ))
            })?;
            registry
                .register_node_decl(desc)
                .map_err(|e| CppManifestError::Registry(e.to_string()))?;

            if node.shader.is_some() {
                #[cfg(feature = "gpu-wgpu")]
                {
                    install_shader_node(node, self.base_dir.as_deref(), registry).map_err(|e| {
                        CppManifestError::Registry(format!("shader node {}: {e}", node.id))
                    })?;
                }
                continue;
            }

            if let Some(cap) = node.capability.clone() {
                let inputs = node.inputs.clone();
                let outputs = node.outputs.clone();
                if outputs.len() != 1 {
                    return Err(CppManifestError::Registry(format!(
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

            let cc_path = node
                .cc_path
                .as_deref()
                .map(|p| self.resolve_path(p))
                .or_else(|| self.default_lib.clone())
                .ok_or_else(|| {
                    CppManifestError::Cpp(format!("node {} missing cc_path", node.id))
                })?;
            let cc_function = node.cc_function.as_deref().ok_or_else(|| {
                CppManifestError::Cpp(format!("node {} missing cc_function", node.id))
            })?;
            let cc_free = node.cc_free.as_deref().unwrap_or("daedalus_free");

            if !cc_path.exists() {
                return Err(CppManifestError::Cpp(format!(
                    "node {} cc_path does not exist: {}",
                    node.id,
                    cc_path.display()
                )));
            }

            let lib = if let Some(lib) = libs.get(&cc_path) {
                *lib
            } else {
                let lib = unsafe { Library::new(&cc_path) }.map_err(|e| {
                    CppManifestError::Cpp(format!(
                        "failed to load C/C++ library {}: {e}",
                        cc_path.display()
                    ))
                })?;
                let lib_ref: &'static Library = Box::leak(Box::new(lib));
                libs.insert(cc_path.clone(), lib_ref);
                lib_ref
            };

            let node_fn: CppNodeFn = unsafe { lib.get::<CppNodeFn>(cc_function.as_bytes()) }
                .map(|s| *s)
                .map_err(|e| {
                    CppManifestError::Cpp(format!(
                        "missing cc_function symbol '{}' in {}: {e}",
                        cc_function,
                        cc_path.display()
                    ))
                })?;
            let free_fn: CppFreeFn = unsafe { lib.get::<CppFreeFn>(cc_free.as_bytes()) }
                .map(|s| *s)
                .map_err(|e| {
                    CppManifestError::Cpp(format!(
                        "missing cc_free symbol '{}' in {}: {e}",
                        cc_free,
                        cc_path.display()
                    ))
                })?;

            let inputs = node.inputs.clone();
            let outputs = node.outputs.clone();
            let is_stateful = node.stateful || node.state.is_some();
            let state_spec = node.state.clone();
            let raw_io = node.raw_io;
            let node_id = node.id.clone();
            let cc_function = cc_function.to_string();

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
                            "function": cc_function,
                            "node_id": node_id,
                            "args": args,
                            "stateful": true,
                            "raw_io": raw_io,
                            "state_spec": state_spec,
                            "state": state.take().unwrap_or(serde_json::Value::Null),
                            "ctx": { "metadata": ctx_json },
                            "node": node_json,
                        });
                        let value = call_cpp(node_fn, free_fn, payload)?;
                        if let Some(obj) = value.as_object()
                            && let Some(st) = obj.get("state")
                        {
                            state = Some(st.clone());
                        }
                        push_outputs(io, &outputs, value)
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
                        "function": cc_function,
                        "node_id": node_id,
                        "args": args,
                        "stateful": false,
                        "raw_io": raw_io,
                        "ctx": { "metadata": ctx_json },
                        "node": node_json,
                    });
                    let value = call_cpp(node_fn, free_fn, payload)?;
                    push_outputs(io, &outputs, value)
                });
            }
        }
        Ok(())
    }
}

fn call_cpp(
    node_fn: CppNodeFn,
    free_fn: CppFreeFn,
    payload: serde_json::Value,
) -> Result<serde_json::Value, NodeError> {
    let s = serde_json::to_string(&payload).map_err(|e| NodeError::Handler(e.to_string()))?;
    let c_payload = CString::new(s).map_err(|e| NodeError::Handler(e.to_string()))?;
    let res = unsafe { node_fn(c_payload.as_ptr()) };

    unsafe fn take_c_string(
        ptr: *const std::os::raw::c_char,
        free_fn: CppFreeFn,
    ) -> Option<String> {
        if ptr.is_null() {
            return None;
        }
        // Copy out, then free using the plugin's free function.
        let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string();
        unsafe { free_fn(ptr as *mut std::os::raw::c_char) };
        Some(s)
    }

    if let Some(err) = unsafe { take_c_string(res.error, free_fn) } {
        return Err(NodeError::Handler(err));
    }

    let json_str = unsafe { take_c_string(res.json, free_fn) }
        .ok_or_else(|| NodeError::Handler("C/C++ node returned null result".into()))?;
    let value: serde_json::Value =
        serde_json::from_str(&json_str).map_err(|e| NodeError::Handler(e.to_string()))?;
    Ok(value)
}

/// Load a C/C++ shared library that exports a `daedalus_cpp_manifest()` function returning
/// a manifest JSON string (single-artifact flow, closer to Rust `cdylib` plugins).
///
/// Expected symbols:
/// - `daedalus_cpp_manifest` (returns `DaedalusCppResult` where `json` is manifest JSON)
/// - `daedalus_free` (frees returned strings)
pub fn load_cpp_library_plugin(
    path: impl AsRef<Path>,
) -> Result<CppManifestPlugin, CppManifestError> {
    let lib_path = path.as_ref().to_path_buf();
    let base = lib_path.parent().map(|p| p.to_path_buf());

    let lib = unsafe { Library::new(&lib_path) }.map_err(|e| {
        CppManifestError::Cpp(format!(
            "failed to load C/C++ library {}: {e}",
            lib_path.display()
        ))
    })?;
    let lib = Box::leak(Box::new(lib));

    type ManifestFn = unsafe extern "C" fn() -> DaedalusCppResult;
    let mf: ManifestFn = unsafe { lib.get::<ManifestFn>(b"daedalus_cpp_manifest") }
        .map(|s| *s)
        .map_err(|e| {
            CppManifestError::Cpp(format!(
                "missing daedalus_cpp_manifest in {}: {e}",
                lib_path.display()
            ))
        })?;
    let free_fn: CppFreeFn = unsafe { lib.get::<CppFreeFn>(b"daedalus_free") }
        .map(|s| *s)
        .map_err(|e| {
            CppManifestError::Cpp(format!(
                "missing daedalus_free in {}: {e}",
                lib_path.display()
            ))
        })?;

    // Call manifest fn; copy out JSON and free the returned string.
    let res = unsafe { mf() };
    unsafe fn take(ptr: *const std::os::raw::c_char, free_fn: CppFreeFn) -> Option<String> {
        if ptr.is_null() {
            return None;
        }
        let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string();
        unsafe { free_fn(ptr as *mut std::os::raw::c_char) };
        Some(s)
    }
    if let Some(err) = unsafe { take(res.error, free_fn) } {
        return Err(CppManifestError::Cpp(err));
    }
    let json = unsafe { take(res.json, free_fn) }
        .ok_or_else(|| CppManifestError::Cpp("daedalus_cpp_manifest returned null".into()))?;

    let mut manifest: Manifest = serde_json::from_str(&json).map_err(CppManifestError::Json)?;

    // For single-artifact flow, nodes can omit cc_path; we set a default library to this path.
    // Also set cc_path to the library filename if absent so relative manifests still work when bundled.
    let filename = lib_path
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string());
    if let Some(fname) = filename {
        for n in &mut manifest.nodes {
            if n.cc_path.is_none() {
                n.cc_path = Some(fname.clone());
            }
        }
    }

    Ok(CppManifestPlugin::from_manifest_with_base_and_default_lib(
        manifest,
        base,
        Some(lib_path),
    ))
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

impl Plugin for CppManifestPlugin {
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
