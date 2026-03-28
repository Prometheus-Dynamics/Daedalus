use std::fs;
use std::path::Path;
use std::process::Command;
#[cfg(feature = "inline_py")]
use std::sync::OnceLock;

use daedalus_runtime::io::NodeIo;
use daedalus_runtime::{
    NodeError,
    plugins::{Plugin, PluginRegistry},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(feature = "inline_py")]
use pyo3::{prelude::*, types::PyModule};

use crate::bridge::{inputs_to_json, json_to_output, manifest_node_to_descriptor, push_output};
use crate::manifest::Manifest;
#[cfg(feature = "gpu-wgpu")]
use crate::shader_manifest::install_shader_node;

pub type PythonManifest = Manifest;

#[derive(Debug, Error)]
pub enum PythonManifestError {
    #[error("failed to read manifest: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse manifest json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("registry error: {0}")]
    Registry(String),
    #[error("unsupported value in manifest metadata/const: {0}")]
    UnsupportedValue(String),
    #[error("python error: {0}")]
    Python(String),
}

/// Shared image payload struct used by examples and conversions.
#[repr(C)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImageCompute {
    pub data_b64: String,
    pub width: i64,
    pub height: i64,
    pub channels: i64,
    pub dtype: String,
    pub layout: String,
}

/// Load a Python-generated manifest from disk.
pub fn load_python_manifest(path: impl AsRef<Path>) -> Result<PythonManifest, PythonManifestError> {
    let text = fs::read_to_string(path)?;
    let manifest: PythonManifest = serde_json::from_str(&text)?;
    Ok(manifest)
}

pub struct PythonManifestPlugin {
    id: &'static str,
    manifest: PythonManifest,
    module_search: Option<std::path::PathBuf>,
}

impl PythonManifestPlugin {
    pub fn from_manifest(manifest: PythonManifest) -> Self {
        Self::from_manifest_with_base(manifest, None)
    }

    pub fn from_manifest_with_base(
        manifest: PythonManifest,
        base: Option<std::path::PathBuf>,
    ) -> Self {
        let leaked = Box::leak(manifest.plugin.name.clone().into_boxed_str());
        Self {
            id: leaked,
            manifest,
            module_search: base,
        }
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, PythonManifestError> {
        let manifest = load_python_manifest(&path)?;
        let base = path.as_ref().parent().map(|p| p.to_path_buf());
        Ok(Self::from_manifest_with_base(manifest, base))
    }

    pub fn id(&self) -> &'static str {
        self.id
    }

    fn install_inner(&self, registry: &mut PluginRegistry) -> Result<(), PythonManifestError> {
        for node in &self.manifest.nodes {
            let desc = manifest_node_to_descriptor(node).map_err(|e| {
                PythonManifestError::Registry(format!("invalid descriptor for {}: {}", node.id, e))
            })?;
            registry
                .registry
                .register_node(desc)
                .map_err(|e| PythonManifestError::Registry(e.to_string()))?;

            if node.shader.is_some() {
                #[cfg(feature = "gpu-wgpu")]
                {
                    install_shader_node(node, self.module_search.as_deref(), registry).map_err(
                        |e| PythonManifestError::Registry(format!("shader node {}: {e}", node.id)),
                    )?;
                }
                // If `gpu-wgpu` isn't enabled, we still register the descriptor; planning will fail
                // because shader nodes default to `GpuRequired`. Skip installing a language handler.
                continue;
            }

            if let Some(cap) = node.capability.clone() {
                let inputs = node.inputs.clone();
                let outputs = node.outputs.clone();
                if outputs.len() != 1 {
                    return Err(PythonManifestError::Registry(format!(
                        "capability node {} must have exactly 1 output",
                        node.id
                    )));
                }
                let out_port = outputs[0].name.clone();
                registry
                    .handlers
                    .on(&node.id, move |_node_rt, _ctx_rt, io| {
                        let mut args_any: Vec<&dyn std::any::Any> =
                            Vec::with_capacity(inputs.len());
                        for p in &inputs {
                            let a = io.get_any_raw(&p.name).ok_or_else(|| {
                                NodeError::InvalidInput(format!("missing {}", p.name))
                            })?;
                            args_any.push(a);
                        }
                        let cap_read = daedalus_runtime::capabilities::global()
                            .read()
                            .map_err(|_| NodeError::Handler("capability lock poisoned".into()))?;
                        let entries = cap_read.get(&cap).ok_or_else(|| {
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

            let module = node.py_module.clone();
            let path = node.py_path.clone();
            if module.is_none() && path.is_none() {
                return Err(PythonManifestError::Python(
                    "missing py_module/py_path".into(),
                ));
            }
            let function = node
                .py_function
                .clone()
                .ok_or_else(|| PythonManifestError::Python("missing py_function".into()))?;
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
                            "path": path,
                            "function": function,
                            "args": args,
                            "stateful": true,
                            "raw_io": raw_io,
                            "state_spec": state_spec,
                            "state": state.take().unwrap_or(serde_json::Value::Null),
                            "ctx": { "metadata": ctx_json },
                            "node": node_json,
                        });
                        run_python_call(&module_search, payload).and_then(|value| {
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
                        "path": path,
                        "function": function,
                        "args": args,
                        "stateful": false,
                        "raw_io": raw_io,
                        "ctx": { "metadata": ctx_json },
                        "node": node_json,
                    });
                    let value = run_python_call(&module_search, payload)?;
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
        // Allow returning named outputs: {"out": 1} or {"out0": 1, "out1": 2} or {"outputs": {...}}.
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
        .ok_or_else(|| NodeError::Handler("expected array from python".into()))?;
    if arr.len() != outputs.len() {
        return Err(NodeError::Handler("python tuple arity mismatch".into()));
    }
    for (idx, out) in outputs.iter().enumerate() {
        let val = json_to_output(arr[idx].clone(), &out.ty).map_err(NodeError::Handler)?;
        push_output(io, &out.name, val);
    }
    Ok(())
}

fn run_python_call(
    module_search: &Option<std::path::PathBuf>,
    payload: serde_json::Value,
) -> Result<serde_json::Value, NodeError> {
    #[cfg(feature = "inline_py")]
    {
        let mode = std::env::var("DAEDALUS_PY_MODE").unwrap_or_default();
        if mode == "inline" {
            return run_python_inline(module_search, payload);
        }
    }
    run_python_subprocess(module_search, payload)
}

#[cfg(feature = "inline_py")]
fn run_python_inline(
    module_search: &Option<std::path::PathBuf>,
    payload: serde_json::Value,
) -> Result<serde_json::Value, NodeError> {
    static MODULE: OnceLock<Py<PyModule>> = OnceLock::new();
    use std::ffi::CString;
    Python::attach(|py| {
        // Adjust sys.path
        if let Some(base) = module_search
            && let Some(s) = base.to_str()
        {
            let sys = py
                .import("sys")
                .map_err(|e: pyo3::PyErr| NodeError::Handler(e.to_string()))?;
            let path_any = sys
                .getattr("path")
                .map_err(|e: pyo3::PyErr| NodeError::Handler(e.to_string()))?;
            let path = path_any
                .cast::<pyo3::types::PyList>()
                .map_err(|e| NodeError::Handler(e.to_string()))?;
            let mut found = false;
            for item in path.iter() {
                if let Ok(v) = item.extract::<String>()
                    && v == s
                {
                    found = true;
                    break;
                }
            }
            if !found {
                path.insert(0, s)
                    .map_err(|e: pyo3::PyErr| NodeError::Handler(e.to_string()))?;
            }
        }

        let module = MODULE.get_or_init(|| {
            let code = CString::new(PY_BRIDGE).expect("bridge code cstr");
            let filename = CString::new("daedalus_py_bridge.py").expect("bridge filename cstr");
            let name = CString::new("daedalus_py_bridge").expect("bridge module cstr");
            PyModule::from_code(py, &code, &filename, &name)
                .expect("bridge compile")
                .into()
        });
        let module = module.bind(py);
        let invoke = module
            .getattr("invoke")
            .map_err(|e: pyo3::PyErr| NodeError::Handler(e.to_string()))?;
        let data =
            serde_json::to_string(&payload).map_err(|e| NodeError::Handler(e.to_string()))?;
        let json = py
            .import("json")
            .map_err(|e: pyo3::PyErr| NodeError::Handler(e.to_string()))?;
        let payload_py = json
            .call_method1("loads", (data,))
            .map_err(|e: pyo3::PyErr| NodeError::Handler(e.to_string()))?;
        let result_py = invoke
            .call1((payload_py,))
            .map_err(|e: pyo3::PyErr| NodeError::Handler(e.to_string()))?;
        let dumped = json
            .call_method1("dumps", (result_py,))
            .map_err(|e: pyo3::PyErr| NodeError::Handler(e.to_string()))?;
        let dumped_str: String = dumped
            .extract()
            .map_err(|e: pyo3::PyErr| NodeError::Handler(e.to_string()))?;
        serde_json::from_str(&dumped_str).map_err(|e| NodeError::Handler(e.to_string()))
    })
}

fn run_python_subprocess(
    module_search: &Option<std::path::PathBuf>,
    payload: serde_json::Value,
) -> Result<serde_json::Value, NodeError> {
    let script = PY_BRIDGE;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let script_path = std::env::temp_dir().join(format!(
        "daedalus_py_bridge_{}_{}.py",
        std::process::id(),
        nanos
    ));
    std::fs::write(&script_path, script).map_err(|e| NodeError::Handler(e.to_string()))?;

    let mut cmd = Command::new(std::env::var("PYTHON").unwrap_or_else(|_| "python".to_string()));
    cmd.arg(&script_path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    if let Some(base) = module_search {
        cmd.current_dir(base);
        let mut paths: Vec<std::path::PathBuf> = std::env::var_os("PYTHONPATH")
            .map(|p| std::env::split_paths(&p).collect())
            .unwrap_or_default();
        paths.insert(0, base.clone());
        if let Ok(joined) = std::env::join_paths(paths) {
            cmd.env("PYTHONPATH", joined);
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
        return Err(NodeError::Handler(format!(
            "python exited with {}",
            output.status
        )));
    }
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).map_err(|e| NodeError::Handler(e.to_string()))?;
    Ok(value)
}

const PY_BRIDGE: &str = r#"import json, importlib, importlib.util, dataclasses, enum, sys, inspect, os
from typing import get_type_hints, get_origin, get_args, Union
def _load_module_from_path(path, name=None):
    p = path
    if not os.path.isabs(p):
        p = os.path.join(os.getcwd(), p)
    mod_name = name or f"_daedalus_pathmod_{abs(hash(p))}"
    spec = importlib.util.spec_from_file_location(mod_name, p)
    if spec is None or spec.loader is None:
        raise ImportError(f"failed to load python module from path: {p}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod

def invoke(data):
    # Ensure the manifest directory (cwd) is searched before the bridge script directory,
    # so temp files in /tmp can't shadow the manifest-adjacent runtime module.
    sys.path.insert(0, os.getcwd())
    mod=None
    if data.get('path'):
        mod=_load_module_from_path(data.get('path'), data.get('module'))
    else:
        mod=importlib.import_module(data['module'])
    func=getattr(mod,data['function'])
    args=data.get('args',[])
    # Upgrade image-like dicts into CvImage if available.
    cv_cls=getattr(mod, 'CvImage', None)
    def dict_to_cv(d):
        if cv_cls:
            try:
                return cv_cls.from_payload(d)
            except Exception:
                pass
        try:
            import base64 as _b64
            import numpy as _np
            buf=_b64.b64decode(d.get('data_b64','').encode('ascii'))
            # Fast path: raw bytes with shape metadata (no PNG decode).
            layout=str(d.get('layout','HWC'))
            dtype=str(d.get('dtype','u8'))
            enc=str(d.get('encoding',''))
            w=int(d.get('width',0) or 0)
            h=int(d.get('height',0) or 0)
            ch=int(d.get('channels',0) or 0)
            dt_map={'u8':'uint8','i8':'int8','u16':'uint16','i16':'int16','u32':'uint32','i32':'int32','f32':'float32','f64':'float64'}
            dt=_np.dtype(dt_map.get(dtype, dtype))
            expected=w*h*max(ch,1)*dt.itemsize if w>0 and h>0 else -1
            if (enc == 'raw' or (layout == 'HWC' and w>0 and h>0 and ch>0)) and (expected < 0 or len(buf) == expected):
                arr=_np.frombuffer(buf, dtype=dt)
                if ch == 1:
                    mat=arr.reshape((h,w))
                else:
                    mat=arr.reshape((h,w,ch))
            else:
                import cv2 as _cv2
                arr=_np.frombuffer(buf, dtype=_np.uint8)
                mat=_cv2.imdecode(arr, _cv2.IMREAD_UNCHANGED)
            class _DictImg:
                def __init__(self, m): self._m=m
                def to_mat(self): return self._m
            return _DictImg(mat)
        except Exception:
            return d
    new_args=[]
    for a in args:
        if isinstance(a, dict) and 'data_b64' in a and 'width' in a and 'height' in a and 'channels' in a:
            new_args.append(dict_to_cv(a))
            continue
        new_args.append(a)
    args=new_args

    # Best-effort typed struct/dataclass coercion for port inputs, so Python authoring can be
    # closer to Rust (struct ports become typed objects, not raw dicts).
    def _coerce_dataclass(v, ann):
        if ann is None:
            return v
        try:
            origin = get_origin(ann)
            if origin is Union:
                parts = [a for a in get_args(ann)]
                non_none = [a for a in parts if a is not type(None)]
                if len(non_none) == 1:
                    if v is None:
                        return None
                    return _coerce_dataclass(v, non_none[0])
        except Exception:
            pass
        try:
            if isinstance(ann, type) and dataclasses.is_dataclass(ann) and isinstance(v, dict):
                return ann(**v)
        except Exception:
            return v
        return v

    try:
        hints_all = get_type_hints(func)
    except Exception:
        hints_all = {}
    sig_local = None
    try:
        sig_local = inspect.signature(func)
    except Exception:
        sig_local = None
    if sig_local is not None and isinstance(args, list) and args:
        port_params = []
        for name, p in sig_local.parameters.items():
            if name in ("self", "ctx", "node", "io", "state"):
                continue
            port_params.append(name)
        for i in range(min(len(args), len(port_params))):
            nm = port_params[i]
            ann = hints_all.get(nm)
            args[i] = _coerce_dataclass(args[i], ann)
    stateful=data.get('stateful', False)
    state_spec=data.get('state_spec') or {}
    state_init=state_spec.get('init') if isinstance(state_spec, dict) else None
    state_val=data.get('state')
    state_obj=state_val
    py_dc=state_spec.get('py_dataclass') if isinstance(state_spec, dict) else None
    if stateful and py_dc:
        try:
            if isinstance(py_dc, dict) and py_dc.get('path'):
                dc_mod=_load_module_from_path(py_dc.get('path'), py_dc.get('module'))
            elif isinstance(py_dc, dict) and py_dc.get('module'):
                dc_mod=importlib.import_module(py_dc.get('module'))
            else:
                dc_mod=mod
            dc_cls=getattr(dc_mod, py_dc['name'])
            if state_val is None:
                state_obj=dc_cls()
            elif isinstance(state_val, dict):
                try:
                    state_obj=dc_cls(**state_val)
                except Exception:
                    state_obj=state_val
            else:
                state_obj=state_val
        except Exception:
            state_obj=state_val
    if stateful and state_init and state_val is None:
        try:
            if py_dc:
                if isinstance(py_dc, dict) and py_dc.get('path'):
                    dc_mod=_load_module_from_path(py_dc.get('path'), py_dc.get('module'))
                elif isinstance(py_dc, dict) and py_dc.get('module'):
                    dc_mod=importlib.import_module(py_dc.get('module'))
                else:
                    dc_mod=mod
                dc_cls=getattr(dc_mod, py_dc['name'])
                init_fn=getattr(dc_cls, state_init)
            else:
                init_fn=getattr(mod, state_init)
            state_obj=init_fn()
        except Exception:
            state_obj=state_val

    def to_json(x):
        if dataclasses.is_dataclass(x):
            return dataclasses.asdict(x)
        if cv_cls and isinstance(x, cv_cls):
            return x.to_payload()
        if isinstance(x, enum.Enum):
            return {'name': x.name, 'value': to_json(x.value)}
        if isinstance(x, tuple):
            return [to_json(i) for i in x]
        if isinstance(x, list):
            return [to_json(i) for i in x]
        if isinstance(x, dict):
            return {k: to_json(v) for k,v in x.items()}
        return x

    sig=None
    try:
        sig=inspect.signature(func)
    except Exception:
        sig=None
    wants_ctx = bool(sig and ('ctx' in sig.parameters or any(p.kind==inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())))
    wants_node = bool(sig and ('node' in sig.parameters or any(p.kind==inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())))
    wants_io = bool(sig and ('io' in sig.parameters or any(p.kind==inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())))
    ctx_obj=data.get('ctx')
    node_obj=data.get('node')
    raw_io=bool(data.get('raw_io', False))
    io_obj=None
    if raw_io:
        class _RawIo:
            def __init__(self):
                self.events=[]
            def push(self, port, value):
                if not isinstance(port, str) or not port:
                    raise TypeError('io.push(port,value) requires a port string')
                self.events.append({'port': port, 'value': to_json(value)})
            def push_many(self, port, values):
                for v in values:
                    self.push(port, v)
        io_obj=_RawIo()
    kwargs={}
    if wants_ctx:
        kwargs['ctx']=ctx_obj
    if wants_node:
        kwargs['node']=node_obj
    if wants_io and raw_io:
        kwargs['io']=io_obj
    if stateful:
        kwargs['state']=state_obj
        res=func(*args, **kwargs)
    else:
        res=func(*args, **kwargs)
    if raw_io and io_obj is not None and getattr(io_obj, 'events', None):
        if stateful:
            result={'state': to_json(state_obj), 'events': io_obj.events}
        else:
            result={'events': io_obj.events}
        return result
    if stateful:
        new_state=state_obj
        outputs=res
        if isinstance(res, dict) and 'state' in res:
            new_state=res.get('state')
            outputs=res.get('outputs')
        elif isinstance(res, tuple) and len(res)==2:
            new_state, outputs = res
        result={'state': to_json(new_state), 'outputs': to_json(outputs)}
        if raw_io and io_obj is not None:
            result['events']=io_obj.events
    else:
        result=to_json(res)
    return result
if __name__ == '__main__':
    data=json.load(sys.stdin)
    json.dump(invoke(data), sys.stdout)
"#;

impl Plugin for PythonManifestPlugin {
    fn id(&self) -> &'static str {
        self.id
    }

    fn install(&self, registry: &mut PluginRegistry) -> Result<(), &'static str> {
        self.install_inner(registry)
            .map_err(|e| Box::leak(e.to_string().into_boxed_str()) as &'static str)
    }
}
