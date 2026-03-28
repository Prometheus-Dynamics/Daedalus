use std::any::Any;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine;
use daedalus_core::sync::{SyncGroup, SyncPolicy};
use daedalus_data::model::{TypeExpr, Value};
use daedalus_registry::ids::NodeId;
use daedalus_registry::store::{NodeDescriptor, Port};
use daedalus_runtime::NodeError;
use daedalus_runtime::io::NodeIo;
use image::DynamicImage;

use crate::manifest::{ManifestPort, ManifestSyncGroup, NodeManifest};
use crate::python::ImageCompute;

pub(crate) fn json_to_value(v: serde_json::Value) -> Result<Value, String> {
    Ok(match v {
        serde_json::Value::Null => Value::Unit,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                return Err(n.to_string());
            }
        }
        serde_json::Value::String(s) => Value::String(Cow::Owned(s)),
        serde_json::Value::Array(items) => {
            let mut vals = Vec::with_capacity(items.len());
            for item in items {
                vals.push(json_to_value(item)?);
            }
            Value::List(vals)
        }
        serde_json::Value::Object(map) => {
            let mut entries = Vec::with_capacity(map.len());
            for (k, v) in map {
                entries.push((Value::String(Cow::Owned(k)), json_to_value(v)?));
            }
            Value::Map(entries)
        }
    })
}

pub(crate) fn json_map_to_values(
    map: &BTreeMap<String, serde_json::Value>,
) -> Result<BTreeMap<String, Value>, String> {
    let mut out = BTreeMap::new();
    for (k, v) in map {
        out.insert(k.clone(), json_to_value(v.clone())?);
    }
    Ok(out)
}

fn value_to_plain_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Unit => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(i),
        Value::Float(f) => serde_json::json!(f),
        Value::String(s) => serde_json::json!(s),
        Value::Bytes(b) => serde_json::json!(b.as_ref()),
        Value::List(items) => {
            serde_json::Value::Array(items.iter().map(value_to_plain_json).collect())
        }
        Value::Tuple(items) => {
            serde_json::Value::Array(items.iter().map(value_to_plain_json).collect())
        }
        Value::Struct(fields) => {
            let mut obj = serde_json::Map::new();
            for f in fields {
                obj.insert(f.name.clone(), value_to_plain_json(&f.value));
            }
            serde_json::Value::Object(obj)
        }
        Value::Enum(ev) => {
            let mut obj = serde_json::Map::new();
            obj.insert("name".into(), serde_json::Value::String(ev.name.clone()));
            if let Some(v) = &ev.value {
                obj.insert("value".into(), value_to_plain_json(v));
            }
            serde_json::Value::Object(obj)
        }
        Value::Map(entries) => {
            // Prefer object form if all keys are strings (common for configs/struct-ish payloads).
            let mut obj = serde_json::Map::new();
            let mut all_string_keys = true;
            for (k, _v) in entries {
                if !matches!(k, Value::String(_)) {
                    all_string_keys = false;
                    break;
                }
            }
            if all_string_keys {
                for (k, v) in entries {
                    if let Value::String(s) = k {
                        obj.insert(s.to_string(), value_to_plain_json(v));
                    }
                }
                return serde_json::Value::Object(obj);
            }

            // Fallback: encode as list of [k,v] pairs.
            serde_json::Value::Array(
                entries
                    .iter()
                    .map(|(k, v)| {
                        serde_json::Value::Array(vec![
                            value_to_plain_json(k),
                            value_to_plain_json(v),
                        ])
                    })
                    .collect(),
            )
        }
    }
}

pub(crate) fn any_to_json(v: &dyn Any) -> Option<serde_json::Value> {
    if let Some(j) = v.downcast_ref::<serde_json::Value>() {
        return Some(j.clone());
    }
    if let Some(val) = v.downcast_ref::<Value>() {
        return Some(value_to_plain_json(val));
    }
    if let Some(img) = v.downcast_ref::<DynamicImage>() {
        // Fast path: ship raw RGBA8 bytes (no PNG encoding).
        let rgba = img.to_rgba8();
        let bytes = rgba.into_raw();
        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        return Some(serde_json::json!({
            "data_b64": b64,
            "width": img.width() as i64,
            "height": img.height() as i64,
            "channels": 4,
            "dtype": "u8",
            "layout": "HWC",
            "encoding": "raw",
        }));
    }
    if v.type_id() == std::any::TypeId::of::<DynamicImage>() {
        // SAFETY: v really is a DynamicImage; we've checked its TypeId.
        let img: &DynamicImage = unsafe { &*(v as *const _ as *const DynamicImage) };
        return any_to_json(img);
    }
    if let Some(img_arc) = v.downcast_ref::<Arc<DynamicImage>>()
        && let Some(json) = any_to_json(img_arc.as_ref())
    {
        return Some(json);
    }
    if let Some(img) = v.downcast_ref::<ImageCompute>() {
        return Some(serde_json::json!({
            "data_b64": img.data_b64,
            "width": img.width,
            "height": img.height,
            "channels": img.channels,
            "dtype": img.dtype,
            "layout": img.layout,
        }));
    }
    // Cross-dylib type ids won't match, so also fall back to a name check and raw pointer cast.
    let type_name = std::any::type_name_of_val(v);
    if type_name == std::any::type_name::<ImageCompute>() || type_name.ends_with("::ImageCompute") {
        if std::mem::size_of_val(v) != std::mem::size_of::<ImageCompute>()
            || std::mem::align_of_val(v) != std::mem::align_of::<ImageCompute>()
        {
            return None;
        }
        // SAFETY: ImageCompute is `#[repr(C)]` and contains only `String` + scalar fields; when
        // the dynamic type name matches and the runtime size/alignment match, it's safe to
        // reinterpret across dylibs (even if `TypeId` differs).
        let (data_ptr, _): (*const (), *const ()) = unsafe { std::mem::transmute(v) };
        let img: &ImageCompute = unsafe { &*(data_ptr as *const ImageCompute) };
        return Some(serde_json::json!({
            "data_b64": img.data_b64,
            "width": img.width,
            "height": img.height,
            "channels": img.channels,
            "dtype": img.dtype,
            "layout": img.layout,
        }));
    }
    if let Some(i) = v.downcast_ref::<i32>() {
        return Some(serde_json::json!(i));
    }
    if let Some(i) = v.downcast_ref::<u32>() {
        return Some(serde_json::json!(i));
    }
    if let Some(i) = v.downcast_ref::<i64>() {
        return Some(serde_json::json!(i));
    }
    if let Some(f) = v.downcast_ref::<f32>() {
        return Some(serde_json::json!(f));
    }
    if let Some(f) = v.downcast_ref::<f64>() {
        return Some(serde_json::json!(f));
    }
    if let Some(b) = v.downcast_ref::<bool>() {
        return Some(serde_json::json!(b));
    }
    if let Some(s) = v.downcast_ref::<String>() {
        return Some(serde_json::json!(s));
    }
    if let Some(s) = v.downcast_ref::<&str>() {
        return Some(serde_json::json!(s));
    }
    if let Some(bytes) = v.downcast_ref::<Vec<u8>>() {
        return Some(serde_json::json!(bytes));
    }
    if let Some(bytes) = v.downcast_ref::<Arc<Vec<u8>>>() {
        return Some(serde_json::json!(bytes));
    }
    if let Some(map) = v.downcast_ref::<std::collections::HashMap<String, serde_json::Value>>() {
        return Some(serde_json::json!(map));
    }
    if let Some(map) = v.downcast_ref::<BTreeMap<String, serde_json::Value>>() {
        return Some(serde_json::json!(map));
    }
    None
}

pub(crate) fn inputs_to_json(
    io: &NodeIo,
    inputs: &[ManifestPort],
) -> Result<Vec<serde_json::Value>, NodeError> {
    let mut out = Vec::with_capacity(inputs.len());
    for port in inputs {
        match io.get_any_raw(&port.name) {
            Some(any) => {
                if let Some(v) = any_to_json(any) {
                    out.push(v);
                } else {
                    return Err(NodeError::InvalidInput(format!(
                        "unsupported input {}",
                        port.name
                    )));
                }
            }
            None => {
                if let Some(value) = io.get_value(&port.name) {
                    out.push(value_to_plain_json(value));
                    continue;
                }
                // Allow sync-group-driven partial invocation by representing missing ports as null.
                // Language adapters should treat this like an absent/optional value.
                out.push(serde_json::Value::Null);
            }
        }
    }
    Ok(out)
}

pub(crate) enum OutputVal {
    I64(i64),
    I32(i32),
    U32(u32),
    F32(f32),
    F64(f64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
    Unit,
}

pub(crate) fn json_to_output(val: serde_json::Value, ty: &TypeExpr) -> Result<OutputVal, String> {
    match ty {
        TypeExpr::Scalar(s) => match s {
            daedalus_data::model::ValueType::Int => {
                let num = val
                    .as_i64()
                    .ok_or_else(|| format!("expected int, got {val}"))?;
                Ok(OutputVal::I64(num))
            }
            daedalus_data::model::ValueType::I32 => {
                let num = val
                    .as_i64()
                    .ok_or_else(|| format!("expected i32, got {val}"))?;
                let out: i32 = num
                    .try_into()
                    .map_err(|_| format!("i32 out of range: {num}"))?;
                Ok(OutputVal::I32(out))
            }
            daedalus_data::model::ValueType::U32 => {
                let num = val
                    .as_u64()
                    .ok_or_else(|| format!("expected u32, got {val}"))?;
                let out: u32 = num
                    .try_into()
                    .map_err(|_| format!("u32 out of range: {num}"))?;
                Ok(OutputVal::U32(out))
            }
            daedalus_data::model::ValueType::Float => {
                let num = val
                    .as_f64()
                    .ok_or_else(|| format!("expected float, got {val}"))?;
                Ok(OutputVal::F64(num))
            }
            daedalus_data::model::ValueType::F32 => {
                let num = val
                    .as_f64()
                    .ok_or_else(|| format!("expected f32, got {val}"))?;
                Ok(OutputVal::F32(num as f32))
            }
            daedalus_data::model::ValueType::Bool => {
                let b = val
                    .as_bool()
                    .ok_or_else(|| format!("expected bool, got {val}"))?;
                Ok(OutputVal::Bool(b))
            }
            daedalus_data::model::ValueType::String => {
                let s = val
                    .as_str()
                    .ok_or_else(|| format!("expected string, got {val}"))?;
                Ok(OutputVal::String(s.to_string()))
            }
            daedalus_data::model::ValueType::Bytes => {
                if let Some(arr) = val.as_array() {
                    let mut bytes = Vec::with_capacity(arr.len());
                    for v in arr {
                        let n = v
                            .as_i64()
                            .ok_or_else(|| format!("expected byte, got {v}"))?;
                        bytes.push(n as u8);
                    }
                    return Ok(OutputVal::Bytes(bytes));
                }
                if let Some(s) = val.as_str() {
                    return Ok(OutputVal::Bytes(s.as_bytes().to_vec()));
                }
                Err(format!("expected bytes, got {val}"))
            }
            daedalus_data::model::ValueType::Unit => Ok(OutputVal::Unit),
        },
        TypeExpr::Struct(_)
        | TypeExpr::List(_)
        | TypeExpr::Map(_, _)
        | TypeExpr::Tuple(_)
        | TypeExpr::Enum(_) => Ok(OutputVal::Json(val)),
        other => Err(format!("unsupported output type {other:?}")),
    }
}

pub(crate) fn push_output(io: &mut NodeIo, port: &str, val: OutputVal) {
    match val {
        OutputVal::I64(v) => io.push_any(Some(port), v),
        OutputVal::I32(v) => io.push_any(Some(port), v),
        OutputVal::U32(v) => io.push_any(Some(port), v),
        OutputVal::F32(v) => io.push_any(Some(port), v),
        OutputVal::F64(v) => io.push_any(Some(port), v),
        OutputVal::Bool(v) => io.push_any(Some(port), v),
        OutputVal::String(v) => io.push_any(Some(port), v),
        OutputVal::Bytes(v) => io.push_any(Some(port), v),
        OutputVal::Json(v) => io.push_any(Some(port), v),
        OutputVal::Unit => io.push_any::<()>(Some(port), ()),
    }
}

pub(crate) fn manifest_node_to_descriptor(node: &NodeManifest) -> Result<NodeDescriptor, String> {
    let mut inputs: Vec<Port> = Vec::with_capacity(node.inputs.len());
    for p in &node.inputs {
        let const_value = match &p.const_value {
            Some(v) => Some(json_to_value(v.clone())?),
            None => None,
        };
        inputs.push(Port {
            name: p.name.clone(),
            ty: p.ty.clone(),
            access: Default::default(),
            source: p.source.clone(),
            const_value,
        });
    }

    let mut outputs: Vec<Port> = node
        .outputs
        .iter()
        .map(|p| Port {
            name: p.name.clone(),
            ty: p.ty.clone(),
            access: Default::default(),
            source: p.source.clone(),
            const_value: None,
        })
        .collect();

    inputs.sort_by(|a, b| a.name.cmp(&b.name));
    outputs.sort_by(|a, b| a.name.cmp(&b.name));

    let sync_groups: Vec<SyncGroup> = node
        .sync_groups
        .iter()
        .enumerate()
        .map(|(idx, group)| match group {
            ManifestSyncGroup::Ports(ports) => SyncGroup {
                name: format!("group{idx}"),
                policy: SyncPolicy::AllReady,
                backpressure: None,
                capacity: None,
                ports: ports.clone(),
            },
            ManifestSyncGroup::Group(spec) => SyncGroup {
                name: spec.name.clone().unwrap_or_else(|| format!("group{idx}")),
                policy: spec.policy,
                backpressure: spec.backpressure.clone(),
                capacity: spec.capacity,
                ports: spec.ports.clone(),
            },
        })
        .collect();

    let metadata = json_map_to_values(&node.metadata)?;

    let default_compute = if node.shader.is_some()
        && matches!(
            node.default_compute,
            daedalus_core::compute::ComputeAffinity::CpuOnly
        ) {
        daedalus_core::compute::ComputeAffinity::GpuRequired
    } else {
        node.default_compute
    };

    let desc = NodeDescriptor {
        id: NodeId::new(node.id.clone()),
        feature_flags: node.feature_flags.clone(),
        label: node.label.clone(),
        group: None,
        inputs,
        fanin_inputs: Vec::new(),
        outputs,
        default_compute,
        sync_groups,
        metadata,
    };
    desc.validate().map_err(|e| e.to_string())?;
    Ok(desc)
}
