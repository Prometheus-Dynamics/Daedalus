//! Loaded mode: plan blobs (the format is documented in docs/mcu.md and read by
//! `daedalus_mcu::loaded`), and the plan manifest that names ports and parameters by id.

use daedalus_data::model::Value;
use daedalus_mcu::loaded::{FORMAT_VERSION, MAGIC};
use daedalus_mcu::wire::Write;
use daedalus_mcu::{Overflow, ParamSpec, ParamUpdate, Scalar, ScalarKind, name_hash};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

use crate::lower::{Endpoint, HostPort, InputSource, McuPlan, scalar_value};
use crate::{CompileError, LibraryManifest};

/// The device codec writing into a `Vec`.
struct Bytes(Vec<u8>);

impl Write for Bytes {
    fn put(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}

fn unsupported(message: String) -> CompileError {
    CompileError::Unsupported(message)
}

impl McuPlan {
    /// Encode the plan as a blob for a firmware with `library` (deterministic: the same plan
    /// and library give the same bytes).
    pub fn to_blob(&self, library: &LibraryManifest) -> Result<Vec<u8>, CompileError> {
        let mut w = Bytes(Vec::new());
        w.put(&MAGIC);
        w.put(&[FORMAT_VERSION]);
        w.varint(library.hash);
        w.varint(self.hash);
        w.varint(self.nodes.len() as u64);
        for node in &self.nodes {
            let entry = u8::try_from(node.entry)
                .map_err(|_| unsupported(format!("node `{}`: library entry", node.name)))?;
            w.put(&[entry]);
        }
        for ports in [&self.host_inputs, &self.host_outputs] {
            w.varint(ports.len() as u64);
            for port in ports {
                let ty = library.type_id(&port.key).ok_or_else(|| {
                    unsupported(format!(
                        "host port `{}`: type `{}` not in the library",
                        port.name, port.key
                    ))
                })?;
                w.varint(name_hash(&port.name).into());
                w.varint(ty.into());
            }
        }
        w.varint(self.nodes.len() as u64);
        for node in &self.nodes {
            w.put(&[node.wait_all.into()]);
            w.varint(node.inputs.len() as u64);
            for input in &node.inputs {
                match input {
                    InputSource::Absent => w.put(&[0]),
                    InputSource::Edge { edge, .. } => {
                        w.put(&[1]);
                        self.write_edge(&mut w, *edge)?;
                    }
                    InputSource::Const {
                        value: Some(value), ..
                    } => {
                        w.put(&[2]);
                        w.scalar(*value);
                    }
                    InputSource::Const { expr, .. } => {
                        return Err(unsupported(format!(
                            "constant {expr} of `{}`: loaded plans take scalar constants",
                            node.name
                        )));
                    }
                    InputSource::Param { param, .. } => {
                        let param = &self.params[*param];
                        w.put(&[3]);
                        for value in [param.value, param.min, param.max] {
                            w.scalar(value);
                        }
                    }
                }
            }
        }
        w.varint(self.host_outputs.len() as u64);
        for port in &self.host_outputs {
            self.write_edge(&mut w, port.edges[0])?;
        }
        Ok(w.0)
    }

    fn write_edge(&self, w: &mut Bytes, edge: usize) -> Result<(), CompileError> {
        let edge = &self.edges[edge];
        match edge.from {
            Endpoint::Host { port } => {
                w.put(&[0]);
                w.varint(port as u64);
            }
            Endpoint::Node { node, output } => {
                w.put(&[1]);
                w.varint(node as u64);
                w.put(&[output as u8]);
            }
        }
        let capacity = u16::try_from(edge.capacity)
            .map_err(|_| unsupported(format!("edge {}: capacity", edge.label)))?;
        w.varint(capacity.into());
        let overflow = Overflow::ALL.iter().position(|o| *o == edge.overflow);
        w.put(&[overflow.unwrap_or_default() as u8]);
        Ok(())
    }

    /// Graph edge indices in blob order (`McuError::QueueFull::edge` of a loaded plan).
    pub fn blob_edges(&self) -> Vec<usize> {
        let inputs = self.nodes.iter().flat_map(|node| &node.inputs);
        let into_nodes = inputs.filter_map(|input| match input {
            InputSource::Edge { edge, .. } => Some(*edge),
            _ => None,
        });
        into_nodes
            .chain(self.host_outputs.iter().map(|port| port.edges[0]))
            .collect()
    }

    /// The manifest of the compiled module (`library` = `None`) or of the blob for `library`.
    pub fn manifest(&self, library: Option<&LibraryManifest>) -> PlanManifest {
        let edges = match library {
            Some(_) => self.blob_edges(),
            None => (0..self.edges.len()).collect(),
        };
        let ports = |ports: &[HostPort]| {
            ports
                .iter()
                .enumerate()
                .map(|(id, port)| PortManifest {
                    id: id as u16,
                    name: port.name.clone(),
                    ty: port.ty.clone(),
                })
                .collect()
        };
        PlanManifest {
            format: PlanManifest::FORMAT.into(),
            plan_hash: self.hash,
            library_hash: library.map(|library| library.hash),
            nodes: self.nodes.iter().map(|node| node.name.clone()).collect(),
            edges: edges.iter().map(|&e| self.edges[e].label.clone()).collect(),
            inputs: ports(&self.host_inputs),
            outputs: ports(&self.host_outputs),
            params: self
                .params
                .iter()
                .enumerate()
                .map(|(id, param)| {
                    let (min, max) = param.value.kind().bounds();
                    let bound = |b: Scalar, full: Scalar| (b != full).then(|| to_json(b));
                    ParamManifest {
                        id: id as u16,
                        name: param.name.clone(),
                        ty: param.value.kind().rust_name().into(),
                        default: to_json(param.value),
                        min: bound(param.min, min),
                        max: bound(param.max, max),
                    }
                })
                .collect(),
        }
    }
}

/// A plan's ids for host tools: node, edge, host port and parameter names in id order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanManifest {
    /// [`PlanManifest::FORMAT`].
    pub format: String,
    pub plan_hash: u64,
    /// The library a loaded plan was compiled for; `None` for a compiled module.
    pub library_hash: Option<u64>,
    /// `McuError::Node::node`.
    pub nodes: Vec<String>,
    /// `McuError::QueueFull::edge`.
    pub edges: Vec<String>,
    pub inputs: Vec<PortManifest>,
    pub outputs: Vec<PortManifest>,
    pub params: Vec<ParamManifest>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PortManifest {
    pub id: u16,
    pub name: String,
    /// Rust type.
    pub ty: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParamManifest {
    pub id: u16,
    /// `<node>.<input>`.
    pub name: String,
    /// Rust scalar type.
    pub ty: String,
    pub default: Json,
    /// Range bounds; absent for the type's own limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<Json>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<Json>,
}

impl PlanManifest {
    pub const FORMAT: &'static str = "daedalus.mcu.plan";

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }

    pub fn from_json(json: &str) -> Result<Self, CompileError> {
        let manifest: Self =
            serde_json::from_str(json).map_err(|err| CompileError::Manifest(err.to_string()))?;
        if manifest.format != Self::FORMAT {
            return Err(CompileError::Manifest("not a plan manifest".into()));
        }
        Ok(manifest)
    }

    /// The `ParamUpdate` message setting parameter `name` to `value`, checked on the host with
    /// the device's rules (type conversion, then range).
    pub fn param_update(&self, name: &str, value: &Value) -> Result<Vec<u8>, CompileError> {
        let bad = |why: String| CompileError::Manifest(format!("parameter `{name}`: {why}"));
        let param = self
            .params
            .iter()
            .find(|p| p.name == name)
            .ok_or_else(|| bad("no such parameter".into()))?;
        let kind = ScalarKind::ALL
            .into_iter()
            .find(|k| k.rust_name() == param.ty)
            .ok_or_else(|| bad(format!("type `{}`", param.ty)))?;
        let (full_min, full_max) = kind.bounds();
        let bound = |json: &Option<Json>, full: Scalar| match json {
            None => Ok(full),
            Some(json) => from_json(json)
                .and_then(|v| v.coerce(kind))
                .ok_or_else(|| bad("range".into())),
        };
        let spec = ParamSpec {
            kind,
            min: bound(&param.min, full_min)?,
            max: bound(&param.max, full_max)?,
        };
        let value = scalar_value(kind, value).map_err(bad)?;
        let value = spec.check(value).map_err(|err| {
            bad(format!(
                "{err:?}: {value:?} outside {:?}..={:?}",
                spec.min, spec.max
            ))
        })?;
        let mut w = Bytes(Vec::new());
        ParamUpdate {
            id: param.id,
            value,
        }
        .encode(&mut w);
        Ok(w.0)
    }
}

fn to_json(value: Scalar) -> Json {
    let float = |v: f64| {
        serde_json::Number::from_f64(v).map_or_else(|| Json::String(format!("{v}")), Json::Number)
    };
    match value {
        Scalar::Bool(b) => Json::Bool(b),
        Scalar::I8(v) => v.into(),
        Scalar::I16(v) => v.into(),
        Scalar::I32(v) => v.into(),
        Scalar::I64(v) => v.into(),
        Scalar::U8(v) => v.into(),
        Scalar::U16(v) => v.into(),
        Scalar::U32(v) => v.into(),
        Scalar::U64(v) => v.into(),
        Scalar::F32(v) => float(v.into()),
        Scalar::F64(v) => float(v),
    }
}

fn from_json(json: &Json) -> Option<Scalar> {
    match json {
        Json::Bool(b) => Some(Scalar::Bool(*b)),
        Json::Number(n) => n
            .as_i64()
            .map(Scalar::I64)
            .or_else(|| n.as_u64().map(Scalar::U64))
            .or_else(|| n.as_f64().map(Scalar::F64)),
        Json::String(s) => s.parse::<f64>().ok().map(Scalar::F64),
        _ => None,
    }
}
