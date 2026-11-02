//! Loaded mode: the node library of a firmware, as generated Rust (`LIBRARY`) and as a JSON
//! manifest for host tools.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use daedalus_mcu::loaded::{MAX_ADAPTERS, MAX_PORTS, SCALAR_TYPES};
use serde::{Deserialize, Serialize};

use crate::lower::{TypeNames, scalar_kind};
use crate::{AdapterSpec, CompileError, NodeSpec, registry};

const MCU: &str = "::daedalus_mcu";

/// A firmware's node library: the nodes in entry order, the user adapters, their non-scalar port
/// types in type-id order (after the [`daedalus_mcu::ScalarKind`]s), and the hash of their
/// interfaces.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LibraryManifest {
    /// [`LibraryManifest::FORMAT`].
    pub format: String,
    pub hash: u64,
    pub nodes: Vec<NodeSpec>,
    /// User adapters, in index order (an edge of a plan names one by index).
    #[serde(default)]
    pub adapters: Vec<AdapterSpec>,
    pub types: Vec<String>,
}

impl LibraryManifest {
    pub const FORMAT: &'static str = "daedalus.mcu.library";

    pub fn new(nodes: Vec<NodeSpec>, adapters: Vec<AdapterSpec>) -> Result<Self, CompileError> {
        if nodes.len() > 256 {
            return Err(CompileError::Unsupported(
                "a node library has at most 256 nodes".into(),
            ));
        }
        if adapters.len() > MAX_ADAPTERS {
            return Err(CompileError::Unsupported(format!(
                "a node library has at most {MAX_ADAPTERS} user adapters"
            )));
        }
        let mut ids = BTreeSet::new();
        let mut types = Vec::new();
        let mut canonical = String::new();
        for node in &nodes {
            if !ids.insert(&node.id) {
                return Err(CompileError::Unsupported(format!(
                    "node `{}` twice in the library",
                    node.id
                )));
            }
            if node.inputs.len() > MAX_PORTS || node.outputs.len() > MAX_PORTS {
                return Err(CompileError::Unsupported(format!(
                    "node `{}`: loaded mode runs nodes with at most {MAX_PORTS} inputs and outputs",
                    node.id
                )));
            }
            let _ = writeln!(canonical, "node {} {}", node.id, node.fire_all);
            for (dir, ports) in [("in", &node.inputs), ("out", &node.outputs)] {
                for port in ports {
                    let _ = writeln!(
                        canonical,
                        "{dir} {} {} {}",
                        port.name, port.key, port.optional
                    );
                    if scalar_kind(&port.key).is_none() && !types.contains(&port.key) {
                        types.push(port.key.clone());
                    }
                }
            }
        }
        for adapter in &adapters {
            if !ids.insert(&adapter.id) {
                return Err(CompileError::Unsupported(format!(
                    "adapter `{}` twice in the library",
                    adapter.id
                )));
            }
            let _ = writeln!(
                canonical,
                "adapter {} {} {}",
                adapter.id, adapter.from, adapter.to
            );
            for key in [&adapter.from, &adapter.to] {
                if scalar_kind(key).is_none() && !types.contains(key) {
                    types.push(key.clone());
                }
            }
        }
        registry(&nodes, &adapters)?;
        Ok(Self {
            format: Self::FORMAT.into(),
            hash: fnv1a64(canonical.as_bytes()),
            nodes,
            adapters,
            types,
        })
    }

    /// Read a manifest written by [`Self::to_json`], checking its hash.
    pub fn from_json(json: &str) -> Result<Self, CompileError> {
        let read: Self =
            serde_json::from_str(json).map_err(|err| CompileError::Manifest(err.to_string()))?;
        let rebuilt = Self::new(read.nodes.clone(), read.adapters.clone())?;
        if read.format != Self::FORMAT || rebuilt != read {
            return Err(CompileError::Manifest(
                "not a library manifest, or its hash or types do not match its nodes".into(),
            ));
        }
        Ok(read)
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }

    /// The index of the user adapter `id`.
    pub fn adapter_index(&self, id: &str) -> Option<u8> {
        let index = self.adapters.iter().position(|adapter| adapter.id == id)?;
        u8::try_from(index).ok()
    }

    /// The type id of transport key `key`.
    pub fn type_id(&self, key: &str) -> Option<u16> {
        if let Some(kind) = scalar_kind(key) {
            return Some(kind as u16);
        }
        let index = self.types.iter().position(|ty| ty == key)?;
        Some(SCALAR_TYPES + index as u16)
    }

    /// The Rust module defining `LIBRARY_HASH` and `LIBRARY` (a `daedalus_mcu::loaded::Library`)
    /// with a type-erased `run` adapter per node, for `include!` in the firmware.
    pub fn to_rust(&self) -> Result<String, CompileError> {
        let names = TypeNames::new(&self.nodes, &self.adapters);
        let types = self
            .types
            .iter()
            .map(|key| {
                Ok(format!(
                    "{MCU}::loaded::TypeInfo::of::<{}>()",
                    names.of_key(key)?
                ))
            })
            .collect::<Result<Vec<_>, CompileError>>()?;
        let mut out = String::new();
        let ids: Vec<String> = self.nodes.iter().map(|n| format!("{:?}", n.id)).collect();
        let _ = writeln!(
            out,
            "// @generated by daedalus-mcu-build: node library {hash:016x}; do not edit.\n\n\
             /// Hash of the node interfaces: plans compiled for another library are rejected.\n\
             pub const LIBRARY_HASH: u64 = {hash:#x};\n\
             /// Node ids in entry order.\n\
             pub const LIBRARY_NODES: [&str; {}] = [{}];\n\n\
             /// The node library of this firmware (`daedalus_mcu::loaded::Interpreter::new`).\n\
             pub static LIBRARY: {MCU}::loaded::Library = {MCU}::loaded::Library {{\n    \
             hash: LIBRARY_HASH,\n    types: &[{}],\n    nodes: &[",
            ids.len(),
            ids.join(", "),
            types.join(", "),
            hash = self.hash,
        );
        for (i, node) in self.nodes.iter().enumerate() {
            let ty = |key: &str| self.type_id(key).unwrap_or_default();
            let inputs: Vec<String> = node
                .inputs
                .iter()
                .map(|p| {
                    format!(
                        "{MCU}::loaded::InputEntry {{ ty: {}, optional: {} }}",
                        ty(&p.key),
                        p.optional
                    )
                })
                .collect();
            let outputs: Vec<String> = node
                .outputs
                .iter()
                .map(|p| ty(&p.key).to_string())
                .collect();
            let _ = writeln!(
                out,
                "        // {}\n        {MCU}::loaded::NodeEntry {{\n            \
                 inputs: &[{}],\n            outputs: &[{}],\n            \
                 state: {MCU}::loaded::StateEntry::of::<::{}::State>(),\n            \
                 run: run_{i},\n        }},",
                node.id,
                inputs.join(", "),
                outputs.join(", "),
                node.path
            );
        }
        out.push_str("    ],\n    adapters: &[");
        for (i, adapter) in self.adapters.iter().enumerate() {
            let ty = |key: &str| self.type_id(key).unwrap_or_default();
            let _ = write!(
                out,
                "\n        // {}\n        {MCU}::loaded::AdapterEntry {{ input: {}, output: {}, run: adapt_{i} }},",
                adapter.id,
                ty(&adapter.from),
                ty(&adapter.to),
            );
        }
        out.push_str(if self.adapters.is_empty() {
            "],\n};\n"
        } else {
            "\n    ],\n};\n"
        });
        for (i, adapter) in self.adapters.iter().enumerate() {
            let _ = writeln!(
                out,
                "\n/// Adapter `{id}`: converts the input value at `input` into the output slot.\n\
                 unsafe fn adapt_{i}(input: *const u8, output: *mut u8) {{\n    \
                 // SAFETY: the interpreter passes a value of the adapter's input type and a slot of\n    \
                 // its output type (checked when the plan was loaded).\n    unsafe {{\n        \
                 let value = input.cast::<::{path}::In>().read_unaligned();\n        \
                 output.cast::<::{path}::Out>().write_unaligned(::{path}::run(value));\n    }}\n}}",
                id = adapter.id,
                path = adapter.path,
            );
        }
        for (i, node) in self.nodes.iter().enumerate() {
            let args: Vec<String> = node
                .inputs
                .iter()
                .enumerate()
                .map(|(k, p)| {
                    let getter = if p.optional { "input_opt" } else { "input" };
                    format!("io.{getter}({k})")
                })
                .collect();
            let _ = writeln!(
                out,
                "\n/// `{id}`\n#[allow(unused_variables, clippy::let_unit_value)]\n\
                 unsafe fn run_{i}(\n    state: *mut u8,\n    ctx: &{MCU}::Ctx,\n    \
                 io: &mut {MCU}::loaded::Io,\n) -> Result<(), {MCU}::NodeError> {{\n    \
                 // SAFETY: the interpreter passes this entry's state slot and values of its port\n    \
                 // types (checked when the plan was loaded).\n    unsafe {{\n        \
                 let out = ::{path}::run(&mut *state.cast::<::{path}::State>(), ctx{sep}{})?;",
                args.join(", "),
                id = node.id,
                path = node.path,
                sep = if args.is_empty() { "" } else { ", " },
            );
            for k in 0..node.outputs.len() {
                let _ = writeln!(out, "        io.output({k}, out.{k});");
            }
            out.push_str("    }\n    Ok(())\n}\n");
        }
        Ok(out)
    }
}

/// 64-bit FNV-1a.
fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}
