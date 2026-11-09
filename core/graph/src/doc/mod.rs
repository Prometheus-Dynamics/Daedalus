//! The JSON document. Strict: an unknown field at any level is an error naming its path.

#![cfg(feature = "alloc")]

mod hex;
mod patch;
#[cfg(feature = "schema")]
mod schema;

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt;

use serde::{Deserialize, Serialize};

pub use patch::{GraphPatch, NodeSelector, PatchOp};
#[cfg(feature = "schema")]
pub use schema::json_schema;

/// The value of the `format` field.
pub const FORMAT: &str = "daedalus.graph";
/// The only schema version this build reads and writes.
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum DocError {
    /// Not valid, at this JSON path.
    Invalid {
        path: String,
        message: String,
    },
    UnknownFormat {
        found: String,
    },
    UnsupportedVersion {
        found: u32,
    },
    Serialize(String),
}

impl fmt::Display for DocError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DocError::Invalid { path, message } => {
                write!(f, "invalid graph document at `{path}`: {message}")
            }
            DocError::UnknownFormat { found } => {
                write!(
                    f,
                    "unknown graph document format `{found}` (expected `{FORMAT}`)"
                )
            }
            DocError::UnsupportedVersion { found } => write!(
                f,
                "unsupported schema_version {found} (this build reads {SCHEMA_VERSION})"
            ),
            DocError::Serialize(message) => write!(f, "cannot write graph document: {message}"),
        }
    }
}

impl core::error::Error for DocError {}

/// A constant, in its JSON form: `{ "type": ..., "value": ... }`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Lit {
    Unit,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Bytes(#[serde(with = "hex")] Vec<u8>),
    List(Vec<Lit>),
    Struct(Vec<LitField>),
    Enum {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<Box<Lit>>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LitField {
    pub name: String,
    pub value: Lit,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requirement {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKindDoc {
    #[default]
    Node,
    Group,
    HostBridge,
    Control,
}

fn is_default_kind(kind: &NodeKindDoc) -> bool {
    *kind == NodeKindDoc::Node
}

fn is_false(flag: &bool) -> bool {
    !flag
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateDoc {
    pub num: u32,
    pub den: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDoc {
    pub id: String,
    #[serde(default, skip_serializing_if = "is_default_kind")]
    pub kind: NodeKindDoc,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub consts: BTreeMap<String, Lit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fire: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reusable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate: Option<RateDoc>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub shareable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ui_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub port_types: BTreeMap<String, String>,
    /// How a host bridge's inputs behave, by port. A port not named here is consumed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub host_inputs: BTreeMap<String, HostInputPolicy>,
    /// Input ports that must arrive together, and how.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sync_groups: Vec<SyncGroupDoc>,
    /// The input ports a stored graph lists. Empty means none are listed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<String>,
    /// The output ports a stored graph lists. Empty means none are listed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Lit>,
    /// What a `control` node does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control: Option<ControlDoc>,
}

impl NodeDoc {
    /// A node with every optional field empty.
    pub fn new(id: impl Into<String>) -> NodeDoc {
        NodeDoc {
            id: id.into(),
            kind: NodeKindDoc::Node,
            label: None,
            backend: None,
            consts: BTreeMap::new(),
            fire: None,
            reusable: None,
            rate: None,
            shareable: false,
            ui_id: None,
            port_types: BTreeMap::new(),
            host_inputs: BTreeMap::new(),
            sync_groups: Vec::new(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            metadata: BTreeMap::new(),
            control: None,
        }
    }
}

/// Which control operator a node is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlOp {
    If,
    ForEach,
    Until,
}

/// A control node's operator and its named body graphs. Each body is closed: its one host bridge
/// holds its ports, and nothing in it names anything outside.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlDoc {
    pub op: ControlOp,
    /// `until` only: how many times the body may run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u32>,
    pub bodies: BTreeMap<String, GraphBody>,
}

/// How the runner lines up a sync group's inputs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicy {
    #[default]
    AllReady,
    Latest,
    ZipByTag,
}

/// Input ports that must arrive together.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncGroupDoc {
    pub name: String,
    #[serde(default)]
    pub policy: SyncPolicy,
    pub ports: Vec<String>,
}

/// What the host's value does once a node has read it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostInputPolicy {
    /// Read once.
    #[default]
    Consume,
    /// Stays until the host replaces it, and is read every tick.
    Held,
    /// Stays, and other graphs read the same value.
    Shared,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortRefDoc {
    pub node: u32,
    pub port: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EdgePolicyDoc {
    #[default]
    Fifo,
    LatestOnly,
    Bounded {
        capacity: u32,
        overflow: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeDoc {
    pub from: PortRefDoc,
    pub to: PortRefDoc,
    #[serde(default)]
    pub policy: EdgePolicyDoc,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagePortDoc {
    pub stage: String,
    pub port: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageConstDoc {
    pub at: StagePortDoc,
    pub value: Lit,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupDoc {
    pub id: String,
    /// Stage name to node id.
    pub stages: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connects: Vec<(StagePortDoc, StagePortDoc)>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub inputs: BTreeMap<String, Vec<StagePortDoc>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub outputs: BTreeMap<String, StagePortDoc>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consts: Vec<StageConstDoc>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphBody {
    pub nodes: Vec<NodeDoc>,
    #[serde(default)]
    pub edges: Vec<EdgeDoc>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<GroupDoc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphDoc {
    pub format: String,
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<Requirement>,
    /// Free-form application data, such as editor state. Ordered, so output is deterministic.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
    pub graph: GraphBody,
}

impl GraphDoc {
    /// A document around `graph`, at the current format and version.
    pub fn new(graph: GraphBody) -> GraphDoc {
        GraphDoc {
            format: FORMAT.to_string(),
            schema_version: SCHEMA_VERSION,
            requires: Vec::new(),
            metadata: BTreeMap::new(),
            graph,
        }
    }

    pub fn parse(text: &str) -> Result<GraphDoc, DocError> {
        let doc: GraphDoc = parse_json(text)?;
        if doc.format != FORMAT {
            return Err(DocError::UnknownFormat { found: doc.format });
        }
        if doc.schema_version != SCHEMA_VERSION {
            return Err(DocError::UnsupportedVersion {
                found: doc.schema_version,
            });
        }
        Ok(doc)
    }

    /// Pretty JSON with a trailing newline.
    pub fn to_json(&self) -> Result<String, DocError> {
        to_json(self)
    }
}

pub(crate) fn parse_json<'a, T: Deserialize<'a>>(text: &'a str) -> Result<T, DocError> {
    let mut json = serde_json::Deserializer::from_str(text);
    serde_path_to_error::deserialize(&mut json).map_err(|error| DocError::Invalid {
        path: error.path().to_string(),
        message: error.inner().to_string(),
    })
}

pub(crate) fn to_json<T: Serialize>(value: &T) -> Result<String, DocError> {
    let mut text = serde_json::to_string_pretty(value)
        .map_err(|error| DocError::Serialize(error.to_string()))?;
    text.push('\n');
    Ok(text)
}
