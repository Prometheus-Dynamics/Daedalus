//! Versioned, persisted graph document format.
//!
//! A [`GraphDocument`] wraps a planner [`Graph`] with a format marker, a schema version, the
//! plugins the graph needs, and free-form application metadata:
//!
//! ```json
//! {
//!   "format": "daedalus.graph",
//!   "schema_version": 1,
//!   "requires": [{ "id": "demo.math", "version": ">=1.2.0" }],
//!   "metadata": { "title": "Example" },
//!   "graph": { "nodes": [], "edges": [], "metadata": {} }
//! }
//! ```
//!
//! Documents are strict: unknown fields are rejected at every level of the format.

mod requirements;
#[cfg(feature = "schema")]
mod schema;

use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::fmt;

use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::graph::Graph;

pub use requirements::{
    MissingPlugins, PluginRequirement, UnmetReason, UnmetRequirement, check_plugin_requirements,
};

/// Value of the `format` marker field.
pub const GRAPH_DOCUMENT_FORMAT: &str = "daedalus.graph";
/// Current (and highest supported) graph document schema version.
pub const GRAPH_DOCUMENT_SCHEMA_VERSION: u32 = 1;

/// Errors produced while reading or writing a [`GraphDocument`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GraphDocumentError {
    #[error("graph document is not valid JSON: {0}")]
    Syntax(#[source] serde_json::Error),
    #[error("graph document must be a JSON object")]
    NotAnObject,
    #[error("unknown graph document format `{found}` (expected `{GRAPH_DOCUMENT_FORMAT}`)")]
    UnknownFormat { found: String },
    #[error("graph document is missing a numeric `schema_version`")]
    MissingSchemaVersion,
    #[error(
        "unsupported graph document schema_version {found} (this build supports up to {supported})"
    )]
    UnsupportedSchemaVersion { found: u64, supported: u32 },
    #[error("invalid graph document at `{path}`: {source}")]
    Invalid {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid plugin requirement #{index} (`{id}`): {message}")]
    InvalidRequirement {
        index: usize,
        id: String,
        message: String,
    },
    #[error("failed to serialize graph document: {0}")]
    Serialize(#[source] serde_json::Error),
}

/// A versioned graph document: the persisted form of a Daedalus graph.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphDocument {
    /// Schema version; [`GRAPH_DOCUMENT_SCHEMA_VERSION`] for newly created/upgraded documents.
    pub schema_version: u32,
    /// Plugins that must be loaded for this graph to plan.
    pub requires: Vec<PluginRequirement>,
    /// Application metadata (editor state, titles, ...). Ordered for deterministic output.
    pub metadata: BTreeMap<String, JsonValue>,
    /// The graph itself.
    pub graph: Graph,
}

#[derive(Serialize)]
struct WireRef<'a> {
    format: &'static str,
    schema_version: u32,
    requires: &'a [PluginRequirement],
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    metadata: &'a BTreeMap<String, JsonValue>,
    graph: &'a Graph,
}

/// Owned wire form. `format` and `schema_version` stay untyped so [`Header::check`] reports them
/// with typed errors.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireOwned {
    #[serde(default)]
    format: Option<JsonValue>,
    #[serde(default)]
    schema_version: Option<JsonValue>,
    #[serde(default)]
    requires: Vec<PluginRequirement>,
    #[serde(default)]
    metadata: BTreeMap<String, JsonValue>,
    graph: Graph,
}

impl WireOwned {
    fn into_document(self) -> Result<GraphDocument, GraphDocumentError> {
        let schema_version = Header {
            format: self.format,
            schema_version: self.schema_version,
        }
        .check()?;
        let doc = GraphDocument {
            schema_version,
            requires: self.requires,
            metadata: self.metadata,
            graph: self.graph,
        };
        doc.validate()?;
        Ok(doc)
    }
}

/// The `format`/`schema_version` pair, read on its own first so it is checked before (and
/// independently of) the rest of the document, whatever the field order.
#[derive(Default)]
struct Header {
    format: Option<JsonValue>,
    schema_version: Option<JsonValue>,
}

impl Header {
    /// Validate the format marker and return the supported schema version.
    fn check(self) -> Result<u32, GraphDocumentError> {
        match self.format {
            Some(JsonValue::String(f)) if f == GRAPH_DOCUMENT_FORMAT => {}
            Some(JsonValue::String(found)) => {
                return Err(GraphDocumentError::UnknownFormat { found });
            }
            other => {
                return Err(GraphDocumentError::UnknownFormat {
                    found: other.map_or_else(|| "<missing>".into(), |v| v.to_string()),
                });
            }
        }
        let version = self
            .schema_version
            .as_ref()
            .and_then(JsonValue::as_u64)
            .ok_or(GraphDocumentError::MissingSchemaVersion)?;
        u32::try_from(version)
            .ok()
            .filter(|v| (1..=GRAPH_DOCUMENT_SCHEMA_VERSION).contains(v))
            .ok_or(GraphDocumentError::UnsupportedSchemaVersion {
                found: version,
                supported: GRAPH_DOCUMENT_SCHEMA_VERSION,
            })
    }
}

impl<'de> Deserialize<'de> for Header {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct HeaderVisitor;

        impl<'de> Visitor<'de> for HeaderVisitor {
            type Value = Header;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a graph document object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Header, A::Error> {
                let mut header = Header::default();
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "format" => header.format = Some(map.next_value()?),
                        "schema_version" => header.schema_version = Some(map.next_value()?),
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                Ok(header)
            }
        }

        deserializer.deserialize_map(HeaderVisitor)
    }
}

impl GraphDocument {
    /// Wrap a graph in a current-version document with no requirements.
    pub fn new(graph: Graph) -> Self {
        Self {
            schema_version: GRAPH_DOCUMENT_SCHEMA_VERSION,
            requires: Vec::new(),
            metadata: BTreeMap::new(),
            graph,
        }
    }

    /// Add a plugin requirement.
    pub fn require(mut self, requirement: PluginRequirement) -> Self {
        self.requires.push(requirement);
        self
    }

    /// Replace the plugin requirements.
    pub fn with_requires(mut self, requires: impl IntoIterator<Item = PluginRequirement>) -> Self {
        self.requires = requires.into_iter().collect();
        self
    }

    /// Set one metadata entry.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Consume the document, returning its graph.
    pub fn into_graph(self) -> Graph {
        self.graph
    }

    /// Parse a versioned document with typed errors.
    ///
    /// `format` and `schema_version` are checked first; any other problem, including an unknown
    /// field at any level, is reported as [`GraphDocumentError::Invalid`] with its JSON path.
    pub fn from_json(json: &str) -> Result<Self, GraphDocumentError> {
        serde_json::from_str::<Header>(json)
            .map_err(|err| {
                if err.is_data() {
                    GraphDocumentError::NotAnObject
                } else {
                    GraphDocumentError::Syntax(err)
                }
            })?
            .check()?;
        let mut de = serde_json::Deserializer::from_str(json);
        serde_path_to_error::deserialize::<_, WireOwned>(&mut de)
            .map_err(|err| GraphDocumentError::Invalid {
                path: err.path().to_string(),
                source: err.into_inner(),
            })?
            .into_document()
    }

    /// Check structural invariants (requirement syntax).
    pub fn validate(&self) -> Result<(), GraphDocumentError> {
        for (index, req) in self.requires.iter().enumerate() {
            req.validate()
                .map_err(|message| GraphDocumentError::InvalidRequirement {
                    index,
                    id: req.id.clone(),
                    message,
                })?;
        }
        Ok(())
    }

    /// Serialize as compact JSON (always in the versioned format).
    pub fn to_json(&self) -> Result<String, GraphDocumentError> {
        serde_json::to_string(&self.wire()).map_err(GraphDocumentError::Serialize)
    }

    /// Serialize as pretty JSON (always in the versioned format, deterministic key order).
    pub fn to_json_pretty(&self) -> Result<String, GraphDocumentError> {
        serde_json::to_string_pretty(&self.wire()).map_err(GraphDocumentError::Serialize)
    }

    /// Check `requires` against loaded plugins; see [`check_plugin_requirements`].
    pub fn check_requirements<'a, F>(&self, lookup: F) -> Result<(), MissingPlugins>
    where
        F: FnMut(&str) -> Option<Option<&'a str>>,
    {
        check_plugin_requirements(&self.requires, lookup)
    }

    fn wire(&self) -> WireRef<'_> {
        WireRef {
            format: GRAPH_DOCUMENT_FORMAT,
            schema_version: self.schema_version,
            requires: &self.requires,
            metadata: &self.metadata,
            graph: &self.graph,
        }
    }
}

impl Default for GraphDocument {
    fn default() -> Self {
        Self::new(Graph::default())
    }
}

impl From<Graph> for GraphDocument {
    fn from(graph: Graph) -> Self {
        Self::new(graph)
    }
}

impl core::str::FromStr for GraphDocument {
    type Err = GraphDocumentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_json(s)
    }
}

impl Serialize for GraphDocument {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.wire().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GraphDocument {
    /// Deserialize the versioned form (prefer [`GraphDocument::from_json`] for typed, path-aware
    /// errors).
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        WireOwned::deserialize(deserializer)?
            .into_document()
            .map_err(serde::de::Error::custom)
    }
}
