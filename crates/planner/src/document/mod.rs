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
//! [`GraphDocument::from_json`] also accepts the legacy forms (a bare `Graph`, or a
//! `{"graph": Graph}` envelope) and upgrades them to the current schema version.

mod requirements;

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
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

/// Which on-disk shape a document was parsed from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GraphDocumentSource {
    /// A versioned `{"format": "daedalus.graph", ...}` document.
    Versioned,
    /// A legacy bare `Graph` JSON object.
    LegacyGraph,
    /// A legacy `{"graph": Graph}` envelope.
    LegacyEnvelope,
}

/// Errors produced while reading or writing a [`GraphDocument`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GraphDocumentError {
    #[error("graph document is not valid JSON: {0}")]
    Syntax(#[source] serde_json::Error),
    #[error(
        "unrecognized graph document: expected a `daedalus.graph` document, a graph object with \
         `nodes`, or a `{{\"graph\": ...}}` envelope"
    )]
    UnrecognizedShape,
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

#[derive(Deserialize)]
struct WireOwned {
    format: String,
    schema_version: u32,
    #[serde(default)]
    requires: Vec<PluginRequirement>,
    #[serde(default)]
    metadata: BTreeMap<String, JsonValue>,
    graph: Graph,
}

#[derive(Deserialize)]
struct LegacyEnvelope {
    graph: Graph,
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

    /// Parse a document, accepting the versioned format and the legacy shapes.
    pub fn from_json(json: &str) -> Result<Self, GraphDocumentError> {
        Self::from_json_with_source(json).map(|(doc, _)| doc)
    }

    /// Like [`GraphDocument::from_json`], also reporting which shape was parsed.
    pub fn from_json_with_source(
        json: &str,
    ) -> Result<(Self, GraphDocumentSource), GraphDocumentError> {
        let probe: JsonValue = serde_json::from_str(json).map_err(GraphDocumentError::Syntax)?;
        let JsonValue::Object(obj) = &probe else {
            return Err(GraphDocumentError::UnrecognizedShape);
        };
        if obj.contains_key("format") || obj.contains_key("schema_version") {
            match obj.get("format") {
                Some(JsonValue::String(f)) if f == GRAPH_DOCUMENT_FORMAT => {}
                Some(other) => {
                    let found = match other {
                        JsonValue::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    return Err(GraphDocumentError::UnknownFormat { found });
                }
                None => {
                    return Err(GraphDocumentError::UnknownFormat {
                        found: "<missing>".into(),
                    });
                }
            }
            let version = obj
                .get("schema_version")
                .and_then(JsonValue::as_u64)
                .ok_or(GraphDocumentError::MissingSchemaVersion)?;
            if version == 0 || version > u64::from(GRAPH_DOCUMENT_SCHEMA_VERSION) {
                return Err(GraphDocumentError::UnsupportedSchemaVersion {
                    found: version,
                    supported: GRAPH_DOCUMENT_SCHEMA_VERSION,
                });
            }
            let wire: WireOwned = parse_with_path(json)?;
            let doc = Self {
                schema_version: wire.schema_version,
                requires: wire.requires,
                metadata: wire.metadata,
                graph: wire.graph,
            };
            doc.validate()?;
            return Ok((doc, GraphDocumentSource::Versioned));
        }
        if obj.contains_key("graph") {
            let envelope: LegacyEnvelope = parse_with_path(json)?;
            return Ok((
                Self::new(envelope.graph),
                GraphDocumentSource::LegacyEnvelope,
            ));
        }
        if obj.contains_key("nodes") {
            let graph: Graph = parse_with_path(json)?;
            return Ok((Self::new(graph), GraphDocumentSource::LegacyGraph));
        }
        Err(GraphDocumentError::UnrecognizedShape)
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

impl std::str::FromStr for GraphDocument {
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
    /// Strict deserialization of the versioned form only (use [`GraphDocument::from_json`] to
    /// also accept legacy shapes).
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let wire = WireOwned::deserialize(deserializer)?;
        if wire.format != GRAPH_DOCUMENT_FORMAT {
            return Err(D::Error::custom(format!(
                "unknown graph document format `{}`",
                wire.format
            )));
        }
        if wire.schema_version == 0 || wire.schema_version > GRAPH_DOCUMENT_SCHEMA_VERSION {
            return Err(D::Error::custom(format!(
                "unsupported graph document schema_version {}",
                wire.schema_version
            )));
        }
        let doc = Self {
            schema_version: wire.schema_version,
            requires: wire.requires,
            metadata: wire.metadata,
            graph: wire.graph,
        };
        doc.validate().map_err(D::Error::custom)?;
        Ok(doc)
    }
}

fn parse_with_path<T: DeserializeOwned>(json: &str) -> Result<T, GraphDocumentError> {
    let mut de = serde_json::Deserializer::from_str(json);
    serde_path_to_error::deserialize(&mut de).map_err(|err| {
        let path = err.path().to_string();
        GraphDocumentError::Invalid {
            path,
            source: err.into_inner(),
        }
    })
}
