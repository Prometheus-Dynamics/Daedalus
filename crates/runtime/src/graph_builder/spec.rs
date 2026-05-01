use crate::handles::PortHandle;
use daedalus_registry::ids::IdValidationError;

/// Convenience wrapper so callers can pre-prefix ids (e.g. via a plugin helper)
/// and pass them into `GraphBuilder::node_spec`.
#[derive(Clone, Debug)]
pub struct NodeSpec {
    pub id: String,
}

impl NodeSpec {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    pub fn prefixed(prefix: &str, id: &str) -> Self {
        Self {
            id: format!("{prefix}:{id}"),
        }
    }
}

/// Internal representation of a port reference, from either strings or handles.
#[derive(Clone, Debug)]
pub struct PortSpec {
    pub node: String,
    pub port: String,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum GraphBuildError {
    #[error("nested alias '{alias}' already in use")]
    DuplicateNestedAlias { alias: String },
    #[error("cannot connect nested graph '{from}' directly to nested graph '{to}'")]
    NestedToNested { from: String, to: String },
    #[error("node alias '{alias}' not found")]
    MissingNodeAlias { alias: String },
    #[error(
        "node alias '{alias}' has no declared {direction} port '{port}' (available: {available:?})"
    )]
    MissingNodePort {
        alias: String,
        node_id: String,
        direction: String,
        port: String,
        available: Vec<String>,
    },
    #[error("node id '{id}' not found in capability registry")]
    MissingNodeId { id: String },
    #[error("nested graph '{alias}' input '{port}' not found")]
    MissingNestedInput { alias: String, port: String },
    #[error("nested graph '{alias}' output '{port}' not found")]
    MissingNestedOutput { alias: String, port: String },
    #[error("nested graph missing host bridge")]
    MissingHostBridge,
    #[error("nested graph missing host bridge alias '{alias}'")]
    MissingHostBridgeAlias { alias: String },
    #[error("invalid node id '{id}': {source}")]
    InvalidNodeId {
        id: String,
        source: IdValidationError,
    },
}

/// Conversion into a graph port reference.
///
/// `&str` shorthand is intended for concise examples. It splits `node.port`, so release code with
/// user-controlled aliases or aliases that contain separators should prefer `PortHandle` or
/// explicit `(node, port)` tuples.
pub trait IntoPortSpec {
    fn into_spec(self) -> PortSpec;
}

impl IntoPortSpec for &str {
    fn into_spec(self) -> PortSpec {
        if let Some((node, port)) = self.rsplit_once('.') {
            return PortSpec {
                node: node.to_string(),
                port: port.to_string(),
            };
        }
        PortSpec {
            node: String::new(),
            port: self.to_string(),
        }
    }
}

impl IntoPortSpec for (&str, &str) {
    fn into_spec(self) -> PortSpec {
        PortSpec {
            node: self.0.to_string(),
            port: self.1.to_string(),
        }
    }
}

impl IntoPortSpec for (String, String) {
    fn into_spec(self) -> PortSpec {
        PortSpec {
            node: self.0,
            port: self.1,
        }
    }
}

impl IntoPortSpec for &PortHandle {
    fn into_spec(self) -> PortSpec {
        PortSpec {
            node: self.node_alias().to_string(),
            port: self.port().to_string(),
        }
    }
}
