use serde::{Deserialize, Serialize};
use std::fmt;

/// ID for node registrations.
///
/// ```
/// use daedalus_registry::ids::NodeId;
/// let id = NodeId::namespaced("demo", "node");
/// assert_eq!(id.0, "demo.node");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn namespaced(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        let ns = namespace.into();
        let name = name.into();
        if ns.is_empty() {
            return Self(name);
        }
        Self(format!("{ns}.{name}"))
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.0.is_empty() {
            return Err("id must not be empty");
        }
        if !self.0.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-' | ':')
        }) {
            return Err("id must be lowercase/digit/._-:");
        }
        Ok(())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// ID for node-group registrations.
///
/// ```no_run
/// use daedalus_registry::ids::GroupId;
/// let id = GroupId::namespaced("demo", "group");
/// assert_eq!(id.0, "demo.group");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct GroupId(pub String);

impl GroupId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn namespaced(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        let ns = namespace.into();
        let name = name.into();
        if ns.is_empty() {
            return Self(name);
        }
        Self(format!("{ns}.{name}"))
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.0.is_empty() {
            return Err("id must not be empty");
        }
        if !self.0.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-' | ':')
        }) {
            return Err("id must be lowercase/digit/._-:");
        }
        Ok(())
    }
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
