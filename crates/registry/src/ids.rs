use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Structured validation error for registry identifiers.
#[derive(Clone, Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdValidationError {
    #[error("id must not be empty")]
    Empty,
    #[error("id must be lowercase/digit/._-:")]
    InvalidCharacters,
}

impl From<IdValidationError> for crate::diagnostics::RegistryError {
    fn from(error: IdValidationError) -> Self {
        crate::diagnostics::RegistryError::new(
            crate::diagnostics::RegistryErrorCode::Internal,
            error.to_string(),
        )
    }
}

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

    pub fn validate(&self) -> Result<(), IdValidationError> {
        if self.0.is_empty() {
            return Err(IdValidationError::Empty);
        }
        if !self.0.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-' | ':')
        }) {
            return Err(IdValidationError::InvalidCharacters);
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

    pub fn validate(&self) -> Result<(), IdValidationError> {
        if self.0.is_empty() {
            return Err(IdValidationError::Empty);
        }
        if !self.0.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-' | ':')
        }) {
            return Err(IdValidationError::InvalidCharacters);
        }
        Ok(())
    }
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_validation_returns_typed_errors() {
        assert_eq!(NodeId::new("").validate(), Err(IdValidationError::Empty));
        assert_eq!(
            NodeId::new("Demo.Node").validate(),
            Err(IdValidationError::InvalidCharacters)
        );
        assert_eq!(NodeId::new("demo.node:ok_1").validate(), Ok(()));
    }

    #[test]
    fn group_id_validation_returns_typed_errors() {
        assert_eq!(GroupId::new("").validate(), Err(IdValidationError::Empty));
        assert_eq!(
            GroupId::new("demo/group").validate(),
            Err(IdValidationError::InvalidCharacters)
        );
        assert_eq!(GroupId::new("demo.group:ok_1").validate(), Ok(()));
    }
}
