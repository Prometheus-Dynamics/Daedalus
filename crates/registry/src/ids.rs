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
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn try_new(id: impl Into<String>) -> Result<Self, IdValidationError> {
        let id = Self::new(id);
        id.validate()?;
        Ok(id)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_validation_returns_typed_errors() {
        assert_eq!(NodeId::new("").validate(), Err(IdValidationError::Empty));
        assert_eq!(NodeId::try_new(""), Err(IdValidationError::Empty));
        assert_eq!(
            NodeId::new("Demo.Node").validate(),
            Err(IdValidationError::InvalidCharacters)
        );
        assert_eq!(
            NodeId::try_new("Demo.Node"),
            Err(IdValidationError::InvalidCharacters)
        );
        assert_eq!(NodeId::new("demo.node:ok_1").validate(), Ok(()));
        assert_eq!(
            NodeId::try_new("demo.node:ok_1"),
            Ok(NodeId::new("demo.node:ok_1"))
        );
    }
}
