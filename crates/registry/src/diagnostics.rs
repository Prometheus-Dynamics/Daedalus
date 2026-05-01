use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable error codes for registry operations.
///
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RegistryErrorCode {
    Conflict,
    MissingDependency,
    FeatureBlocked,
    ConverterError,
    AdapterError,
    BundleError,
    Internal,
}

/// Structured registry error with code + message.
///
#[derive(Clone, Debug, Error, Serialize, Deserialize, PartialEq, Eq)]
#[error("{code:?}: {message}")]
pub struct RegistryError {
    code: RegistryErrorCode,
    message: String,
    conflict_key: Option<String>,
    conflict_kind: Option<ConflictKind>,
    payload: Option<RegistryErrorCompute>,
}

/// Structured conflict kinds for diagnostics.
///
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ConflictKind {
    Value,
    Node,
    Group,
    Converter,
    Type,
    Adapter,
    Plugin,
    Serializer,
    Device,
}

/// Optional structured payload for diagnostics.
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryErrorCompute {
    Conflict { key: String, kind: ConflictKind },
    MissingDependency { key: String },
}

impl RegistryError {
    pub fn new(code: RegistryErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            conflict_key: None,
            conflict_kind: None,
            payload: None,
        }
    }

    pub fn code(&self) -> RegistryErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn conflict_key(&self) -> Option<&str> {
        self.conflict_key.as_deref()
    }

    pub fn with_conflict_key(mut self, key: impl Into<String>) -> Self {
        self.conflict_key = Some(key.into());
        self
    }

    pub fn conflict_kind(&self) -> Option<ConflictKind> {
        self.conflict_kind
    }

    pub fn with_conflict_kind(mut self, kind: ConflictKind) -> Self {
        self.conflict_kind = Some(kind);
        self
    }

    pub fn payload(&self) -> Option<&RegistryErrorCompute> {
        self.payload.as_ref()
    }

    pub fn with_payload(mut self, payload: RegistryErrorCompute) -> Self {
        self.payload = Some(payload);
        self
    }
}

/// Convenience alias for registry results.
pub type RegistryResult<T> = Result<T, RegistryError>;
