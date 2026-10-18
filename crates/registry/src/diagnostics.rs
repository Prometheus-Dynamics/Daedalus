use alloc::string::String;
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
            payload: None,
        }
    }

    pub fn code(&self) -> RegistryErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// The key of the conflicting declaration, when this is a conflict (see [`Self::payload`]).
    pub fn conflict_key(&self) -> Option<&str> {
        match &self.payload {
            Some(RegistryErrorCompute::Conflict { key, .. }) => Some(key),
            _ => None,
        }
    }

    /// The kind of the conflicting declaration, when this is a conflict (see [`Self::payload`]).
    pub fn conflict_kind(&self) -> Option<ConflictKind> {
        match &self.payload {
            Some(RegistryErrorCompute::Conflict { kind, .. }) => Some(*kind),
            _ => None,
        }
    }

    /// The structured payload: the conflict or missing dependency this error reports. The
    /// conflict accessors read it, so the key and kind are stored once.
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
