use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Stable graph/plugin-facing type identity.
///
/// `TypeKey` is the transport identity that survives manifests, plugins, and FFI boundaries.
/// Native Rust `TypeId` can still be used as a fast path, but it is not stable enough to be the
/// graph type identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TypeKey(Arc<str>);

impl TypeKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into().into())
    }

    pub fn opaque(key: impl Into<String>) -> Self {
        Self::new(key)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TypeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for TypeKey {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for TypeKey {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

/// Deterministic ABI/layout identity for types that may cross a Rust dynamic plugin boundary.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LayoutHash(Arc<str>);

impl LayoutHash {
    pub fn new(hash: impl Into<String>) -> Self {
        Self(hash.into().into())
    }

    pub fn for_type<T: 'static>() -> Self {
        Self::new(format!(
            "{}:{}:{}",
            std::any::type_name::<T>(),
            std::mem::size_of::<T>(),
            std::mem::align_of::<T>()
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LayoutHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for LayoutHash {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for LayoutHash {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

/// Layout identity or constraint for payloads with meaningful memory/device layout.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Layout(String);

impl Layout {
    pub fn new(layout: impl Into<String>) -> Self {
        Self(layout.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Layout {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Layout {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Source id recorded in payload lineage.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceId(String);

impl SourceId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for SourceId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for SourceId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Stable adapter identifier.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AdapterId(String);

impl AdapterId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AdapterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for AdapterId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for AdapterId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}
