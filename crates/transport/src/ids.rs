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

    /// Runtime-local fallback layout identity for plain Rust values.
    ///
    /// This is intentionally tied to the concrete Rust type name plus basic ABI facts. Dynamic
    /// plugin boundaries that already have schema/field metadata should prefer
    /// [`Self::for_schema`] so same-name, same-size layout changes are still rejected.
    pub fn for_type<T: 'static>() -> Self {
        Self::new(format!(
            "rust-type-v1:{}:{}:{}",
            std::any::type_name::<T>(),
            std::mem::size_of::<T>(),
            std::mem::align_of::<T>()
        ))
    }

    /// Stable schema-derived layout identity for boundary-crossing payload contracts.
    pub fn for_schema<T: 'static>(schema: impl fmt::Display) -> Self {
        Self::new(format!(
            "rust-schema-v1:{}:{}:{}:{:016x}",
            std::any::type_name::<T>(),
            std::mem::size_of::<T>(),
            std::mem::align_of::<T>(),
            stable_hash64(schema.to_string().as_bytes())
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn stable_hash64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
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
