//! Payload-handle resolution shared by the worker language crates.
//!
//! Every worker language resolves a [`WirePayloadHandle`] the same way: prefer a mapped region
//! (a memory-mapped file or named shared memory) when the transport enables it and the handle
//! names one, otherwise fall back to an in-process buffer sized by `bytes_estimate`.

use std::collections::BTreeMap;

use thiserror::Error;

use super::WirePayloadHandle;

/// Handle metadata keys that locate a mapped payload region.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MappedPayloadKeys {
    pub location: &'static str,
    pub offset: &'static str,
    pub len: &'static str,
}

impl MappedPayloadKeys {
    /// A memory-mapped file: `mmap_path`, `mmap_offset`, `mmap_len`.
    pub const MMAP: Self = Self {
        location: "mmap_path",
        offset: "mmap_offset",
        len: "mmap_len",
    };
    /// Named shared memory: `shared_memory_name`, `shared_memory_offset`, `shared_memory_len`.
    pub const SHARED_MEMORY: Self = Self {
        location: "shared_memory_name",
        offset: "shared_memory_offset",
        len: "shared_memory_len",
    };
}

/// Language-neutral view of a resolved payload handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PayloadView {
    Mapped {
        location: String,
        offset: u64,
        len: u64,
    },
    Buffer {
        bytes_estimate: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedPayload<V> {
    pub id: String,
    pub type_key: String,
    pub access: String,
    pub view: V,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum PayloadResolveError {
    #[error("payload transport enables neither a mapped nor a buffer view")]
    UnsupportedTransport,
    #[error("payload handle `{0}` is missing `{1}` metadata")]
    MissingMetadata(String, &'static str),
}

impl WirePayloadHandle {
    /// Resolve this handle to a mapped region (when `mapped` is set and the handle names one) or
    /// to a buffer (when `buffer` is enabled), then convert the view with `into_view`.
    pub fn resolve_view<V>(
        &self,
        mapped: Option<MappedPayloadKeys>,
        buffer: bool,
        into_view: impl FnOnce(PayloadView) -> V,
    ) -> Result<ResolvedPayload<V>, PayloadResolveError> {
        let missing = |key| PayloadResolveError::MissingMetadata(self.id.clone(), key);
        let view = match mapped.and_then(|keys| Some((keys, self.metadata_str(keys.location)?))) {
            Some((keys, location)) => PayloadView::Mapped {
                location: location.to_owned(),
                offset: self.metadata_u64(keys.offset).unwrap_or(0),
                len: self
                    .metadata_u64(keys.len)
                    .or_else(|| self.metadata_u64("bytes_estimate"))
                    .ok_or_else(|| missing(keys.len))?,
            },
            None if buffer => PayloadView::Buffer {
                bytes_estimate: self
                    .metadata_u64("bytes_estimate")
                    .ok_or_else(|| missing("bytes_estimate"))?,
            },
            None => return Err(PayloadResolveError::UnsupportedTransport),
        };
        Ok(ResolvedPayload {
            id: self.id.clone(),
            type_key: self.type_key.to_string(),
            access: self.access.to_string(),
            view: into_view(view),
        })
    }

    fn metadata_str(&self, key: &str) -> Option<&str> {
        self.metadata.get(key).and_then(serde_json::Value::as_str)
    }

    fn metadata_u64(&self, key: &str) -> Option<u64> {
        self.metadata.get(key).and_then(serde_json::Value::as_u64)
    }
}

/// Backend options advertising which payload views a worker transport enables.
pub fn payload_transport_options(views: &[(&str, bool)]) -> BTreeMap<String, serde_json::Value> {
    let views = views
        .iter()
        .map(|(name, enabled)| ((*name).to_owned(), serde_json::Value::Bool(*enabled)))
        .collect::<serde_json::Map<_, _>>();
    BTreeMap::from([("payload_transport".into(), serde_json::Value::Object(views))])
}
