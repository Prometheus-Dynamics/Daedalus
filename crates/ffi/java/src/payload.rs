//! Java payload transport options and payload-handle resolution.

use std::collections::BTreeMap;

use crate::core::WirePayloadHandle;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JavaPayloadTransport {
    pub direct_byte_buffer: bool,
    pub mmap: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JavaResolvedPayload {
    pub id: String,
    pub type_key: String,
    pub access: String,
    pub view: JavaPayloadView,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JavaPayloadView {
    DirectByteBuffer { bytes_estimate: u64 },
    Mmap { path: String, offset: u64, len: u64 },
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum JavaPayloadResolveError {
    #[error("java payload transport supports neither direct ByteBuffer nor mmap")]
    UnsupportedTransport,
    #[error("payload handle `{0}` is missing `{1}` metadata")]
    MissingMetadata(String, &'static str),
}

impl JavaPayloadTransport {
    pub fn direct_byte_buffer_and_mmap() -> Self {
        Self {
            direct_byte_buffer: true,
            mmap: true,
        }
    }

    pub fn backend_options(&self) -> BTreeMap<String, serde_json::Value> {
        BTreeMap::from([(
            "payload_transport".into(),
            serde_json::json!({
                "direct_byte_buffer": self.direct_byte_buffer,
                "mmap": self.mmap,
            }),
        )])
    }
}

pub fn resolve_java_payload_handle(
    handle: &WirePayloadHandle,
    transport: &JavaPayloadTransport,
) -> Result<JavaResolvedPayload, JavaPayloadResolveError> {
    let view = if transport.mmap {
        if let Some(path) = metadata_string(handle, "mmap_path") {
            Some(JavaPayloadView::Mmap {
                path,
                offset: metadata_u64(handle, "mmap_offset").unwrap_or(0),
                len: metadata_u64(handle, "mmap_len")
                    .or_else(|| metadata_u64(handle, "bytes_estimate"))
                    .ok_or_else(|| {
                        JavaPayloadResolveError::MissingMetadata(handle.id.clone(), "mmap_len")
                    })?,
            })
        } else {
            None
        }
    } else {
        None
    };
    let view = match view {
        Some(view) => view,
        None if transport.direct_byte_buffer => JavaPayloadView::DirectByteBuffer {
            bytes_estimate: metadata_u64(handle, "bytes_estimate").ok_or_else(|| {
                JavaPayloadResolveError::MissingMetadata(handle.id.clone(), "bytes_estimate")
            })?,
        },
        None => return Err(JavaPayloadResolveError::UnsupportedTransport),
    };
    Ok(JavaResolvedPayload {
        id: handle.id.clone(),
        type_key: handle.type_key.to_string(),
        access: handle.access.to_string(),
        view,
    })
}

fn metadata_string(handle: &WirePayloadHandle, key: &'static str) -> Option<String> {
    handle
        .metadata
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn metadata_u64(handle: &WirePayloadHandle, key: &'static str) -> Option<u64> {
    handle.metadata.get(key).and_then(serde_json::Value::as_u64)
}
