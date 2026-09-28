//! Java payload transport options and payload-handle resolution.

use std::collections::BTreeMap;

use crate::core::{
    MappedPayloadKeys, PayloadResolveError, PayloadView, ResolvedPayload, WirePayloadHandle,
    payload_transport_options,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JavaPayloadTransport {
    pub direct_byte_buffer: bool,
    pub mmap: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JavaPayloadView {
    DirectByteBuffer { bytes_estimate: u64 },
    Mmap { path: String, offset: u64, len: u64 },
}

impl JavaPayloadTransport {
    pub fn direct_byte_buffer_and_mmap() -> Self {
        Self {
            direct_byte_buffer: true,
            mmap: true,
        }
    }

    pub fn backend_options(&self) -> BTreeMap<String, serde_json::Value> {
        payload_transport_options(&[
            ("direct_byte_buffer", self.direct_byte_buffer),
            ("mmap", self.mmap),
        ])
    }
}

pub fn resolve_java_payload_handle(
    handle: &WirePayloadHandle,
    transport: &JavaPayloadTransport,
) -> Result<ResolvedPayload<JavaPayloadView>, PayloadResolveError> {
    let mapped = transport.mmap.then_some(MappedPayloadKeys::MMAP);
    handle.resolve_view(mapped, transport.direct_byte_buffer, |view| match view {
        PayloadView::Mapped {
            location,
            offset,
            len,
        } => JavaPayloadView::Mmap {
            path: location,
            offset,
            len,
        },
        PayloadView::Buffer { bytes_estimate } => {
            JavaPayloadView::DirectByteBuffer { bytes_estimate }
        }
    })
}
