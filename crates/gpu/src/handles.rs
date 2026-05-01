use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{GpuFormat, GpuMemoryLocation, GpuUsage};

static NEXT_BUFFER_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_IMAGE_ID: AtomicU64 = AtomicU64::new(1);

/// Opaque buffer identifier.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GpuBufferId(pub u64);

/// Opaque image identifier.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GpuImageId(pub u64);

impl fmt::Display for GpuBufferId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "buf-{}", self.0)
    }
}

impl fmt::Display for GpuImageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "img-{}", self.0)
    }
}

fn next_buffer_id() -> GpuBufferId {
    GpuBufferId(NEXT_BUFFER_ID.fetch_add(1, Ordering::Relaxed))
}

fn next_image_id() -> GpuImageId {
    GpuImageId(NEXT_IMAGE_ID.fetch_add(1, Ordering::Relaxed))
}

pub(crate) trait GpuDropToken: Send + Sync + fmt::Debug {}

impl<T: Send + Sync + fmt::Debug> GpuDropToken for T {}

/// Opaque buffer handle (no backend types).
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuBufferHandle {
    pub id: GpuBufferId,
    pub size_bytes: u64,
    pub location: GpuMemoryLocation,
    pub usage: GpuUsage,
    pub label: Option<String>,
    #[serde(skip)]
    pub(crate) drop_token: Option<Arc<dyn GpuDropToken>>,
}

impl GpuBufferHandle {
    /// Create a new buffer handle.
    pub fn new(size_bytes: u64, location: GpuMemoryLocation, usage: GpuUsage) -> Self {
        Self {
            id: next_buffer_id(),
            size_bytes,
            location,
            usage,
            label: None,
            drop_token: None,
        }
    }

    /// Attach a label for diagnostics.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
}

impl Drop for GpuBufferHandle {
    fn drop(&mut self) {
        // This token is used for its Drop side-effects (backend resource cleanup).
        // Touch it so `-D dead-code` doesn't treat it as unused.
        let _ = self.drop_token.take();
    }
}

impl PartialEq for GpuBufferHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.size_bytes == other.size_bytes
            && self.location == other.location
            && self.usage == other.usage
            && self.label == other.label
    }
}

impl Eq for GpuBufferHandle {}

/// Opaque image/texture handle (no backend types).
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuImageHandle {
    pub id: GpuImageId,
    pub format: GpuFormat,
    pub width: u32,
    pub height: u32,
    pub location: GpuMemoryLocation,
    pub usage: GpuUsage,
    pub label: Option<String>,
    #[serde(skip)]
    pub(crate) drop_token: Option<Arc<dyn GpuDropToken>>,
}

impl GpuImageHandle {
    /// Create a new image handle.
    pub fn new(
        format: GpuFormat,
        width: u32,
        height: u32,
        location: GpuMemoryLocation,
        usage: GpuUsage,
    ) -> Self {
        Self {
            id: next_image_id(),
            format,
            width,
            height,
            location,
            usage,
            label: None,
            drop_token: None,
        }
    }

    /// Attach a label for diagnostics.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
}

impl Drop for GpuImageHandle {
    fn drop(&mut self) {
        // This token is used for its Drop side-effects (backend resource cleanup).
        // Touch it so `-D dead-code` doesn't treat it as unused.
        let _ = self.drop_token.take();
    }
}

impl PartialEq for GpuImageHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.format == other.format
            && self.width == other.width
            && self.height == other.height
            && self.location == other.location
            && self.usage == other.usage
            && self.label == other.label
    }
}

impl Eq for GpuImageHandle {}
