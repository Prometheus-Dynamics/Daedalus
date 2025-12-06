//! GPU integration shims (feature-gated).
//!
//! Concrete GPU backend types must live in `daedalus-gpu`. Here we only define
//! handle wrappers and memory location hints for GPU-aware values, plus a
//! minimal trait surface the rest of the system can rely on without pulling in
//! backend-specific types.

use serde::{Deserialize, Serialize};

pub use crate::descriptor::MemoryLocation;
pub use daedalus_gpu::GpuBackendKind;

/// Opaque handle to a GPU-backed value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuValueHandle {
    pub id: String,
    pub backend: GpuBackendKind,
    pub location: MemoryLocation,
    pub kind: Option<String>,
    pub format: Option<String>,
    pub adapter_label: Option<String>,
}

impl GpuValueHandle {
    pub fn new(id: impl Into<String>, backend: GpuBackendKind) -> Self {
        Self {
            id: id.into(),
            backend,
            location: MemoryLocation::Device,
            kind: None,
            format: None,
            adapter_label: None,
        }
    }

    pub fn with_location(mut self, location: MemoryLocation) -> Self {
        self.location = location;
        self
    }

    pub fn with_kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }

    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }

    pub fn with_adapter_label(mut self, label: impl Into<String>) -> Self {
        self.adapter_label = Some(label.into());
        self
    }
}

/// Types that can expose a GPU handle for planner/runtime hand-off.
pub trait AsGpuHandle {
    fn as_gpu_handle(&self) -> &GpuValueHandle;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_builder_sets_location() {
        let handle = GpuValueHandle::new("h1", GpuBackendKind::Noop)
            .with_location(MemoryLocation::Shared)
            .with_kind("image")
            .with_format("rgba8unorm")
            .with_adapter_label("mock");
        assert_eq!(handle.location, MemoryLocation::Shared);
        assert_eq!(handle.backend, GpuBackendKind::Noop);
        assert_eq!(handle.kind.as_deref(), Some("image"));
        assert_eq!(handle.format.as_deref(), Some("rgba8unorm"));
        assert_eq!(handle.adapter_label.as_deref(), Some("mock"));
    }
}
