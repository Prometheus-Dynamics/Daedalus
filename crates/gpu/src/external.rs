//! Import of externally owned frame memory (Linux dmabuf) into GPU image handles.
//!
//! The types here are backend-agnostic and dependency-light: a frame source (camera stack,
//! decoder, compositor) describes its buffer with an [`ExternalFrameDescriptor`] and hands it to
//! [`GpuBackend::import_dmabuf`](crate::GpuBackend::import_dmabuf) (or
//! [`GpuContextHandle::import_dmabuf`](crate::GpuContextHandle::import_dmabuf)). Backends that
//! can alias the memory without a CPU copy return a regular [`GpuImageHandle`](crate::GpuImageHandle);
//! the others return [`ExternalImportError::Unsupported`] with a reason.
//!
//! # Ownership and lifetime
//!
//! - Every plane carries an [`OwnedFd`](std::os::fd::OwnedFd). The descriptor is consumed by the
//!   import, so ownership of the file descriptors moves into the backend. Use
//!   [`ExternalPlane::from_borrowed`] to `dup` a descriptor the frame source keeps owning.
//! - The dmabuf itself is reference counted by the kernel, so the imported memory stays valid for
//!   as long as the GPU image exists. That does *not* stop the producer from recycling the buffer
//!   (a camera re-queues it and overwrites it). Attach an [`ExternalKeepalive`] (for example the
//!   camera request or buffer lease) with [`ExternalFrameDescriptor::with_keepalive`]; the backend
//!   drops it only once the GPU image is destroyed and no submitted GPU work still uses it.
//! - Holding imported images therefore holds producer buffers. Drop handles promptly: a camera
//!   with four buffers stalls once four frames are held.
//!
//! # Synchronization
//!
//! If the producer may still be writing when it hands the buffer over, pass its `sync_file` with
//! [`ExternalFrameDescriptor::with_acquire_fence`] (or [`ExternalFrameDescriptor::with_implicit_fence`]
//! for producers that only fence the dmabuf itself). The import waits for it on the CPU, bounded by
//! [`ExternalFrameDescriptor::acquire_timeout`], before the GPU can touch the memory.

use std::fmt;
use std::time::Duration;

use crate::{GpuBackendKind, GpuError, GpuFormat};

/// DRM format modifier for plain row-major (linear) layouts.
pub const DRM_FORMAT_MOD_LINEAR: u64 = 0;
/// Reserved DRM modifier meaning "no valid modifier"; rejected by imports.
pub const DRM_FORMAT_MOD_INVALID: u64 = 0x00ff_ffff_ffff_ffff;

/// DRM fourcc pixel format code (`drm_fourcc.h`), little-endian packed.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct DrmFourcc(pub u32);

impl DrmFourcc {
    /// Build a fourcc from its four ASCII characters, e.g. `DrmFourcc::from_bytes(*b"XR24")`.
    pub const fn from_bytes(code: [u8; 4]) -> Self {
        Self(u32::from_le_bytes(code))
    }

    /// Single 8-bit channel (`DRM_FORMAT_R8`, "R8  ") — also a luma (Y) plane.
    pub const R8: Self = Self::from_bytes(*b"R8  ");
    /// Two 8-bit channels, R in the low byte (`DRM_FORMAT_GR88`) — also an interleaved NV12 UV plane.
    pub const GR88: Self = Self::from_bytes(*b"GR88");
    /// 32-bit B,G,R,X in memory order (`DRM_FORMAT_XRGB8888`); alpha is undefined.
    pub const XRGB8888: Self = Self::from_bytes(*b"XR24");
    /// 32-bit B,G,R,A in memory order (`DRM_FORMAT_ARGB8888`).
    pub const ARGB8888: Self = Self::from_bytes(*b"AR24");
    /// 32-bit R,G,B,X in memory order (`DRM_FORMAT_XBGR8888`); alpha is undefined.
    pub const XBGR8888: Self = Self::from_bytes(*b"XB24");
    /// 32-bit R,G,B,A in memory order (`DRM_FORMAT_ABGR8888`).
    pub const ABGR8888: Self = Self::from_bytes(*b"AB24");
    /// Two-plane 4:2:0 YUV (Y, then interleaved UV); imported as one [`GpuFormat::Nv12`] image
    /// where the device supports it.
    pub const NV12: Self = Self::from_bytes(*b"NV12");
    /// Three-plane 4:2:0 YUV. Not importable as one image; import each plane separately.
    pub const YUV420: Self = Self::from_bytes(*b"YU12");

    /// The four ASCII characters of the code.
    pub const fn to_bytes(self) -> [u8; 4] {
        self.0.to_le_bytes()
    }

    /// GPU image format the fourcc imports as, if it is importable as one image.
    pub fn gpu_format(self) -> Option<GpuFormat> {
        match self {
            Self::R8 => Some(GpuFormat::R8Unorm),
            Self::GR88 => Some(GpuFormat::Rg8Unorm),
            Self::XBGR8888 | Self::ABGR8888 => Some(GpuFormat::Rgba8Unorm),
            Self::XRGB8888 | Self::ARGB8888 => Some(GpuFormat::Bgra8Unorm),
            Self::NV12 => Some(GpuFormat::Nv12),
            _ => None,
        }
    }

    /// Whether the fourcc describes a multi-planar YUV layout.
    pub fn is_multi_planar(self) -> bool {
        matches!(self, Self::NV12 | Self::YUV420)
    }
}

impl fmt::Display for DrmFourcc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.to_bytes();
        if bytes.iter().all(|b| b.is_ascii_graphic() || *b == b' ') {
            write!(f, "{}", String::from_utf8_lossy(&bytes).trim_end())
        } else {
            write!(f, "0x{:08x}", self.0)
        }
    }
}

impl fmt::Debug for DrmFourcc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DrmFourcc({self})")
    }
}

/// Opaque guard kept alive until the imported GPU image is destroyed and idle.
pub type ExternalKeepalive = std::sync::Arc<dyn std::any::Any + Send + Sync>;

/// Whether a backend can import dmabuf frames, and why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalImportSupport {
    Supported,
    Unsupported { reason: String },
}

impl ExternalImportSupport {
    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self::Unsupported {
            reason: reason.into(),
        }
    }

    pub fn is_supported(&self) -> bool {
        matches!(self, Self::Supported)
    }

    /// Why import is unavailable, or `None` when supported.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Supported => None,
            Self::Unsupported { reason } => Some(reason),
        }
    }

    /// Default answer for backends that never import external memory.
    pub(crate) fn backend_cannot_import(kind: GpuBackendKind) -> Self {
        Self::unsupported(format!(
            "the {} backend cannot import external memory",
            kind.as_str()
        ))
    }
}

/// Typed failure of an external-memory import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalImportError {
    /// Backend, platform, build, or device cannot import this kind of memory at all.
    Unsupported { reason: String },
    /// The descriptor is malformed (zero size, bad stride, buffer too small, bad fence, ...).
    InvalidDescriptor { reason: String },
    /// The device cannot import this format/modifier combination.
    UnsupportedFormat {
        fourcc: DrmFourcc,
        modifier: Option<u64>,
        reason: String,
    },
    /// The acquire fence did not signal within the descriptor's timeout.
    FenceTimeout { timeout: Duration },
    /// The driver rejected the import.
    ImportFailed { reason: String },
}

impl ExternalImportError {
    pub(crate) fn invalid(reason: impl Into<String>) -> Self {
        Self::InvalidDescriptor {
            reason: reason.into(),
        }
    }
}

impl fmt::Display for ExternalImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { reason } => write!(f, "external import unsupported: {reason}"),
            Self::InvalidDescriptor { reason } => {
                write!(f, "invalid external frame descriptor: {reason}")
            }
            Self::UnsupportedFormat {
                fourcc,
                modifier,
                reason,
            } => match modifier {
                Some(m) => write!(
                    f,
                    "unsupported format {fourcc} (modifier 0x{m:x}): {reason}"
                ),
                None => write!(f, "unsupported format {fourcc}: {reason}"),
            },
            Self::FenceTimeout { timeout } => {
                write!(f, "acquire fence not signaled within {timeout:?}")
            }
            Self::ImportFailed { reason } => write!(f, "external import failed: {reason}"),
        }
    }
}

impl std::error::Error for ExternalImportError {}

impl From<ExternalImportError> for GpuError {
    fn from(err: ExternalImportError) -> Self {
        match err {
            ExternalImportError::Unsupported { .. }
            | ExternalImportError::UnsupportedFormat { .. } => GpuError::Unsupported,
            other => GpuError::Internal(other.to_string()),
        }
    }
}

#[cfg(target_os = "linux")]
#[path = "external/fence.rs"]
mod fence;
#[cfg(target_os = "linux")]
#[path = "external/linux.rs"]
mod linux;

#[cfg(target_os = "linux")]
pub use fence::{DEFAULT_ACQUIRE_TIMEOUT, DmabufAccess, export_dmabuf_fence};
#[cfg(target_os = "linux")]
pub use linux::*;

#[cfg(all(test, target_os = "linux"))]
#[path = "external/tests.rs"]
mod tests;
