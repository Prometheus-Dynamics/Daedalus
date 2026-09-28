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

use std::fmt;

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
    /// Two-plane 4:2:0 YUV. Not importable as one image; import each plane separately.
    pub const NV12: Self = Self::from_bytes(*b"NV12");
    /// Three-plane 4:2:0 YUV. Not importable as one image; import each plane separately.
    pub const YUV420: Self = Self::from_bytes(*b"YU12");

    /// The four ASCII characters of the code.
    pub const fn to_bytes(self) -> [u8; 4] {
        self.0.to_le_bytes()
    }

    /// GPU image format the fourcc maps to, if it is importable as a single-plane image.
    pub fn gpu_format(self) -> Option<GpuFormat> {
        match self {
            Self::R8 => Some(GpuFormat::R8Unorm),
            Self::GR88 => Some(GpuFormat::Rg8Unorm),
            Self::XBGR8888 | Self::ABGR8888 => Some(GpuFormat::Rgba8Unorm),
            Self::XRGB8888 | Self::ARGB8888 => Some(GpuFormat::Bgra8Unorm),
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
    /// The descriptor is malformed (zero size, bad stride, buffer too small, ...).
    InvalidDescriptor { reason: String },
    /// The device cannot import this format/modifier combination.
    UnsupportedFormat {
        fourcc: DrmFourcc,
        modifier: Option<u64>,
        reason: String,
    },
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
pub use linux::*;

#[cfg(target_os = "linux")]
mod linux {
    use std::fmt;
    use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd};

    use super::{DRM_FORMAT_MOD_INVALID, DrmFourcc, ExternalImportError, ExternalKeepalive};
    use crate::{GpuFormat, GpuUsage, format_bytes_per_pixel};

    /// One plane of an external frame: a dmabuf fd plus the plane layout inside it.
    pub struct ExternalPlane {
        /// dmabuf file descriptor; ownership moves into the backend on import.
        pub fd: OwnedFd,
        /// Byte offset of the plane's first row inside the dmabuf.
        pub offset: u64,
        /// Bytes between the starts of consecutive rows.
        pub stride: u64,
    }

    impl ExternalPlane {
        pub fn new(fd: OwnedFd, offset: u64, stride: u64) -> Self {
            Self { fd, offset, stride }
        }

        /// Duplicate (`dup`) a descriptor the frame source keeps owning.
        pub fn from_borrowed(
            fd: BorrowedFd<'_>,
            offset: u64,
            stride: u64,
        ) -> std::io::Result<Self> {
            Ok(Self::new(fd.try_clone_to_owned()?, offset, stride))
        }
    }

    impl fmt::Debug for ExternalPlane {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ExternalPlane")
                .field("fd", &self.fd.as_raw_fd())
                .field("offset", &self.offset)
                .field("stride", &self.stride)
                .finish()
        }
    }

    /// Description of an externally owned frame to import without copying.
    pub struct ExternalFrameDescriptor {
        pub width: u32,
        pub height: u32,
        pub fourcc: DrmFourcc,
        /// DRM format modifier. `None` means the implicit layout, treated as
        /// [`DRM_FORMAT_MOD_LINEAR`](super::DRM_FORMAT_MOD_LINEAR) (what V4L2/libcamera buffers use).
        pub modifier: Option<u64>,
        pub planes: Vec<ExternalPlane>,
        /// Extra usages beyond the implied sampling + copy-source (`UPLOAD` = copy destination,
        /// `STORAGE`, `RENDER_TARGET` write into the producer's buffer).
        pub usage: GpuUsage,
        pub label: Option<String>,
        /// Kept alive until the GPU image is destroyed and idle; see the module docs.
        pub keepalive: Option<ExternalKeepalive>,
    }

    impl ExternalFrameDescriptor {
        pub fn new(width: u32, height: u32, fourcc: DrmFourcc, planes: Vec<ExternalPlane>) -> Self {
            Self {
                width,
                height,
                fourcc,
                modifier: None,
                planes,
                usage: GpuUsage::empty(),
                label: None,
                keepalive: None,
            }
        }

        /// Convenience for the common single-plane case.
        pub fn single_plane(
            width: u32,
            height: u32,
            fourcc: DrmFourcc,
            plane: ExternalPlane,
        ) -> Self {
            Self::new(width, height, fourcc, vec![plane])
        }

        pub fn with_modifier(mut self, modifier: u64) -> Self {
            self.modifier = Some(modifier);
            self
        }

        pub fn with_usage(mut self, usage: GpuUsage) -> Self {
            self.usage = usage;
            self
        }

        pub fn with_label(mut self, label: impl Into<String>) -> Self {
            self.label = Some(label.into());
            self
        }

        pub fn with_keepalive(mut self, keepalive: ExternalKeepalive) -> Self {
            self.keepalive = Some(keepalive);
            self
        }

        /// Backend-independent checks shared by every backend: non-zero extent, exactly one plane,
        /// a known single-plane fourcc, a valid modifier, and a stride that fits a row.
        pub fn validate(&self) -> Result<ValidatedLayout, ExternalImportError> {
            if self.width == 0 || self.height == 0 {
                return Err(ExternalImportError::invalid(format!(
                    "extent {}x{} must be non-zero",
                    self.width, self.height
                )));
            }
            if self.modifier == Some(DRM_FORMAT_MOD_INVALID) {
                return Err(ExternalImportError::invalid(
                    "DRM_FORMAT_MOD_INVALID is not an importable modifier",
                ));
            }
            let Some(format) = self.fourcc.gpu_format() else {
                let reason = if self.fourcc.is_multi_planar() {
                    "multi-planar YUV is not imported as one image; import each plane separately \
                     (Y as R8, interleaved UV as GR88) using its offset and stride"
                } else {
                    "no GPU format mapping for this fourcc"
                };
                return Err(ExternalImportError::UnsupportedFormat {
                    fourcc: self.fourcc,
                    modifier: self.modifier,
                    reason: reason.into(),
                });
            };
            if self.planes.len() != 1 {
                return Err(ExternalImportError::invalid(format!(
                    "{} expects exactly 1 plane, got {}",
                    self.fourcc,
                    self.planes.len()
                )));
            }
            let plane = &self.planes[0];
            let bpp = u64::from(format_bytes_per_pixel(format).unwrap_or(4));
            let row_bytes = u64::from(self.width) * bpp;
            if plane.stride < row_bytes {
                return Err(ExternalImportError::invalid(format!(
                    "stride {} is smaller than a {}-pixel row of {} ({} bytes)",
                    plane.stride, self.width, self.fourcc, row_bytes
                )));
            }
            let min_len = plane
                .stride
                .checked_mul(u64::from(self.height - 1))
                .and_then(|v| v.checked_add(row_bytes))
                .and_then(|v| v.checked_add(plane.offset))
                .ok_or_else(|| ExternalImportError::invalid("plane layout overflows u64"))?;
            Ok(ValidatedLayout {
                format,
                bytes_per_pixel: bpp as u32,
                min_len,
            })
        }
    }

    impl fmt::Debug for ExternalFrameDescriptor {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ExternalFrameDescriptor")
                .field("width", &self.width)
                .field("height", &self.height)
                .field("fourcc", &self.fourcc)
                .field("modifier", &self.modifier)
                .field("planes", &self.planes)
                .field("usage", &self.usage)
                .field("label", &self.label)
                .field("keepalive", &self.keepalive.is_some())
                .finish()
        }
    }

    /// Result of [`ExternalFrameDescriptor::validate`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ValidatedLayout {
        pub format: GpuFormat,
        pub bytes_per_pixel: u32,
        /// Minimum dmabuf length needed to hold the plane (offset + last row end).
        pub min_len: u64,
    }

    /// Drop token holding an import's fds and keepalive for as long as the handle lives (mock
    /// backend; the wgpu backend hands both to the Vulkan image instead).
    #[cfg(feature = "gpu-mock")]
    pub(crate) struct ExternalImageToken {
        pub(crate) fds: Vec<OwnedFd>,
        pub(crate) keepalive: Option<ExternalKeepalive>,
    }

    #[cfg(feature = "gpu-mock")]
    impl fmt::Debug for ExternalImageToken {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ExternalImageToken")
                .field("fds", &self.fds)
                .field("keepalive", &self.keepalive.is_some())
                .finish()
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
#[path = "external/tests.rs"]
mod tests;
