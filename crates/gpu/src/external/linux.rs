//! Linux frame descriptors: dmabuf planes, acquire fence, and backend-independent validation.

use std::fmt;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::time::Duration;

use super::fence::{DEFAULT_ACQUIRE_TIMEOUT, DmabufAccess, export_dmabuf_fence, wait_sync_file};
use super::{DRM_FORMAT_MOD_INVALID, DrmFourcc, ExternalImportError, ExternalKeepalive};
use crate::{GpuFormat, GpuUsage, format_planes};

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
    pub fn from_borrowed(fd: BorrowedFd<'_>, offset: u64, stride: u64) -> std::io::Result<Self> {
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
    /// One entry per format plane, in order (NV12: Y then interleaved UV). Planes may share one
    /// dmabuf (same or `dup`ed fd, different offsets) or live in separate dmabufs.
    pub planes: Vec<ExternalPlane>,
    /// Extra usages beyond the implied sampling + copy-source (`UPLOAD` = copy destination,
    /// `STORAGE`, `RENDER_TARGET` write into the producer's buffer). Multi-planar formats (NV12)
    /// are sample-only and take none.
    pub usage: GpuUsage,
    pub label: Option<String>,
    /// Kept alive until the GPU image is destroyed and idle; see the module docs.
    pub keepalive: Option<ExternalKeepalive>,
    /// `sync_file` signaled when the producer's writes are complete; the import waits for it.
    pub acquire_fence: Option<OwnedFd>,
    /// Upper bound for the acquire-fence wait ([`DEFAULT_ACQUIRE_TIMEOUT`] by default).
    pub acquire_timeout: Duration,
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
            acquire_fence: None,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
        }
    }

    /// Convenience for the common single-plane case.
    pub fn single_plane(width: u32, height: u32, fourcc: DrmFourcc, plane: ExternalPlane) -> Self {
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

    /// Wait for this `sync_file` (e.g. a V4L2/libcamera or GPU producer fence) before first use.
    pub fn with_acquire_fence(mut self, fence: OwnedFd) -> Self {
        self.acquire_fence = Some(fence);
        self
    }

    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Use the implicit fences of the first plane's dmabuf as the acquire fence (see
    /// [`export_dmabuf_fence`]); write usages also wait for pending readers.
    pub fn with_implicit_fence(self) -> std::io::Result<Self> {
        let plane = self.planes.first().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "descriptor has no planes")
        })?;
        let writes = GpuUsage::UPLOAD | GpuUsage::STORAGE | GpuUsage::RENDER_TARGET;
        let access = if self.usage.intersects(writes) {
            DmabufAccess::ReadWrite
        } else {
            DmabufAccess::Read
        };
        let fence = export_dmabuf_fence(plane.fd.as_fd(), access)?;
        Ok(self.with_acquire_fence(fence))
    }

    /// Block until the acquire fence (if any) is signaled, or fail with
    /// [`ExternalImportError::FenceTimeout`]. Backends call this before the GPU can use the memory.
    pub fn wait_acquire_fence(&self) -> Result<(), ExternalImportError> {
        match &self.acquire_fence {
            Some(fence) => wait_sync_file(fence.as_fd(), self.acquire_timeout),
            None => Ok(()),
        }
    }

    /// Backend-independent checks shared by every backend: non-zero extent (even for subsampled
    /// formats), one plane per format plane, a known fourcc, a valid modifier, and strides that
    /// fit a row.
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
                "three-plane YUV is not imported as one image; import each plane separately as R8 \
                 using its offset and stride"
            } else {
                "no GPU format mapping for this fourcc"
            };
            return Err(ExternalImportError::UnsupportedFormat {
                fourcc: self.fourcc,
                modifier: self.modifier,
                reason: reason.into(),
            });
        };
        let formats = format_planes(format);
        if formats.len() > 1 && !self.usage.is_empty() {
            // wgpu only samples multi-planar textures.
            return Err(ExternalImportError::UnsupportedFormat {
                fourcc: self.fourcc,
                modifier: self.modifier,
                reason: format!("{format:?} images are sample-only, not {:?}", self.usage),
            });
        }
        if self.planes.len() != formats.len() {
            return Err(ExternalImportError::invalid(format!(
                "{} expects exactly {} plane(s), got {}",
                self.fourcc,
                formats.len(),
                self.planes.len()
            )));
        }
        let mut min_len = Vec::with_capacity(formats.len());
        for (index, (plane_format, plane)) in formats.iter().zip(&self.planes).enumerate() {
            let sub = plane_format.subsampling;
            if !self.width.is_multiple_of(sub) || !self.height.is_multiple_of(sub) {
                return Err(ExternalImportError::invalid(format!(
                    "{} needs an extent divisible by {sub}, got {}x{}",
                    self.fourcc, self.width, self.height
                )));
            }
            let (width, height) = plane_format.extent(self.width, self.height);
            let row_bytes = u64::from(width) * u64::from(plane_format.bytes_per_texel);
            if plane.stride < row_bytes {
                return Err(ExternalImportError::invalid(format!(
                    "plane {index}: stride {} is smaller than a {width}-texel row of {} \
                     ({row_bytes} bytes)",
                    plane.stride, self.fourcc
                )));
            }
            let end = plane
                .stride
                .checked_mul(u64::from(height - 1))
                .and_then(|v| v.checked_add(row_bytes))
                .and_then(|v| v.checked_add(plane.offset))
                .ok_or_else(|| {
                    ExternalImportError::invalid(format!("plane {index}: layout overflows u64"))
                })?;
            min_len.push(end);
        }
        Ok(ValidatedLayout { format, min_len })
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
            .field(
                "acquire_fence",
                &self.acquire_fence.as_ref().map(AsRawFd::as_raw_fd),
            )
            .field("acquire_timeout", &self.acquire_timeout)
            .finish()
    }
}

/// Result of [`ExternalFrameDescriptor::validate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedLayout {
    pub format: GpuFormat,
    /// Minimum dmabuf length each plane needs (its offset + last row end), in plane order.
    pub min_len: Vec<u64>,
}

impl ValidatedLayout {
    /// Multi-planar images (NV12) are sample-only: no copies, storage, or render targets.
    pub fn is_multi_planar(&self) -> bool {
        self.min_len.len() > 1
    }
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
