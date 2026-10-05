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
//! for producers that only fence the dmabuf itself). How a pending fence is waited for is an
//! [`AcquireFenceMode`]: a backend default ([`GpuOptions::acquire_fence_mode`](crate::GpuOptions),
//! `WgpuBackend::set_acquire_fence_mode`) that one import can override with
//! [`ExternalFrameDescriptor::with_acquire_fence_mode`]. The mode resolves to one of the waits the
//! device has ([`AcquireFenceWaits`]); [`ExternalImportSupport::acquire_fence_wait`] reports the
//! one the backend default resolves to:
//!
//! - [`AcquireFenceWait::SyncFd`] (the default where available): the fence is imported as a
//!   binary Vulkan semaphore the GPU waits on; nothing blocks on kernel drivers, but no timeout
//!   applies.
//! - [`AcquireFenceWait::Timeline`] (opt in with [`AcquireFenceMode::Timeline`] for a hard
//!   timeout): the GPU waits on a timeline semaphore that a watcher thread signals once the fence
//!   signals or [`ExternalFrameDescriptor::acquire_timeout`] passes.
//!   [`GpuImageHandle::acquire_status`](crate::GpuImageHandle::acquire_status) reports
//!   [`AcquireStatus::TimedOut`] when the GPU went ahead without the producer. On Mesa drivers the
//!   *next* submission to the device blocks until the wait is released (see the variant docs).
//! - [`AcquireFenceWait::Cpu`]: the import blocks until the fence signals, bounded by
//!   [`ExternalFrameDescriptor::acquire_timeout`].
//!
//! # Handing the buffer back
//!
//! When the last handle of an image is dropped, the wgpu backend releases it to the foreign queue
//! family (`SHADER_READ_ONLY_OPTIMAL -> GENERAL`) after all GPU work using it, and drops the
//! keepalive once that release has executed.

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

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

/// Where an import waits for the descriptor's acquire fence (the wait an [`AcquireFenceMode`]
/// resolved to).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AcquireFenceWait {
    /// The `sync_file` is imported as a binary Vulkan semaphore the GPU waits on. On kernel drivers
    /// nothing blocks (the kernel orders the GPU work behind the fence; lavapipe still holds back
    /// the next submission), but no timeout applies: a fence that never signals stalls the queue.
    /// An fd that is not a `sync_file` cannot be imported and falls back (see [`AcquireFenceMode`]).
    SyncFd,
    /// The GPU waits on a per-device timeline semaphore that a watcher thread signals once the
    /// fence signals or the descriptor's `acquire_timeout` passes, whichever comes first, so a
    /// stuck producer delays the GPU by at most the timeout. The import does not wait for its own
    /// fence. On Mesa drivers (measured on RADV and lavapipe; v3dv shares Mesa's common submit
    /// code) a submission waiting for an unsignaled timeline value goes to a submit thread, and
    /// because wgpu chains submissions with binary semaphores the *next* submission to the device
    /// (another import, a dispatch, a readback) blocks its thread until the wait is released: the
    /// CPU wait moves from the import to the next submission, still bounded by the timeout. That
    /// chaining is also what the Khronos validation layer reports as
    /// `VUID-vkQueueSubmit-pWaitSemaphores-03238` while a wait is pending.
    Timeline,
    /// The import call blocks (`poll`) until the fence signals or the descriptor's
    /// `acquire_timeout` passes.
    Cpu,
}

impl AcquireFenceWait {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SyncFd => "sync_fd",
            Self::Timeline => "timeline",
            Self::Cpu => "cpu",
        }
    }

    /// Whether the descriptor's `acquire_timeout` bounds the wait.
    pub fn has_timeout(self) -> bool {
        !matches!(self, Self::SyncFd)
    }
}

/// How pending acquire fences are waited for: a backend default
/// ([`GpuOptions::acquire_fence_mode`](crate::GpuOptions)) or a per-import override
/// ([`ExternalFrameDescriptor::with_acquire_fence_mode`]).
///
/// An explicit mode the device does not have falls back to [`AcquireFenceWait::Cpu`], which
/// keeps the timeout. A fence fd that is not a `sync_file` (the `SyncFd` wait cannot import it)
/// goes to the timeline watcher in `Auto` and `Timeline` mode when the device has one, else to
/// the CPU wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcquireFenceMode {
    /// [`AcquireFenceWait::SyncFd`], else [`AcquireFenceWait::Timeline`], else
    /// [`AcquireFenceWait::Cpu`]: no thread blocks where the device can import `sync_file`s, at
    /// the price of no timeout.
    #[default]
    Auto,
    /// [`AcquireFenceWait::SyncFd`] or the CPU wait.
    SyncFd,
    /// [`AcquireFenceWait::Timeline`] or the CPU wait: the `acquire_timeout` is always enforced.
    /// An unbounded timeout (`Duration::MAX`) has nothing to enforce and takes the `SyncFd` wait
    /// where the device has it.
    Timeline,
    /// Always [`AcquireFenceWait::Cpu`].
    Cpu,
}

impl AcquireFenceMode {
    pub const ALL: [Self; 4] = [Self::Auto, Self::SyncFd, Self::Timeline, Self::Cpu];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::SyncFd => "sync_fd",
            Self::Timeline => "timeline",
            Self::Cpu => "cpu",
        }
    }
}

/// The GPU-side acquire-fence waits a device has; [`AcquireFenceWait::Cpu`] is always available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct AcquireFenceWaits {
    /// `VK_KHR_external_semaphore_fd` with importable `SYNC_FD` semaphores.
    pub sync_fd: bool,
    /// Vulkan 1.2 timeline semaphores.
    pub timeline: bool,
}

impl AcquireFenceWaits {
    /// Only the CPU wait (mock backend, devices without GPU-side waits).
    pub const CPU: Self = Self {
        sync_fd: false,
        timeline: false,
    };

    pub fn contains(self, wait: AcquireFenceWait) -> bool {
        match wait {
            AcquireFenceWait::SyncFd => self.sync_fd,
            AcquireFenceWait::Timeline => self.timeline,
            AcquireFenceWait::Cpu => true,
        }
    }

    /// The available waits, in [`AcquireFenceMode::Auto`] preference order.
    pub fn iter(self) -> impl Iterator<Item = AcquireFenceWait> {
        [
            AcquireFenceWait::SyncFd,
            AcquireFenceWait::Timeline,
            AcquireFenceWait::Cpu,
        ]
        .into_iter()
        .filter(move |&wait| self.contains(wait))
    }

    /// The wait `mode` resolves to on this device for a fence with `timeout`.
    pub fn resolve(self, mode: AcquireFenceMode, timeout: Duration) -> AcquireFenceWait {
        let or_cpu = |wait| {
            if self.contains(wait) {
                wait
            } else {
                AcquireFenceWait::Cpu
            }
        };
        match mode {
            AcquireFenceMode::Auto => self.iter().next().unwrap_or(AcquireFenceWait::Cpu),
            AcquireFenceMode::SyncFd => or_cpu(AcquireFenceWait::SyncFd),
            AcquireFenceMode::Timeline if timeout == Duration::MAX && self.sync_fd => {
                AcquireFenceWait::SyncFd
            }
            AcquireFenceMode::Timeline => or_cpu(AcquireFenceWait::Timeline),
            AcquireFenceMode::Cpu => AcquireFenceWait::Cpu,
        }
    }

    /// Where a fence goes under `mode` when it resolved to `SyncFd` but is not a `sync_file`.
    pub fn non_sync_file_fallback(self, mode: AcquireFenceMode) -> AcquireFenceWait {
        match mode {
            AcquireFenceMode::Auto | AcquireFenceMode::Timeline if self.timeline => {
                AcquireFenceWait::Timeline
            }
            _ => AcquireFenceWait::Cpu,
        }
    }
}

/// Acquire-fence state of an imported image, from
/// [`GpuImageHandle::acquire_status`](crate::GpuImageHandle::acquire_status).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AcquireStatus {
    /// Nothing left to wait for: no fence, an already signaled one, a completed CPU wait, or a GPU
    /// wait released because the fence signaled.
    Ready,
    /// The GPU still waits for the fence; work using the image is queued behind it.
    Pending,
    /// The fence did not signal within `acquire_timeout` (or reported an error instead of
    /// signaling) and the GPU went ahead without it: the image contents are undefined.
    TimedOut,
}

/// Whether a backend can import dmabuf frames (and how it waits for acquire fences), and why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalImportSupport {
    Supported {
        /// The wait the backend's default mode resolves to (for a bounded timeout).
        acquire_fence: AcquireFenceWait,
        /// The backend's default mode; imports may override it.
        acquire_fence_mode: AcquireFenceMode,
        /// Every wait the device has.
        fence_waits: AcquireFenceWaits,
    },
    Unsupported {
        reason: String,
    },
}

impl ExternalImportSupport {
    /// Import is supported, with `mode` as the default on a device with `waits`.
    pub fn supported(mode: AcquireFenceMode, waits: AcquireFenceWaits) -> Self {
        Self::Supported {
            acquire_fence: waits.resolve(mode, Duration::from_secs(1)),
            acquire_fence_mode: mode,
            fence_waits: waits,
        }
    }

    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self::Unsupported {
            reason: reason.into(),
        }
    }

    pub fn is_supported(&self) -> bool {
        matches!(self, Self::Supported { .. })
    }

    /// Why import is unavailable, or `None` when supported.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Supported { .. } => None,
            Self::Unsupported { reason } => Some(reason),
        }
    }

    /// Where acquire fences are waited for by default, or `None` when import is unsupported.
    pub fn acquire_fence_wait(&self) -> Option<AcquireFenceWait> {
        match self {
            Self::Supported { acquire_fence, .. } => Some(*acquire_fence),
            Self::Unsupported { .. } => None,
        }
    }

    /// The backend's default fence mode, or `None` when import is unsupported.
    pub fn acquire_fence_mode(&self) -> Option<AcquireFenceMode> {
        match self {
            Self::Supported {
                acquire_fence_mode, ..
            } => Some(*acquire_fence_mode),
            Self::Unsupported { .. } => None,
        }
    }

    /// The waits the device has, or `None` when import is unsupported.
    pub fn fence_waits(&self) -> Option<AcquireFenceWaits> {
        match self {
            Self::Supported { fence_waits, .. } => Some(*fence_waits),
            Self::Unsupported { .. } => None,
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

#[cfg(all(target_os = "linux", feature = "gpu-dmabuf"))]
pub(crate) use fence::sync_file_signaled;
#[cfg(target_os = "linux")]
pub use fence::{DEFAULT_ACQUIRE_TIMEOUT, DmabufAccess, export_dmabuf_fence};
#[cfg(target_os = "linux")]
pub use linux::*;

#[cfg(all(test, target_os = "linux"))]
#[path = "external/tests.rs"]
mod tests;
