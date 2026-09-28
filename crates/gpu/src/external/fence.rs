//! Acquire fences (Linux `sync_file`s) for external frames.
//!
//! A producer that finishes writing a buffer asynchronously (a GPU, an ISP, a V4L2 driver with
//! fences) signals a `sync_file`. [`ExternalFrameDescriptor::with_acquire_fence`] hands one to
//! the import, which waits for it before the GPU can touch the memory. Producers that only attach
//! implicit fences to the dmabuf itself can be bridged with [`export_dmabuf_fence`].
//!
//! The wait happens on the CPU (`poll(POLLIN)` with a timeout) inside the import call. wgpu-hal 29
//! offers no way to make a later wgpu submission wait on an imported Vulkan semaphore, so a
//! GPU-side wait cannot be expressed; see the wgpu backend's `dmabuf/vulkan.rs` for details.
//!
//! [`ExternalFrameDescriptor::with_acquire_fence`]: super::ExternalFrameDescriptor::with_acquire_fence

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::time::{Duration, Instant};

use super::ExternalImportError;

/// Default upper bound for the acquire-fence wait of an import.
pub const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(1);

/// Which implicit fences of a dmabuf [`export_dmabuf_fence`] collects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmabufAccess {
    /// The consumer only reads: wait for pending writes.
    Read,
    /// The consumer also writes: wait for pending reads and writes.
    ReadWrite,
}

/// `struct dma_buf_export_sync_file` from `linux/dma-buf.h`.
#[repr(C)]
struct DmaBufExportSyncFile {
    flags: u32,
    fd: i32,
}

const DMA_BUF_SYNC_READ: u32 = 1;
const DMA_BUF_SYNC_WRITE: u32 = 2;
/// `_IOWR('b', 2, struct dma_buf_export_sync_file)`.
const DMA_BUF_IOCTL_EXPORT_SYNC_FILE: u32 = 0xC008_6202;

/// Snapshot the implicit fences attached to a dmabuf as a `sync_file`
/// (`DMA_BUF_IOCTL_EXPORT_SYNC_FILE`, Linux 6.0+).
///
/// Use it for producers that synchronize through the dmabuf's reservation object instead of
/// handing out fences. The result is already signaled when nothing is pending.
pub fn export_dmabuf_fence(dmabuf: BorrowedFd<'_>, access: DmabufAccess) -> io::Result<OwnedFd> {
    let mut arg = DmaBufExportSyncFile {
        flags: match access {
            DmabufAccess::Read => DMA_BUF_SYNC_READ,
            DmabufAccess::ReadWrite => DMA_BUF_SYNC_READ | DMA_BUF_SYNC_WRITE,
        },
        fd: -1,
    };
    // SAFETY: valid fd and a correctly laid out argument the kernel fills in.
    let rc = unsafe {
        libc::ioctl(
            dmabuf.as_raw_fd(),
            DMA_BUF_IOCTL_EXPORT_SYNC_FILE as _,
            &mut arg,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: on success the kernel returned a fresh fd owned by the caller.
    Ok(unsafe { OwnedFd::from_raw_fd(arg.fd) })
}

/// Block until `fence` is signaled (readable), failing after `timeout`.
pub(crate) fn wait_sync_file(
    fence: BorrowedFd<'_>,
    timeout: Duration,
) -> Result<(), ExternalImportError> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut pfd = libc::pollfd {
            fd: fence.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        let millis = remaining
            .as_nanos()
            .div_ceil(1_000_000)
            .min(i32::MAX as u128) as i32;
        // SAFETY: one valid pollfd for the duration of the call.
        match unsafe { libc::poll(&mut pfd, 1, millis) } {
            0 => return Err(ExternalImportError::FenceTimeout { timeout }),
            n if n > 0 && pfd.revents & libc::POLLIN != 0 => return Ok(()),
            n if n > 0 => {
                return Err(ExternalImportError::invalid(format!(
                    "acquire fence polled as {:#x} without POLLIN; expected a sync_file",
                    pfd.revents
                )));
            }
            _ => {
                let err = io::Error::last_os_error();
                if err.kind() != io::ErrorKind::Interrupted {
                    return Err(ExternalImportError::invalid(format!(
                        "cannot poll acquire fence: {err}"
                    )));
                }
            }
        }
    }
}
