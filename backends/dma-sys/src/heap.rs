//! Allocating dma-bufs from a heap the kernel exposes.

use std::ffi::CString;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use crate::buf::DmaBuf;
use crate::uapi::{DMA_HEAP_IOCTL_ALLOC, HeapAllocation};
use crate::{Error, check};

/// An open dma-heap, such as `system`.
pub struct DmaHeap {
    fd: OwnedFd,
}

impl DmaHeap {
    /// Opens `/dev/dma_heap/<name>`.
    pub fn open(name: &str) -> Result<DmaHeap, Error> {
        DmaHeap::open_path(&Path::new("/dev/dma_heap").join(name))
    }

    pub fn open_path(path: &Path) -> Result<DmaHeap, Error> {
        let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| Error::Range)?;
        // SAFETY: the path is a NUL-terminated string that lives through the call.
        let fd = check(unsafe { libc::open(path.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) })?;
        // SAFETY: `open` just returned this fd, and nothing else owns it.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(DmaHeap { fd })
    }

    /// A new dma-buf of at least `len` bytes. The kernel rounds up to whole pages.
    pub fn allocate(&self, len: u64) -> Result<DmaBuf, Error> {
        if len == 0 {
            return Err(Error::Range);
        }
        let mut request = HeapAllocation {
            len,
            fd_flags: (libc::O_RDWR | libc::O_CLOEXEC) as u32,
            ..HeapAllocation::default()
        };
        // SAFETY: the argument is the struct this ioctl reads and writes, alive for the call.
        check(unsafe {
            libc::ioctl(
                self.fd.as_raw_fd(),
                DMA_HEAP_IOCTL_ALLOC as _,
                &raw mut request,
            )
        })?;
        // SAFETY: the kernel returned a new fd in the struct, and nothing else owns it.
        let fd = unsafe { OwnedFd::from_raw_fd(request.fd as i32) };
        Ok(DmaBuf::from_fd(fd))
    }
}
