//! A dma-buf: its size, its identity, a mapping of it, and its fences.

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::ptr::NonNull;

use crate::ident::Identity;
use crate::sync::SyncFence;
use crate::uapi::{
    BufSync, DMA_BUF_IOCTL_EXPORT_SYNC_FILE, DMA_BUF_IOCTL_IMPORT_SYNC_FILE, DMA_BUF_IOCTL_SYNC,
    DMA_BUF_SYNC_END, DMA_BUF_SYNC_READ, DMA_BUF_SYNC_WRITE, SyncFileArg,
};
use crate::{Error, check};

/// What the host does with a buffer's memory, or which of its fences is meant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Access {
    Read,
    Write,
    ReadWrite,
}

impl Access {
    const fn flags(self) -> u64 {
        match self {
            Access::Read => DMA_BUF_SYNC_READ,
            Access::Write => DMA_BUF_SYNC_WRITE,
            Access::ReadWrite => DMA_BUF_SYNC_READ | DMA_BUF_SYNC_WRITE,
        }
    }
}

/// A dma-buf this value owns.
#[derive(Debug)]
pub struct DmaBuf {
    fd: OwnedFd,
}

/// A dma-buf someone else owns.
#[derive(Clone, Copy, Debug)]
pub struct DmaBufRef<'a> {
    fd: BorrowedFd<'a>,
}

impl DmaBuf {
    pub fn from_fd(fd: OwnedFd) -> DmaBuf {
        DmaBuf { fd }
    }

    /// An anonymous file in memory of `len` bytes, which maps and shares like a dma-buf and
    /// needs no device. It has no fences and takes no CPU access brackets.
    pub fn memfd(name: &str, len: u64) -> Result<DmaBuf, Error> {
        let name = std::ffi::CString::new(name).map_err(|_| Error::Range)?;
        let len = libc::off_t::try_from(len).map_err(|_| Error::Range)?;
        if len == 0 {
            return Err(Error::Range);
        }
        // SAFETY: the name is a NUL-terminated string that lives through the call.
        let fd = check(unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) })?;
        // SAFETY: `memfd_create` just returned this fd, and nothing else owns it.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        // SAFETY: `ftruncate` on an fd this function owns touches no memory.
        check(unsafe { libc::ftruncate(fd.as_raw_fd(), len) })?;
        Ok(DmaBuf { fd })
    }

    pub fn into_fd(self) -> OwnedFd {
        self.fd
    }

    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    /// Everything a dma-buf can do is on the borrowed form.
    pub fn as_ref(&self) -> DmaBufRef<'_> {
        DmaBufRef {
            fd: self.fd.as_fd(),
        }
    }

    /// Another fd for the same buffer.
    pub fn try_clone(&self) -> Result<DmaBuf, Error> {
        self.fd
            .try_clone()
            .map(DmaBuf::from_fd)
            .map_err(|error| Error::Os(error.raw_os_error().unwrap_or(0)))
    }
}

impl<'a> DmaBufRef<'a> {
    pub fn from_fd(fd: BorrowedFd<'a>) -> DmaBufRef<'a> {
        DmaBufRef { fd }
    }

    pub fn as_fd(&self) -> BorrowedFd<'a> {
        self.fd
    }

    pub fn identity(&self) -> Result<Identity, Error> {
        Identity::of(self.fd)
    }

    /// The buffer's size in bytes. The fd's position is left where it was.
    pub fn len(&self) -> Result<u64, Error> {
        let fd = self.fd.as_raw_fd();
        let seek = |offset, whence| {
            // SAFETY: `lseek` on an open fd touches no memory.
            let at = unsafe { libc::lseek(fd, offset, whence) };
            if at < 0 { Err(Error::last()) } else { Ok(at) }
        };
        // A dma-buf only seeks to its start or its end, and its position is always the start.
        let was = seek(0, libc::SEEK_CUR).unwrap_or(0);
        let end = seek(0, libc::SEEK_END)?;
        seek(was, libc::SEEK_SET)?;
        Ok(end as u64)
    }

    pub fn is_empty(&self) -> Result<bool, Error> {
        self.len().map(|len| len == 0)
    }

    /// Maps the whole buffer for reading and writing.
    pub fn map(&self) -> Result<Mapping, Error> {
        self.map_as(libc::PROT_READ | libc::PROT_WRITE)
    }

    pub fn map_read_only(&self) -> Result<Mapping, Error> {
        self.map_as(libc::PROT_READ)
    }

    fn map_as(&self, protection: libc::c_int) -> Result<Mapping, Error> {
        let len = usize::try_from(self.len()?).map_err(|_| Error::Range)?;
        if len == 0 {
            return Err(Error::Range);
        }
        // SAFETY: a new shared mapping of an open fd, placed by the kernel. Nothing else has
        // the range, and `Mapping` unmaps it on drop.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                protection,
                libc::MAP_SHARED,
                self.fd.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(Error::last());
        }
        let ptr = NonNull::new(ptr.cast::<u8>()).ok_or(Error::Range)?;
        Ok(Mapping {
            ptr,
            len,
            writable: protection & libc::PROT_WRITE != 0,
            access: std::sync::Mutex::new(()),
            lent: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn sync(&self, flags: u64) -> Result<(), Error> {
        let mut request = BufSync { flags };
        loop {
            // SAFETY: the argument is the struct this ioctl reads, alive for the call.
            let result = check(unsafe {
                libc::ioctl(
                    self.fd.as_raw_fd(),
                    DMA_BUF_IOCTL_SYNC as _,
                    &raw mut request,
                )
            });
            match result {
                Ok(_) => return Ok(()),
                Err(Error::Os(libc::EINTR | libc::EAGAIN)) => {}
                Err(error) => return Err(error),
            }
        }
    }

    /// Call before the host touches the mapped memory. It waits for the device's work on the
    /// buffer and makes the memory current for the host.
    pub fn begin_cpu_access(&self, access: Access) -> Result<(), Error> {
        self.sync(access.flags())
    }

    /// Call when the host is done, with the same access. It hands the memory back to devices.
    pub fn end_cpu_access(&self, access: Access) -> Result<(), Error> {
        self.sync(access.flags() | DMA_BUF_SYNC_END)
    }

    /// A fence for the work now pending on the buffer: `Read` gives the writers a reader has to
    /// wait for, `Write` everyone a writer has to wait for. Needs Linux 6.0.
    pub fn export_fence(&self, access: Access) -> Result<SyncFence, Error> {
        let mut request = SyncFileArg {
            flags: access.flags() as u32,
            fd: -1,
        };
        // SAFETY: the argument is the struct this ioctl reads and writes, alive for the call.
        check(unsafe {
            libc::ioctl(
                self.fd.as_raw_fd(),
                DMA_BUF_IOCTL_EXPORT_SYNC_FILE as _,
                &raw mut request,
            )
        })?;
        // SAFETY: the kernel returned a new fd in the struct, and nothing else owns it.
        let fd = unsafe { OwnedFd::from_raw_fd(request.fd) };
        SyncFence::from_fd(fd)
    }

    /// Attaches `fence` to the buffer as pending work of kind `access`, so other users of the
    /// buffer wait for it.
    pub fn import_fence(&self, fence: &SyncFence, access: Access) -> Result<(), Error> {
        let mut request = SyncFileArg {
            flags: access.flags() as u32,
            fd: fence.as_fd().as_raw_fd(),
        };
        // SAFETY: the argument is the struct this ioctl reads, alive for the call, and the
        // fence's fd is open and a sync_file.
        check(unsafe {
            libc::ioctl(
                self.fd.as_raw_fd(),
                DMA_BUF_IOCTL_IMPORT_SYNC_FILE as _,
                &raw mut request,
            )
        })?;
        Ok(())
    }
}

/// A buffer's memory in this process, until dropped. The memory is shared with devices and other
/// processes, so it is read and written by copying, or lent out read-only with [`Mapping::lend`]
/// while this process's writes through it refuse.
#[derive(Debug)]
pub struct Mapping {
    ptr: NonNull<u8>,
    len: usize,
    writable: bool,
    /// Writes and new lends, one at a time.
    access: std::sync::Mutex<()>,
    /// How many [`Lent`]s are alive. This process writes nothing through the mapping while any is.
    lent: std::sync::atomic::AtomicUsize,
}

// SAFETY: the mapping is plain memory with no thread affinity, and every access through it is a
// bounds-checked copy or a read through a `Lent`. Racing writers can garble bytes, as with any
// shared memory, and can't break memory safety.
unsafe impl Send for Mapping {}
// SAFETY: as above.
unsafe impl Sync for Mapping {}

impl Mapping {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn range(&self, offset: u64, len: usize) -> Result<usize, Error> {
        let start = usize::try_from(offset).map_err(|_| Error::Range)?;
        match start.checked_add(len) {
            Some(end) if end <= self.len => Ok(start),
            _ => Err(Error::Range),
        }
    }

    /// Copies the buffer from `offset` into `out`.
    pub fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), Error> {
        let start = self.range(offset, out.len())?;
        // SAFETY: `start + out.len()` is inside the mapping, which is live and can't overlap a
        // Rust slice.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.ptr.as_ptr().add(start),
                out.as_mut_ptr(),
                out.len(),
            );
        }
        Ok(())
    }

    /// Copies `bytes` into the buffer at `offset`. A read-only mapping refuses.
    pub fn write(&self, offset: u64, bytes: &[u8]) -> Result<(), Error> {
        if !self.writable {
            return Err(Error::Os(libc::EACCES));
        }
        let start = self.range(offset, bytes.len())?;
        let _one = self
            .access
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.lent.load(std::sync::atomic::Ordering::Acquire) != 0 {
            return Err(Error::Lent);
        }
        // SAFETY: as in `read`, and the mapping was made writable.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.ptr.as_ptr().add(start),
                bytes.len(),
            );
        }
        Ok(())
    }
}

impl Mapping {
    /// The mapping's bytes in place, until the [`Lent`] drops. This process's writes through the
    /// mapping refuse while any is alive. The holder sees to it that no device writes the buffer
    /// meanwhile, and brackets the reads with [`DmaBufRef::begin_cpu_access`] and
    /// [`DmaBufRef::end_cpu_access`] as it would a copy.
    pub fn lend(self: &std::sync::Arc<Self>) -> Lent {
        let _one = self
            .access
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.lent.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Lent {
            mapping: std::sync::Arc::clone(self),
        }
    }
}

/// A [`Mapping`]'s bytes, read in place.
#[derive(Debug)]
pub struct Lent {
    mapping: std::sync::Arc<Mapping>,
}

impl std::ops::Deref for Lent {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        let map = &self.mapping;
        // This process writes nothing through the mapping while a `Lent` is alive (`write`
        // refuses). A device or another process writing meanwhile breaks `lend`'s terms, and even
        // then only changes bytes under the reader, as with `read`: every byte value is a `u8`.
        // SAFETY: the pointer and length are the live mapping's, which `mapping` keeps alive.
        unsafe { std::slice::from_raw_parts(map.ptr.as_ptr(), map.len) }
    }
}

impl Drop for Lent {
    fn drop(&mut self) {
        self.mapping
            .lent
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: the range is exactly what `mmap` returned, and nothing borrows from it.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}
