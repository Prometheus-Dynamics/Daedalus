//! Frame memory owned outside Daedalus: a `memfd`, a dma-buf from `/dev/dma_heap`, or a heap
//! fallback. fd-backed buffers are mapped lazily, on the first CPU access, like a camera stack
//! that hands dma-bufs to GPU importers without ever touching their bytes.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};

/// Where a [`FrameBuffer`]'s bytes live.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FrameBacking {
    /// A dma-buf allocated from `/dev/dma_heap/<heap>` (`system` first, then `linux,cma`);
    /// planes expose its file descriptor like a camera's capture buffer.
    DmaHeap,
    /// An anonymous `memfd` mapped shared, the way a driver ring buffer is.
    Memfd,
    /// Plain heap memory (non-Linux, or when the others are unavailable).
    Heap,
}

impl FrameBacking {
    pub fn as_str(self) -> &'static str {
        match self {
            FrameBacking::DmaHeap => "dma-heap",
            FrameBacking::Memfd => "memfd",
            FrameBacking::Heap => "heap",
        }
    }
}

/// Owned frame memory: an fd (mapped on first CPU access) or a heap buffer.
pub struct FrameBuffer {
    len: usize,
    fd: i32,
    backing: FrameBacking,
    heap: Option<Box<[u8]>>,
    /// Lazy shared read-only mapping of `fd` (address), kept until drop.
    mapping: OnceLock<usize>,
    maps: AtomicU64,
    accesses: AtomicU64,
    /// Open CPU accesses: the first syncs the dma-buf for the CPU, the last ends that.
    readers: Mutex<u32>,
}

// Safety: the mapping is only written while the buffer is created and read-only afterwards
// (frames are shared immutably); the lazy mapping is published through `OnceLock`.
unsafe impl Send for FrameBuffer {}
// Safety: as above, shared access is read-only.
unsafe impl Sync for FrameBuffer {}

impl FrameBuffer {
    /// A buffer of `len` bytes on `backing`, filled by `fill`.
    pub fn new(
        backing: FrameBacking,
        len: usize,
        fill: impl FnOnce(&mut [u8]),
    ) -> io::Result<Self> {
        let (fd, heap) = match backing {
            FrameBacking::DmaHeap => (dma_heap(len)?, None),
            FrameBacking::Memfd => (memfd(len)?, None),
            FrameBacking::Heap => (-1, Some(vec![0u8; len].into_boxed_slice())),
        };
        let mut buffer = Self {
            len,
            fd,
            backing,
            heap,
            mapping: OnceLock::new(),
            maps: AtomicU64::new(0),
            accesses: AtomicU64::new(0),
            readers: Mutex::new(0),
        };
        match &mut buffer.heap {
            Some(heap) => fill(heap),
            None => buffer.write(fill)?,
        }
        Ok(buffer)
    }

    /// The first backing of `DmaHeap`, `Memfd`, `Heap` that works here.
    pub fn best(len: usize, fill: impl Fn(&mut [u8])) -> Self {
        [FrameBacking::DmaHeap, FrameBacking::Memfd]
            .into_iter()
            .find_map(|backing| Self::new(backing, len, &fill).ok())
            .unwrap_or_else(|| {
                Self::new(FrameBacking::Heap, len, &fill).expect("heap frame buffer")
            })
    }

    pub fn backing(&self) -> FrameBacking {
        self.backing
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The dma-buf file descriptor (`DmaHeap` only).
    pub fn dmabuf_fd(&self) -> Option<i32> {
        (self.backing == FrameBacking::DmaHeap).then_some(self.fd)
    }

    /// `mmap` calls made for CPU access so far (at most one per fd-backed buffer, none for
    /// heap buffers): 0 proves no consumer read the bytes.
    pub fn map_count(&self) -> u64 {
        self.maps.load(Relaxed)
    }

    /// CPU accesses begun so far ([`Self::begin_cpu_access`]).
    pub fn cpu_access_count(&self) -> u64 {
        self.accesses.load(Relaxed)
    }

    /// Begin a CPU read: map on first use and sync a dma-buf for the CPU when no other access is
    /// open. Pair every `Some` with one [`Self::end_cpu_access`].
    pub fn begin_cpu_access(&self) -> Option<&[u8]> {
        let bytes = self.bytes()?;
        let mut readers = self.readers.lock().unwrap_or_else(|e| e.into_inner());
        if *readers == 0 {
            sync(self, DMA_BUF_SYNC_START | DMA_BUF_SYNC_READ);
        }
        *readers += 1;
        self.accesses.fetch_add(1, Relaxed);
        Some(bytes)
    }

    /// End a CPU read begun by [`Self::begin_cpu_access`].
    pub fn end_cpu_access(&self) {
        let mut readers = self.readers.lock().unwrap_or_else(|e| e.into_inner());
        *readers = readers.saturating_sub(1);
        if *readers == 0 {
            sync(self, DMA_BUF_SYNC_END | DMA_BUF_SYNC_READ);
        }
    }

    /// The bytes, mapping an fd-backed buffer on first use (`None` if that fails). Unsynced:
    /// dma-buf readers go through [`Self::begin_cpu_access`].
    pub fn bytes(&self) -> Option<&[u8]> {
        if let Some(heap) = &self.heap {
            return Some(heap);
        }
        let addr = *self.mapping.get_or_init(|| {
            self.maps.fetch_add(1, Relaxed);
            map(self.fd, self.len, false).map_or(0, |ptr| ptr as usize)
        });
        // Safety: a non-zero `addr` maps `len` bytes until the buffer drops.
        (addr != 0).then(|| unsafe { std::slice::from_raw_parts(addr as *const u8, self.len) })
    }

    /// Fill an fd-backed buffer through a temporary writable mapping.
    fn write(&mut self, fill: impl FnOnce(&mut [u8])) -> io::Result<()> {
        let ptr = map(self.fd, self.len, true)?;
        sync(self, DMA_BUF_SYNC_START | DMA_BUF_SYNC_RW);
        // Safety: `ptr` maps `len` writable bytes nobody else sees yet.
        fill(unsafe { std::slice::from_raw_parts_mut(ptr, self.len) });
        sync(self, DMA_BUF_SYNC_END | DMA_BUF_SYNC_RW);
        unmap(ptr, self.len);
        Ok(())
    }
}

const DMA_BUF_SYNC_READ: u64 = 1;
const DMA_BUF_SYNC_RW: u64 = 3;
const DMA_BUF_SYNC_START: u64 = 0;
const DMA_BUF_SYNC_END: u64 = 4;

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::ffi::CString;

    /// `_IOWR('H', 0, struct dma_heap_allocation_data)`.
    const DMA_HEAP_IOCTL_ALLOC: u64 = 0xC018_4800;
    /// `_IOW('b', 0, struct dma_buf_sync)`.
    const DMA_BUF_IOCTL_SYNC: u64 = 0x4008_6200;
    const HEAPS: [&str; 2] = ["/dev/dma_heap/system", "/dev/dma_heap/linux,cma"];

    #[repr(C)]
    struct DmaHeapAllocationData {
        len: u64,
        fd: u32,
        fd_flags: u32,
        heap_flags: u64,
    }

    pub(super) fn memfd(len: usize) -> io::Result<i32> {
        let name = CString::new("daedalus-frame").expect("static name");
        // Safety: plain syscalls on a name we own; failures are checked.
        let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // Safety: `fd` is the memfd created above.
        if unsafe { libc::ftruncate(fd, len as libc::off_t) } != 0 {
            let error = io::Error::last_os_error();
            // Safety: closing our own fd.
            unsafe { libc::close(fd) };
            return Err(error);
        }
        Ok(fd)
    }

    pub(super) fn dma_heap(len: usize) -> io::Result<i32> {
        let mut last = io::Error::from(io::ErrorKind::NotFound);
        for heap in HEAPS {
            let path = CString::new(heap).expect("static path");
            // Safety: opening a device node by path.
            let heap_fd = unsafe { libc::open(path.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
            if heap_fd < 0 {
                last = io::Error::last_os_error();
                continue;
            }
            let mut data = DmaHeapAllocationData {
                len: len as u64,
                fd: 0,
                fd_flags: (libc::O_RDWR | libc::O_CLOEXEC) as u32,
                heap_flags: 0,
            };
            // Safety: the ioctl reads and writes `data`, laid out as the kernel's struct.
            let result = unsafe { libc::ioctl(heap_fd, DMA_HEAP_IOCTL_ALLOC as _, &mut data) };
            let error = io::Error::last_os_error();
            // Safety: closing our own fd; the allocated dma-buf has its own.
            unsafe { libc::close(heap_fd) };
            if result < 0 {
                last = error;
                continue;
            }
            return Ok(data.fd as i32);
        }
        Err(last)
    }

    /// Map `len` bytes of `fd` shared (read-only unless `writable`).
    pub(super) fn map(fd: i32, len: usize, writable: bool) -> io::Result<*mut u8> {
        let prot = match writable {
            true => libc::PROT_READ | libc::PROT_WRITE,
            false => libc::PROT_READ,
        };
        // Safety: mapping `len` bytes of `fd`, which is at least that large.
        let ptr = unsafe { libc::mmap(std::ptr::null_mut(), len, prot, libc::MAP_SHARED, fd, 0) };
        match ptr == libc::MAP_FAILED {
            true => Err(io::Error::last_os_error()),
            false => Ok(ptr.cast()),
        }
    }

    pub(super) fn unmap(ptr: *mut u8, len: usize) {
        // Safety: `ptr` is a mapping of `len` bytes made by `map` and no longer used.
        unsafe { libc::munmap(ptr.cast(), len) };
    }

    /// Bracket CPU access to a dma-buf (`DMA_BUF_IOCTL_SYNC`); a no-op for other backings.
    pub(super) fn sync(buffer: &FrameBuffer, flags: u64) {
        if buffer.backing == FrameBacking::DmaHeap {
            let mut flags = flags;
            // Safety: the ioctl reads one `u64` of flags.
            unsafe { libc::ioctl(buffer.fd, DMA_BUF_IOCTL_SYNC as _, &mut flags) };
        }
    }

    impl Drop for FrameBuffer {
        fn drop(&mut self) {
            if let Some(&addr) = self.mapping.get()
                && addr != 0
            {
                unmap(addr as *mut u8, self.len);
            }
            if self.fd >= 0 {
                // Safety: closing the fd this buffer owns.
                unsafe { libc::close(self.fd) };
            }
        }
    }
}

#[cfg(target_os = "linux")]
use linux::{dma_heap, map, memfd, sync, unmap};

#[cfg(not(target_os = "linux"))]
mod other {
    use super::*;

    pub(super) fn memfd(_len: usize) -> io::Result<i32> {
        Err(io::ErrorKind::Unsupported.into())
    }

    pub(super) fn dma_heap(_len: usize) -> io::Result<i32> {
        Err(io::ErrorKind::Unsupported.into())
    }

    pub(super) fn map(_fd: i32, _len: usize, _writable: bool) -> io::Result<*mut u8> {
        Err(io::ErrorKind::Unsupported.into())
    }

    pub(super) fn unmap(_ptr: *mut u8, _len: usize) {}

    pub(super) fn sync(_buffer: &FrameBuffer, _flags: u64) {}
}

#[cfg(not(target_os = "linux"))]
use other::{dma_heap, map, memfd, sync, unmap};
