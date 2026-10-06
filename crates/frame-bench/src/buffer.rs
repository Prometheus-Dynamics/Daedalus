//! Frame memory owned outside Daedalus: a `memfd` mapping, a dma-buf from `/dev/dma_heap`, or a
//! heap fallback.

use std::io;

/// Where a [`FrameBuffer`]'s bytes live.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FrameBacking {
    /// A dma-buf allocated from `/dev/dma_heap/<heap>` (`system` first, then `linux,cma`) and
    /// mapped; planes expose its file descriptor like a camera's capture buffer.
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

/// Owned frame memory: a CPU mapping, plus the dma-buf fd when it has one.
pub struct FrameBuffer {
    ptr: *mut u8,
    len: usize,
    fd: i32,
    backing: FrameBacking,
    /// Keeps a heap buffer alive (`ptr` points into it).
    _heap: Option<Box<[u8]>>,
}

// Safety: the buffer owns its mapping exclusively; it is only written while being created and
// only read afterwards (frames are shared immutably).
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
        let mut buffer = match backing {
            FrameBacking::DmaHeap => Self::dma_heap(len)?,
            FrameBacking::Memfd => Self::memfd(len)?,
            FrameBacking::Heap => {
                let mut heap = vec![0u8; len].into_boxed_slice();
                Self {
                    ptr: heap.as_mut_ptr(),
                    len,
                    fd: -1,
                    backing,
                    _heap: Some(heap),
                }
            }
        };
        buffer.write(fill);
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

    pub fn bytes(&self) -> &[u8] {
        // Safety: `ptr` maps `len` bytes for the buffer's lifetime.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    fn write(&mut self, fill: impl FnOnce(&mut [u8])) {
        self.sync(DMA_BUF_SYNC_START | DMA_BUF_SYNC_RW);
        // Safety: `ptr` maps `len` writable bytes and `&mut self` is exclusive.
        fill(unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) });
        self.sync(DMA_BUF_SYNC_END | DMA_BUF_SYNC_RW);
    }
}

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

    impl FrameBuffer {
        pub(super) fn memfd(len: usize) -> io::Result<Self> {
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
            Self::map(fd, len, FrameBacking::Memfd)
        }

        pub(super) fn dma_heap(len: usize) -> io::Result<Self> {
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
                return Self::map(data.fd as i32, len, FrameBacking::DmaHeap);
            }
            Err(last)
        }

        fn map(fd: i32, len: usize, backing: FrameBacking) -> io::Result<Self> {
            // Safety: mapping `len` bytes of `fd`, which is at least that large.
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    fd,
                    0,
                )
            };
            if ptr == libc::MAP_FAILED {
                let error = io::Error::last_os_error();
                // Safety: closing our own fd.
                unsafe { libc::close(fd) };
                return Err(error);
            }
            Ok(Self {
                ptr: ptr.cast(),
                len,
                fd,
                backing,
                _heap: None,
            })
        }

        /// Bracket CPU access to a dma-buf (`DMA_BUF_IOCTL_SYNC`); a no-op for other backings.
        pub(super) fn sync(&self, flags: u64) {
            if self.backing == FrameBacking::DmaHeap {
                let mut flags = flags;
                // Safety: the ioctl reads one `u64` of flags.
                unsafe { libc::ioctl(self.fd, DMA_BUF_IOCTL_SYNC as _, &mut flags) };
            }
        }
    }

    impl Drop for FrameBuffer {
        fn drop(&mut self) {
            if self.fd >= 0 {
                // Safety: unmapping and closing the mapping and fd this buffer owns.
                unsafe {
                    libc::munmap(self.ptr.cast(), self.len);
                    libc::close(self.fd);
                }
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
impl FrameBuffer {
    fn memfd(_len: usize) -> io::Result<Self> {
        Err(io::ErrorKind::Unsupported.into())
    }

    fn dma_heap(_len: usize) -> io::Result<Self> {
        Err(io::ErrorKind::Unsupported.into())
    }

    fn sync(&self, _flags: u64) {}
}
