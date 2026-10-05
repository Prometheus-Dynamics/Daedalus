//! Shared helpers of the hardware tests: dma-heap buffers, validation scopes, backends.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::time::{Duration, Instant};

use crate::{AcquireFenceWait, GpuBackend, WgpuBackend};

const DMA_HEAP_IOCTL_ALLOC: u64 = 0xC018_4800; // _IOWR('H', 0, dma_heap_allocation_data)
const DMA_BUF_IOCTL_SYNC: u64 = 0x4008_6200; // _IOW('b', 0, dma_buf_sync)
const DMA_BUF_SYNC_RW: u64 = 3;
const DMA_BUF_SYNC_END: u64 = 4;

#[repr(C)]
struct DmaHeapAllocationData {
    len: u64,
    fd: u32,
    fd_flags: u32,
    heap_flags: u64,
}

/// A CPU-mapped dma-heap buffer.
pub(super) struct DmaBuf {
    pub fd: OwnedFd,
    ptr: *mut u8,
    len: usize,
}

impl DmaBuf {
    pub fn alloc(len: usize) -> std::io::Result<Self> {
        let heap =
            std::env::var("DAEDALUS_DMA_HEAP").unwrap_or_else(|_| "/dev/dma_heap/system".into());
        let heap = std::fs::File::open(&heap)?;
        let mut data = DmaHeapAllocationData {
            len: len as u64,
            fd: 0,
            fd_flags: (libc::O_RDWR | libc::O_CLOEXEC) as u32,
            heap_flags: 0,
        };
        // SAFETY: valid heap fd and a correctly laid out argument struct.
        let rc = unsafe { libc::ioctl(heap.as_raw_fd(), DMA_HEAP_IOCTL_ALLOC as _, &mut data) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: the kernel returned a fresh fd we now own.
        let fd = unsafe { OwnedFd::from_raw_fd(data.fd as i32) };
        // SAFETY: mapping `len` bytes of a dmabuf we own.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self {
            fd,
            ptr: ptr.cast(),
            len,
        })
    }

    /// CPU write bracketed by DMA_BUF_IOCTL_SYNC so caches are coherent for the GPU.
    pub fn write(&self, f: impl FnOnce(&mut [u8])) {
        let sync = |flags: u64| {
            let mut arg = flags;
            // SAFETY: valid dmabuf fd and u64 argument (struct dma_buf_sync).
            unsafe { libc::ioctl(self.fd.as_raw_fd(), DMA_BUF_IOCTL_SYNC as _, &mut arg) };
        };
        sync(DMA_BUF_SYNC_RW);
        // SAFETY: `ptr` maps `len` bytes for the lifetime of `self`.
        f(unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) });
        sync(DMA_BUF_SYNC_RW | DMA_BUF_SYNC_END);
    }
}

impl Drop for DmaBuf {
    fn drop(&mut self) {
        // SAFETY: unmapping the mapping created in `alloc`.
        unsafe { libc::munmap(self.ptr.cast(), self.len) };
    }
}

/// The test pattern of the 4-byte formats, in memory order.
pub(super) fn pixel(x: u32, y: u32) -> [u8; 4] {
    [x as u8, y as u8, (x ^ y) as u8, 0xff]
}

/// Run `f`, failing on any wgpu validation error it raises (the backend only logs those).
pub(super) fn validated<R>(backend: &WgpuBackend, f: impl FnOnce() -> R) -> R {
    use pollster::FutureExt;
    let scope = backend
        .device_queue()
        .0
        .push_error_scope(wgpu::ErrorFilter::Validation);
    let out = f();
    if let Some(err) = scope.pop().block_on() {
        panic!("wgpu validation error: {err}");
    }
    out
}

pub(super) fn device_poll(backend: &WgpuBackend) {
    let _ = backend.device_queue().0.poll(wgpu::PollType::Wait {
        submission_index: None,
        timeout: None,
    });
}

/// Print why (part of) a hardware test was skipped, or a measurement; visible with `--nocapture`.
#[allow(clippy::print_stderr)]
pub(super) fn note(message: String) {
    eprintln!("{message}");
}

pub(super) fn skip(reason: String) {
    note(format!("skipping: {reason}"));
}

/// Run hardware tests one at a time: they time GPU work (fence latencies, blocked submissions,
/// producer jobs) that other tests on the same GPU would skew. Concurrent driver setup is safe on
/// its own (`driver_lock`).
pub(super) fn exclusive() -> std::sync::MutexGuard<'static, ()> {
    static GPU: std::sync::Mutex<()> = std::sync::Mutex::new(());
    GPU.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The default backend, or `None` (with a skip note) when it cannot import dmabufs.
pub(super) fn import_backend() -> Option<WgpuBackend> {
    let backend = WgpuBackend::new().expect("wgpu backend");
    let support = backend.dmabuf_import_support();
    if !support.is_supported() {
        skip(format!("{:?}", support.reason()));
        return None;
    }
    Some(backend)
}

/// Spin on `done` until it holds, failing after `limit`; returns how long it took.
pub(super) fn wait_until(limit: Duration, what: &str, mut done: impl FnMut() -> bool) -> Duration {
    let start = Instant::now();
    while !done() {
        assert!(start.elapsed() < limit, "{what} within {limit:?}");
        std::thread::yield_now();
    }
    start.elapsed()
}

/// A consumer backend with a GPU-side fence wait, and whether its queue is independent of the
/// producer's GPU. lavapipe (CPU Vulkan) is: its work is not ordered behind the producer's job by
/// a shared hardware ring, so a missing wait shows up as stale pixels. Otherwise the consumer is a
/// second device on the producer's GPU, which only shows that the import does not block (one GPU
/// ring runs both devices' jobs in submission order anyway).
pub(super) fn consumer_backend() -> (WgpuBackend, bool) {
    use pollster::FutureExt;
    let gpu_wait = |backend: &WgpuBackend| {
        backend
            .dmabuf_import_support()
            .acquire_fence_wait()
            .is_some_and(|wait| wait != AcquireFenceWait::Cpu)
    };
    let adapter = {
        let _driver = crate::wgpu_backend::driver_lock();
        let mut desc = wgpu::InstanceDescriptor::new_without_display_handle();
        desc.backends = wgpu::Backends::VULKAN;
        wgpu::Instance::new(desc)
            .enumerate_adapters(wgpu::Backends::VULKAN)
            .block_on()
            .into_iter()
            .find(|adapter| adapter.get_info().device_type == wgpu::DeviceType::Cpu)
    };
    let lavapipe = adapter
        .and_then(|adapter| {
            WgpuBackend::with_adapter(adapter, Default::default())
                .block_on()
                .ok()
        })
        .filter(gpu_wait);
    match lavapipe {
        Some(backend) => (backend, true),
        None => {
            skip("no lavapipe consumer; a missing GPU wait would go unnoticed".into());
            (WgpuBackend::new().expect("wgpu backend"), false)
        }
    }
}
