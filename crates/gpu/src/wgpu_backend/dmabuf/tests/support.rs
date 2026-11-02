//! Shared helpers of the hardware tests: dma-heap buffers, validation scopes, backends.

use std::os::fd::AsFd;
use std::time::{Duration, Instant};

use crate::{
    AcquireFenceMode, AcquireFenceWait, DRM_FORMAT_MOD_LINEAR, DrmFourcc, ExternalFrameDescriptor,
    ExternalImportError, ExternalPlane, GpuBackend, WgpuBackend,
};

/// A CPU-mapped dma-heap buffer.
pub(super) struct DmaBuf {
    pub fd: lemnos_linux::dma::DmaBuf,
    /// Points into `_map`, which stays mapped for the life of `self`.
    ptr: *mut u8,
    len: usize,
    _map: lemnos_linux::dma::Mapping,
}

impl DmaBuf {
    pub fn alloc(len: usize) -> std::io::Result<Self> {
        let heap =
            std::env::var("DAEDALUS_DMA_HEAP").unwrap_or_else(|_| "/dev/dma_heap/system".into());
        let fd = lemnos_linux::dma::DmaHeap::open_path(heap)?.allocate(len)?;
        let mut map = fd.map()?;
        Ok(Self {
            ptr: map.as_mut_slice().as_mut_ptr(),
            fd,
            len,
            _map: map,
        })
    }

    /// CPU write bracketed by DMA_BUF_IOCTL_SYNC so caches are coherent for the GPU.
    pub fn write(&self, f: impl FnOnce(&mut [u8])) {
        use lemnos_linux::dma::Access;
        let _ = self.fd.begin_cpu_access(Access::ReadWrite);
        // SAFETY: `ptr` maps `len` bytes for the lifetime of `self`.
        f(unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) });
        let _ = self.fd.end_cpu_access(Access::ReadWrite);
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
/// its own (`wgpu_backend/driver.rs`).
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

/// [`import_backend`] for tests that fill a linear XRGB8888 dma-heap buffer on the CPU; `None`
/// (skipped) when the device cannot sample LINEAR XRGB8888, as v3dv (Raspberry Pi 5), whose
/// LINEAR images are render and transfer only.
pub(super) fn linear_import_backend() -> Option<WgpuBackend> {
    let backend = import_backend()?;
    let buf = DmaBuf::alloc(256 * 4).expect("dma-heap alloc");
    let plane = ExternalPlane::from_borrowed(buf.fd.as_fd(), 0, 256).unwrap();
    let desc = ExternalFrameDescriptor::single_plane(64, 4, DrmFourcc::XRGB8888, plane)
        .with_modifier(DRM_FORMAT_MOD_LINEAR);
    match backend.import_dmabuf(desc) {
        Err(ExternalImportError::UnsupportedFormat { reason, .. }) => {
            skip(format!("LINEAR XRGB8888: {reason}"));
            None
        }
        _ => Some(backend),
    }
}

/// Make `wait` the backend's default through the mode that selects it; `false` when the device
/// lacks it.
pub(super) fn use_fence_wait(backend: &mut WgpuBackend, wait: AcquireFenceWait) -> bool {
    backend.set_acquire_fence_mode(match wait {
        AcquireFenceWait::SyncFd => AcquireFenceMode::SyncFd,
        AcquireFenceWait::Timeline => AcquireFenceMode::Timeline,
        AcquireFenceWait::Cpu => AcquireFenceMode::Cpu,
    });
    backend.dmabuf_import_support().acquire_fence_wait() == Some(wait)
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
            .fence_waits()
            .is_some_and(|waits| waits.sync_fd || waits.timeline)
    };
    let adapter = {
        let _driver = crate::wgpu_backend::driver_lock();
        let desc = crate::wgpu_backend::instance_descriptor(wgpu::Backends::VULKAN);
        wgpu::Instance::new(desc)
            .enumerate_adapters(wgpu::Backends::VULKAN)
            .block_on()
            .into_iter()
            .find(|adapter| adapter.get_info().device_type == wgpu::DeviceType::Cpu)
    };
    let lavapipe = adapter
        .and_then(|adapter| {
            WgpuBackend::with_adapter(adapter, Default::default(), Default::default())
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
