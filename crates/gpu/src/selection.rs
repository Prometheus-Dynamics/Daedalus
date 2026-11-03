use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::{
    BackendSkip, BackendSkipReason, GpuAdapterInfo, GpuBackend, GpuBackendKind, GpuContextHandle,
    GpuError, GpuOptions, NoopBackend,
};

#[cfg(feature = "gpu-mock")]
use crate::MockBackend;
#[cfg(feature = "gpu-wgpu")]
use crate::WgpuBackend;

fn backend_order(opts: &GpuOptions) -> Vec<GpuBackendKind> {
    let mut order = Vec::new();
    if let Some(pref) = opts.preferred_backend
        && !order.contains(&pref)
    {
        order.push(pref);
    }
    for fallback in [
        GpuBackendKind::Wgpu,
        GpuBackendKind::Mock,
        GpuBackendKind::Noop,
    ] {
        if !order.contains(&fallback) {
            order.push(fallback);
        }
    }
    order
}

/// This synchronous compatibility selector can block when the real wgpu backend is enabled because
/// it calls the synchronous wgpu constructor. Async applications should use
/// `select_backend_async` with the `gpu-async` feature.
/// Order: preferred backend (if set), then wgpu, mock, noop. CUDA is tried only when it is the
/// preferred backend, so the default order is unchanged.
pub fn select_backend(opts: &GpuOptions) -> Result<GpuContextHandle, GpuError> {
    let mut skipped = Vec::new();

    for kind in backend_order(opts) {
        match try_build_backend(kind, opts) {
            Ok((backend, adapter)) => {
                return Ok(GpuContextHandle {
                    chosen: kind,
                    adapter,
                    skipped,
                    backend,
                });
            }
            Err(reason) => skipped.push(BackendSkip {
                backend: kind,
                reason,
            }),
        }
    }

    Err(GpuError::AdapterUnavailable)
}

/// The process's GPU device: the one a previous call made, while any handle to it is alive, else
/// a new one from [`select_backend`] (a CUDA device is shared the same way as a wgpu one). Every
/// Daedalus engine with a device backend takes its context here, and so can any other library
/// in the process that runs Vulkan work (Styx's GPU ISP), so the process opens one device and one
/// queue, with one submit lock, instead of one per user: no duplicate pipelines and memory, no
/// cross-device sync.
///
/// The first call's `opts` choose the device; later calls get that device whatever their
/// options. A request for the mock or noop backend is not shared: it gets its own, as from
/// [`select_backend`]. The device is destroyed when the last handle to it drops, and the next call
/// opens a new one.
pub fn shared_context(opts: &GpuOptions) -> Result<GpuContextHandle, GpuError> {
    if matches!(
        opts.preferred_backend,
        Some(GpuBackendKind::Mock | GpuBackendKind::Noop)
    ) {
        return select_backend(opts);
    }
    // Held while a device is created, so two first callers cannot open two.
    let mut shared = SHARED.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(handle) = shared.as_ref().and_then(Shared::upgrade) {
        return Ok(handle);
    }
    let handle = select_backend(opts)?;
    *shared = matches!(handle.chosen, GpuBackendKind::Wgpu | GpuBackendKind::Cuda)
        .then(|| Shared::of(&handle));
    Ok(handle)
}

/// A wgpu context on the best adapter of PCI vendor `vendor` (`0x10DE` is NVIDIA), for code that
/// shares a device with another API, such as CUDA interop (see `GpuBackendKind::Cuda`). It is
/// not the shared device: each call opens its own.
#[cfg(feature = "gpu-wgpu")]
pub fn select_wgpu_on_vendor(vendor: u32) -> Result<GpuContextHandle, GpuError> {
    let backend = WgpuBackend::new_on_vendor(vendor)?;
    let adapter = crate::GpuBackend::adapter_info(&backend);
    Ok(GpuContextHandle {
        chosen: GpuBackendKind::Wgpu,
        adapter,
        skipped: Vec::new(),
        backend: Arc::new(backend),
    })
}

/// The process's GPU device if one is open (made by [`shared_context`] and still held), without
/// opening one.
pub fn process_context() -> Option<GpuContextHandle> {
    SHARED
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .and_then(Shared::upgrade)
}

/// The number of wgpu devices this copy of the crate has created in the process, through any
/// selector. A test asserts one device per process with it.
pub fn devices_created() -> usize {
    DEVICES_CREATED.load(Ordering::Relaxed)
}

pub(crate) static DEVICES_CREATED: AtomicUsize = AtomicUsize::new(0);

static SHARED: Mutex<Option<Shared>> = Mutex::new(None);

/// The shared device, held weakly so it goes with its last user.
struct Shared {
    backend: Weak<dyn GpuBackend>,
    chosen: GpuBackendKind,
    adapter: GpuAdapterInfo,
}

impl Shared {
    fn of(handle: &GpuContextHandle) -> Self {
        Self {
            backend: Arc::downgrade(&handle.backend),
            chosen: handle.chosen,
            adapter: handle.adapter.clone(),
        }
    }

    fn upgrade(&self) -> Option<GpuContextHandle> {
        Some(GpuContextHandle {
            backend: self.backend.upgrade()?,
            chosen: self.chosen,
            adapter: self.adapter.clone(),
            skipped: Vec::new(),
        })
    }
}

/// The real wgpu backend uses `WgpuBackend::new_async`; mock and noop backends remain immediate.
#[cfg(feature = "gpu-async")]
pub async fn select_backend_async(opts: &GpuOptions) -> Result<GpuContextHandle, GpuError> {
    let mut skipped = Vec::new();

    for kind in backend_order(opts) {
        match try_build_backend_async(kind, opts).await {
            Ok((backend, adapter)) => {
                return Ok(GpuContextHandle {
                    chosen: kind,
                    adapter,
                    skipped,
                    backend,
                });
            }
            Err(reason) => skipped.push(BackendSkip {
                backend: kind,
                reason,
            }),
        }
    }

    Err(GpuError::AdapterUnavailable)
}

fn try_build_backend(
    kind: GpuBackendKind,
    opts: &GpuOptions,
) -> Result<(Arc<dyn GpuBackend>, GpuAdapterInfo), BackendSkipReason> {
    match kind {
        GpuBackendKind::Wgpu => {
            #[cfg(feature = "gpu-wgpu")]
            {
                let mut backend = WgpuBackend::new_with_backends(opts.backends, opts.memory_hints)
                    .map_err(|e| BackendSkipReason::Error(e.to_string()))?;
                backend.set_acquire_fence_mode(opts.acquire_fence_mode);
                let adapter = backend
                    .select_adapter(opts)
                    .map_err(|_| BackendSkipReason::AdapterUnavailable)?;
                Ok((Arc::new(backend), adapter))
            }
            #[cfg(not(feature = "gpu-wgpu"))]
            {
                Err(BackendSkipReason::FeatureNotEnabled)
            }
        }
        GpuBackendKind::Mock => {
            #[cfg(feature = "gpu-mock")]
            {
                let backend = MockBackend::default();
                let adapter = backend
                    .select_adapter(opts)
                    .map_err(|_| BackendSkipReason::AdapterUnavailable)?;
                Ok((Arc::new(backend), adapter))
            }
            #[cfg(not(feature = "gpu-mock"))]
            {
                Err(BackendSkipReason::FeatureNotEnabled)
            }
        }
        GpuBackendKind::Cuda => {
            #[cfg(feature = "gpu-cuda")]
            {
                let backend = crate::CudaBackend::new()
                    .map_err(|e| BackendSkipReason::Error(e.to_string()))?;
                let adapter = backend
                    .select_adapter(opts)
                    .map_err(|_| BackendSkipReason::AdapterUnavailable)?;
                Ok((Arc::new(backend), adapter))
            }
            #[cfg(not(feature = "gpu-cuda"))]
            {
                Err(BackendSkipReason::FeatureNotEnabled)
            }
        }
        GpuBackendKind::Noop => {
            let backend = NoopBackend::default();
            let adapter = backend
                .select_adapter(opts)
                .map_err(|_| BackendSkipReason::AdapterUnavailable)?;
            Ok((Arc::new(backend), adapter))
        }
    }
}

#[cfg(feature = "gpu-async")]
async fn try_build_backend_async(
    kind: GpuBackendKind,
    opts: &GpuOptions,
) -> Result<(Arc<dyn GpuBackend>, GpuAdapterInfo), BackendSkipReason> {
    match kind {
        GpuBackendKind::Wgpu => {
            #[cfg(feature = "gpu-wgpu")]
            {
                let mut backend =
                    WgpuBackend::new_with_backends_async(opts.backends, opts.memory_hints)
                        .await
                        .map_err(|e| BackendSkipReason::Error(e.to_string()))?;
                backend.set_acquire_fence_mode(opts.acquire_fence_mode);
                let adapter = backend
                    .select_adapter(opts)
                    .map_err(|_| BackendSkipReason::AdapterUnavailable)?;
                Ok((Arc::new(backend), adapter))
            }
            #[cfg(not(feature = "gpu-wgpu"))]
            {
                Err(BackendSkipReason::FeatureNotEnabled)
            }
        }
        GpuBackendKind::Mock => {
            #[cfg(feature = "gpu-mock")]
            {
                let backend = MockBackend::default();
                let adapter = backend
                    .select_adapter(opts)
                    .map_err(|_| BackendSkipReason::AdapterUnavailable)?;
                Ok((Arc::new(backend), adapter))
            }
            #[cfg(not(feature = "gpu-mock"))]
            {
                Err(BackendSkipReason::FeatureNotEnabled)
            }
        }
        GpuBackendKind::Cuda => {
            #[cfg(feature = "gpu-cuda")]
            {
                let backend = crate::CudaBackend::new()
                    .map_err(|e| BackendSkipReason::Error(e.to_string()))?;
                let adapter = backend
                    .select_adapter(opts)
                    .map_err(|_| BackendSkipReason::AdapterUnavailable)?;
                Ok((Arc::new(backend), adapter))
            }
            #[cfg(not(feature = "gpu-cuda"))]
            {
                Err(BackendSkipReason::FeatureNotEnabled)
            }
        }
        GpuBackendKind::Noop => {
            let backend = NoopBackend::default();
            let adapter = backend
                .select_adapter(opts)
                .map_err(|_| BackendSkipReason::AdapterUnavailable)?;
            Ok((Arc::new(backend), adapter))
        }
    }
}
