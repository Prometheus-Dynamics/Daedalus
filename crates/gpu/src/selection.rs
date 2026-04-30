use std::sync::Arc;

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
/// [`select_backend_async`] with the `gpu-async` feature.
/// Order: preferred backend (if set), then wgpu, mock, noop.
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
                let backend =
                    WgpuBackend::new().map_err(|e| BackendSkipReason::Error(e.to_string()))?;
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
                let backend = WgpuBackend::new_async()
                    .await
                    .map_err(|e| BackendSkipReason::Error(e.to_string()))?;
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
        GpuBackendKind::Noop => {
            let backend = NoopBackend::default();
            let adapter = backend
                .select_adapter(opts)
                .map_err(|_| BackendSkipReason::AdapterUnavailable)?;
            Ok((Arc::new(backend), adapter))
        }
    }
}
