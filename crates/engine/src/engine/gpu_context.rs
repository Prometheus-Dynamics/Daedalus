//! The engine's GPU context, opened on first use. The executor takes the source as its
//! `GpuSource`: a node opens the device when it asks for its context, and an edge adapter when it
//! first uses the device. A failed open is retried with a capped backoff, never given up, and a
//! Cpu engine never creates the device.

use daedalus_gpu::{GpuBackendKind, GpuContextHandle, GpuOptions, select_backend, shared_context};
use daedalus_runtime::RetryingGpu;

use crate::config::{EngineConfig, GpuBackend};
use crate::portable::Arc;

/// The engine's GPU source, and the pipeline cache that opens with its device.
pub(crate) struct LazyGpu {
    pub(crate) source: Arc<RetryingGpu>,
    #[cfg(feature = "gpu-wgpu")]
    pub(crate) pipeline_cache:
        Arc<daedalus_runtime::sync::Mutex<Option<Arc<crate::pipeline_cache::EnginePipelineCache>>>>,
}

impl LazyGpu {
    pub(crate) fn new(config: &EngineConfig) -> Self {
        let memory_hints = match config.gpu_memory_hints {
            crate::GpuMemoryHints::Auto => daedalus_gpu::GpuMemoryHints::Auto,
            crate::GpuMemoryHints::Performance => daedalus_gpu::GpuMemoryHints::Performance,
            crate::GpuMemoryHints::MemoryUsage => daedalus_gpu::GpuMemoryHints::MemoryUsage,
        };
        let (opts, shared) = match config.gpu {
            GpuBackend::Device => (
                GpuOptions {
                    preferred_backend: Some(GpuBackendKind::Wgpu),
                    adapter_label: None,
                    allow_software: false,
                    memory_hints,
                    ..Default::default()
                },
                true,
            ),
            GpuBackend::Cpu | GpuBackend::Mock => (
                GpuOptions {
                    preferred_backend: Some(GpuBackendKind::Mock),
                    adapter_label: None,
                    allow_software: true,
                    ..Default::default()
                },
                false,
            ),
        };
        #[cfg(feature = "gpu-wgpu")]
        let pipeline_cache = Arc::new(daedalus_runtime::sync::Mutex::new(None));
        #[cfg(feature = "gpu-wgpu")]
        let (slot, dir) = (
            pipeline_cache.clone(),
            config.cache.gpu_pipeline_dir.clone(),
        );
        let caps_dir = super::gpu_caps::cache_dir(config);
        let source = RetryingGpu::new(move || {
            let handle = if shared {
                shared_context(&opts)?
            } else {
                select_backend(&opts)?
            };
            // The first real enumeration: the caps are recorded for the next plan, and a cached
            // entry that disagrees with them is replaced.
            super::gpu_caps::record_real_caps(caps_dir.as_deref(), real_caps(&handle));
            #[cfg(feature = "gpu-wgpu")]
            if let Some(dir) = dir.as_deref() {
                *slot.lock() = Some(Arc::new(crate::pipeline_cache::EnginePipelineCache::open(
                    &handle, dir,
                )));
            }
            Ok(handle)
        });
        Self {
            source: Arc::new(source),
            #[cfg(feature = "gpu-wgpu")]
            pipeline_cache,
        }
    }
}

/// The caps the planner reads, from a real enumeration of `handle`.
fn real_caps(handle: &GpuContextHandle) -> daedalus_planner::GpuCaps {
    let caps = handle.capabilities();
    daedalus_planner::GpuCaps {
        queue_count: caps.queue_count,
        has_transfer_queue: caps.has_transfer_queue,
        rgba8_unorm_sampleable: caps
            .format_features
            .iter()
            .any(|f| f.format == daedalus_gpu::GpuFormat::Rgba8Unorm && f.sampleable),
    }
}

impl super::Engine {
    /// Whether this engine has opened its GPU device (tests: a CPU-only plan leaves it closed).
    #[doc(hidden)]
    pub fn gpu_context_created(&self) -> bool {
        self.lazy_gpu.source.created()
    }
}
