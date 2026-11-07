//! The engine's GPU context: created when a plan first needs one (a plan that runs on a GPU, or
//! a planner that reads the device's caps), then kept for the engine's lifetime. A CPU-only plan
//! never creates the device, and the executors hold the context only when their plan needs it.

use crate::config::GpuBackend;
use crate::error::EngineError;
use crate::portable::Arc;

use super::Engine;

impl Engine {
    /// The GPU context when `needed` (a plan that runs on a GPU), else none: a CPU-only plan never
    /// creates the device.
    #[cfg(feature = "gpu")]
    pub(super) fn gpu_handle_if(
        &self,
        needed: bool,
    ) -> Result<Option<Arc<daedalus_gpu::GpuContextHandle>>, EngineError> {
        if needed {
            self.get_gpu_handle()
        } else {
            Ok(None)
        }
    }

    /// Whether this engine has created its GPU context (tests: a CPU-only plan leaves it absent).
    #[cfg(feature = "gpu")]
    #[doc(hidden)]
    pub fn gpu_context_created(&self) -> bool {
        self.gpu_handle.lock().is_some()
    }

    #[cfg(feature = "gpu")]
    pub(crate) fn get_gpu_handle(
        &self,
    ) -> Result<Option<Arc<daedalus_gpu::GpuContextHandle>>, EngineError> {
        use daedalus_gpu::{GpuBackendKind, GpuOptions, select_backend, shared_context};
        if matches!(self.config.gpu, GpuBackend::Cpu) {
            return Ok(None);
        }
        let mut guard = self.gpu_handle.lock();
        if let Some(handle) = guard.as_ref() {
            return Ok(Some(handle.clone()));
        }
        let memory_hints = match self.config.gpu_memory_hints {
            crate::GpuMemoryHints::Auto => daedalus_gpu::GpuMemoryHints::Auto,
            crate::GpuMemoryHints::Performance => daedalus_gpu::GpuMemoryHints::Performance,
            crate::GpuMemoryHints::MemoryUsage => daedalus_gpu::GpuMemoryHints::MemoryUsage,
        };
        let opts = match self.config.gpu {
            GpuBackend::Cpu => return Ok(None),
            GpuBackend::Mock => GpuOptions {
                preferred_backend: Some(GpuBackendKind::Mock),
                adapter_label: None,
                allow_software: true,
                ..Default::default()
            },
            GpuBackend::Device => GpuOptions {
                preferred_backend: Some(GpuBackendKind::Wgpu),
                adapter_label: None,
                allow_software: false,
                memory_hints,
                ..Default::default()
            },
        };
        // A device is the process's (`shared_context`), shared with every other engine and library
        // that takes it there; the mock stays per engine.
        let handle = Arc::new(match self.config.gpu {
            GpuBackend::Device => shared_context(&opts)?,
            _ => select_backend(&opts)?,
        });
        #[cfg(feature = "gpu-wgpu")]
        if let Some(dir) = self.config.cache.gpu_pipeline_dir.as_deref() {
            *self.pipeline_cache.lock() = Some(Arc::new(
                crate::pipeline_cache::EnginePipelineCache::open(&handle, dir),
            ));
        }
        *guard = Some(handle.clone());
        Ok(Some(handle))
    }
}
