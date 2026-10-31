//! The engine's GPU pipeline cache seen from a graph (`crate::pipeline_cache`).

use daedalus_runtime::executor::NodeHandler;

use super::HostGraph;
use crate::engine::Engine;
use crate::error::EngineError;
use crate::pipeline_cache::PipelineCacheStatus;

impl<H: NodeHandler + Send + Sync + 'static> HostGraph<H> {
    /// The engine's GPU pipeline cache ([`CacheSection::gpu_pipeline_dir`]): its file, the bytes
    /// loaded and saved, and why it is not in use. `None` without a configured directory, a GPU
    /// device, or the `gpu-wgpu` feature.
    ///
    /// [`CacheSection::gpu_pipeline_dir`]: crate::CacheSection::gpu_pipeline_dir
    pub fn pipeline_cache_status(&self) -> Option<PipelineCacheStatus> {
        #[cfg(feature = "gpu-wgpu")]
        {
            self.pipeline_cache
                .as_ref()
                .map(|saver| saver.cache.status())
        }
        #[cfg(not(feature = "gpu-wgpu"))]
        {
            None
        }
    }

    /// Save the engine's GPU pipeline cache now if pipelines were built since its last save.
    /// Returns the bytes written, `None` when nothing was. The graph also saves it after its
    /// 1st, 2nd, 4th, ... 256th tick and on drop; call this after warming a pipeline up when
    /// the process may stop without dropping the graph.
    pub fn save_pipeline_cache(&self) -> Result<Option<usize>, EngineError> {
        #[cfg(feature = "gpu-wgpu")]
        if let Some(saver) = &self.pipeline_cache {
            return saver.cache.save_if_grown();
        }
        Ok(None)
    }

    /// Every tick path starts here.
    pub(super) fn tick_start(&mut self) {
        self.frame_commit();
        #[cfg(feature = "gpu-wgpu")]
        if let Some(saver) = self.pipeline_cache.as_mut() {
            saver.tick_started();
        }
    }
}

impl Engine {
    /// The engine's GPU pipeline cache status ([`HostGraph::pipeline_cache_status`]); `None`
    /// until the engine creates its GPU device.
    pub fn pipeline_cache_status(&self) -> Option<PipelineCacheStatus> {
        #[cfg(feature = "gpu-wgpu")]
        {
            self.pipeline_cache
                .lock()
                .as_ref()
                .map(|cache| cache.status())
        }
        #[cfg(not(feature = "gpu-wgpu"))]
        {
            None
        }
    }

    /// A new graph's handle on the cache, when the engine has one.
    #[cfg(feature = "gpu-wgpu")]
    pub(crate) fn pipeline_cache_saver(&self) -> Option<crate::pipeline_cache::PipelineCacheSaver> {
        self.pipeline_cache
            .lock()
            .clone()
            .map(crate::pipeline_cache::PipelineCacheSaver::new)
    }
}
