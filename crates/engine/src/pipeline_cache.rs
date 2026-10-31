//! The engine's on-disk GPU pipeline cache ([`CacheSection::gpu_pipeline_dir`]).
//!
//! The engine opens and installs it when it creates its GPU device, so every pipeline built on
//! that device uses it, including those a node builds in its first tick. Each [`HostGraph`]
//! compiled from the engine saves it when it grew: after its 1st, 2nd, 4th, ... 256th tick
//! (checked when the next tick starts), on [`HostGraph::save_pipeline_cache`], and on drop. A
//! process that never shuts down cleanly still keeps what its first ticks built.
//!
//! [`CacheSection::gpu_pipeline_dir`]: crate::CacheSection::gpu_pipeline_dir
//! [`HostGraph`]: crate::HostGraph
//! [`HostGraph::save_pipeline_cache`]: crate::HostGraph::save_pipeline_cache

use crate::prelude::*;

/// What the engine's GPU pipeline cache loaded and saved
/// ([`HostGraph::pipeline_cache_status`](crate::HostGraph::pipeline_cache_status)).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PipelineCacheStatus {
    /// The configured directory.
    pub dir: String,
    /// The cache file, named after the adapter. `None` when the cache could not be opened.
    pub path: Option<String>,
    /// Bytes read from the file when the engine created its device (0 for a new cache).
    pub loaded_bytes: usize,
    /// Bytes the file holds: the loaded bytes, or the last save's.
    pub file_bytes: usize,
    /// Saves that wrote the file.
    pub saves: u32,
    /// Why the cache could not be opened (no Vulkan pipeline cache on the device, an
    /// unwritable directory), or the last failed save.
    pub error: Option<String>,
}

#[cfg(feature = "gpu-wgpu")]
pub(crate) use imp::{EnginePipelineCache, PipelineCacheSaver};

#[cfg(feature = "gpu-wgpu")]
mod imp {
    use core::sync::atomic::{AtomicU32, Ordering};

    use daedalus_gpu::{GpuContextHandle, GpuPipelineCache};

    use super::PipelineCacheStatus;
    use crate::error::EngineError;
    use crate::portable::Arc;
    use crate::prelude::*;
    use crate::trace;

    /// The engine's cache, shared with every graph compiled from it.
    pub(crate) struct EnginePipelineCache {
        dir: String,
        cache: Result<GpuPipelineCache, String>,
        saves: AtomicU32,
        last_error: daedalus_runtime::sync::Mutex<Option<String>>,
    }

    impl EnginePipelineCache {
        /// Open and install the cache for `gpu`'s device. A failure is recorded in the status:
        /// the engine runs without a cache.
        pub(crate) fn open(gpu: &GpuContextHandle, dir: &str) -> Self {
            let cache = GpuPipelineCache::open(gpu, dir).map_err(|err| err.to_string());
            match &cache {
                Ok(cache) => {
                    cache.install();
                    trace::info!(
                        path = %cache.path().display(),
                        loaded_bytes = cache.loaded_bytes(),
                        "GPU pipeline cache loaded"
                    );
                }
                Err(err) => trace::warn!(dir, error = %err, "GPU pipeline cache not opened"),
            }
            Self {
                dir: dir.into(),
                cache,
                saves: AtomicU32::new(0),
                last_error: daedalus_runtime::sync::Mutex::new(None),
            }
        }

        /// Write the file when the cache grew. `Ok(None)` when nothing was written.
        pub(crate) fn save_if_grown(&self) -> Result<Option<usize>, EngineError> {
            let Ok(cache) = &self.cache else {
                return Ok(None);
            };
            match cache.save_if_grown() {
                Ok(saved) => {
                    if let Some(bytes) = saved {
                        self.saves.fetch_add(1, Ordering::Relaxed);
                        trace::info!(
                            path = %cache.path().display(),
                            bytes,
                            "GPU pipeline cache saved"
                        );
                    }
                    Ok(saved)
                }
                Err(err) => {
                    trace::warn!(error = %err, "GPU pipeline cache not saved");
                    *self.last_error.lock() = Some(err.to_string());
                    Err(err.into())
                }
            }
        }

        pub(crate) fn status(&self) -> PipelineCacheStatus {
            let (path, loaded_bytes, file_bytes, open_error) = match &self.cache {
                Ok(cache) => (
                    Some(cache.path().display().to_string()),
                    cache.loaded_bytes(),
                    cache.file_bytes(),
                    None,
                ),
                Err(err) => (None, 0, 0, Some(err.clone())),
            };
            PipelineCacheStatus {
                dir: self.dir.clone(),
                path,
                loaded_bytes,
                file_bytes,
                saves: self.saves.load(Ordering::Relaxed),
                error: open_error.or_else(|| self.last_error.lock().clone()),
            }
        }
    }

    /// A graph's handle on the engine's cache: saves it after the graph's first ticks and on drop.
    pub(crate) struct PipelineCacheSaver {
        pub(crate) cache: Arc<EnginePipelineCache>,
        ticks: u32,
    }

    /// Ticks after which a grown cache is saved: 1, 2, 4, ... 256.
    const LAST_AUTO_SAVE_TICK: u32 = 256;

    impl PipelineCacheSaver {
        pub(crate) fn new(cache: Arc<EnginePipelineCache>) -> Self {
            Self { cache, ticks: 0 }
        }

        /// Called as a tick starts: saves a grown cache after the 1st, 2nd, 4th, ... tick.
        pub(crate) fn tick_started(&mut self) {
            if self.ticks > LAST_AUTO_SAVE_TICK {
                return;
            }
            let done = self.ticks;
            self.ticks += 1;
            if done.is_power_of_two() {
                // A failed save is logged and reported in the status; the tick goes on.
                let _ = self.cache.save_if_grown();
            }
        }
    }

    impl Drop for PipelineCacheSaver {
        fn drop(&mut self) {
            let _ = self.cache.save_if_grown();
        }
    }
}
