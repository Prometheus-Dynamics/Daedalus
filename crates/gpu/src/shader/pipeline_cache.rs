//! On-disk pipeline cache across process runs (Vulkan): shorter startup on small GPUs.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

use crate::{GpuContextHandle, GpuError};

/// Caches installed per device ([`GpuPipelineCache::install`]), keyed by `device_key`.
static INSTALLED: OnceLock<Mutex<HashMap<usize, wgpu::PipelineCache>>> = OnceLock::new();

fn installed() -> &'static Mutex<HashMap<usize, wgpu::PipelineCache>> {
    INSTALLED.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The pipeline cache installed for `device`, for a node building its own pipelines: pass it as
/// the descriptor's `cache`. Daedalus's own pipelines (shader dispatch, [`super::FragmentPass`])
/// use it automatically.
pub fn pipeline_cache_for(device: &wgpu::Device) -> Option<wgpu::PipelineCache> {
    installed().lock().get(&super::device_key(device)).cloned()
}

/// A Vulkan pipeline cache loaded from and saved to a file named after the adapter (vendor and
/// device) in a directory. The driver validates the data's header (vendor, device, driver
/// version, cache UUID) on load: data from another driver is dropped and the cache starts empty,
/// so a driver update invalidates it without a manual step.
///
/// Open it at startup, [`install`](Self::install) it before building pipelines, and
/// [`save`](Self::save) it after (or on shutdown).
pub struct GpuPipelineCache {
    cache: wgpu::PipelineCache,
    path: PathBuf,
    loaded_bytes: usize,
    /// Bytes in the file: loaded at `open`, then the last save.
    file_bytes: AtomicUsize,
    /// The backend device's key (`device_key` is per `wgpu::Device` handle, so it is taken from
    /// the backend's own handle at `open`).
    device_key: usize,
}

impl std::fmt::Debug for GpuPipelineCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuPipelineCache")
            .field("path", &self.path)
            .field("loaded_bytes", &self.loaded_bytes)
            .finish_non_exhaustive()
    }
}

impl GpuPipelineCache {
    /// Open (or start) the cache for `gpu`'s adapter in `dir`, creating `dir`. `Unsupported`
    /// when the device has no pipeline cache (not Vulkan, or wgpu's `PIPELINE_CACHE` feature
    /// missing); an unreadable or stale file starts an empty cache.
    pub fn open(gpu: &GpuContextHandle, dir: impl AsRef<Path>) -> Result<Self, GpuError> {
        let (device, _) = gpu.wgpu().ok_or(GpuError::Unsupported)?;
        if !device.features().contains(wgpu::Features::PIPELINE_CACHE) {
            return Err(GpuError::Unsupported);
        }
        let key =
            wgpu::util::pipeline_cache_key(&device.adapter_info()).ok_or(GpuError::Unsupported)?;
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).map_err(|err| {
            GpuError::InvalidInput(format!("pipeline cache dir {}: {err}", dir.display()))
        })?;
        let path = dir.join(key);
        let data = std::fs::read(&path).ok();
        // SAFETY: the data was written by `save` from `PipelineCache::get_data` for an adapter
        // with this key; with `fallback`, the driver validates it and starts empty when it does
        // not match.
        let cache = unsafe {
            device.create_pipeline_cache(&wgpu::PipelineCacheDescriptor {
                label: Some("daedalus-pipeline-cache"),
                data: data.as_deref(),
                fallback: true,
            })
        };
        let loaded_bytes = data.map_or(0, |data| data.len());
        Ok(Self {
            cache,
            path,
            loaded_bytes,
            file_bytes: AtomicUsize::new(loaded_bytes),
            device_key: super::device_key(device),
        })
    }

    /// The wgpu cache, for pipeline descriptors.
    pub fn cache(&self) -> &wgpu::PipelineCache {
        &self.cache
    }

    /// The cache file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Bytes read from the file at [`open`](Self::open) (0 for a fresh cache).
    pub fn loaded_bytes(&self) -> usize {
        self.loaded_bytes
    }

    /// Use this cache for every pipeline Daedalus builds on its device, and for
    /// [`pipeline_cache_for`]. Replaces a cache installed before.
    pub fn install(&self) {
        installed()
            .lock()
            .insert(self.device_key, self.cache.clone());
    }

    /// Write the cache's current data to its file (atomically: a temporary file, then a rename).
    /// Returns the bytes written (0 when the driver had nothing to save).
    pub fn save(&self) -> Result<usize, GpuError> {
        let Some(data) = self.cache.get_data() else {
            return Ok(0);
        };
        self.write(&data)
    }

    /// [`save`](Self::save) only when the cache grew past what the file holds (new pipelines
    /// were built since `open` or the last save). `None` when nothing was written. Cheap enough
    /// to call after the first ticks of a pipeline: the driver copies its data, no file I/O.
    pub fn save_if_grown(&self) -> Result<Option<usize>, GpuError> {
        match self.cache.get_data() {
            Some(data) if data.len() > self.file_bytes.load(Ordering::Relaxed) => {
                self.write(&data).map(Some)
            }
            _ => Ok(None),
        }
    }

    /// Bytes the file holds: those loaded at `open`, or the last save's.
    pub fn file_bytes(&self) -> usize {
        self.file_bytes.load(Ordering::Relaxed)
    }

    fn write(&self, data: &[u8]) -> Result<usize, GpuError> {
        let tmp = self.path.with_extension("tmp");
        let io = |err: std::io::Error| {
            GpuError::Internal(format!("pipeline cache {}: {err}", self.path.display()))
        };
        std::fs::write(&tmp, data).map_err(io)?;
        std::fs::rename(&tmp, &self.path).map_err(io)?;
        self.file_bytes.store(data.len(), Ordering::Relaxed);
        Ok(data.len())
    }
}
