use crate::shader::submit_lock::{LockedQueue, SubmitLock};
use parking_lot::{Mutex, MutexGuard};
use std::sync::Arc;

use crate::handles::{GpuBufferHandle, GpuDropToken, GpuImageHandle};
use crate::shader::SubmissionTracker;
use crate::traits::GpuBackend;
use crate::{
    GpuAdapterInfo, GpuBackendKind, GpuBackends, GpuCapabilities, GpuError, GpuFormat,
    GpuImageRequest, GpuMemoryLocation, GpuOptions, GpuRequest, GpuUsage, buffer::TransferStats,
    format_bytes_per_pixel, validate_texture_bytes,
};
#[cfg(all(feature = "gpu-async", feature = "gpu-wgpu"))]
use async_trait::async_trait;
use pollster::FutureExt;
use wgpu::{Adapter, Backends, Features, Limits};

mod adapter_select;
mod capabilities;
mod copy_limiter;
pub(crate) mod dmabuf;
mod driver;
mod mapping;
mod resources;
mod staging;
mod texture_transfer;

use crate::shader::staging_size;
use adapter_select::{preferred_backends, rank_adapters};
use capabilities::{build_info_from_adapter, caps_from_adapter};
use copy_limiter::CopyLimiter;
pub use dmabuf::texture_plane_views;
pub use driver::{DriverGuard, driver_lock, with_driver_lock};
pub(crate) use driver::{LockedInstance, open_device};
pub(crate) use mapping::{gpu_format_from_wgpu, map_format};
use mapping::{gpu_usage_from_wgpu, map_texture_usage, map_usage};
use resources::{ResourceDropToken, ResourceKind, WgpuResources};
use staging::StagingPool;
pub use staging::{WgpuStagingPoolConfig, WgpuStagingPoolStats};
use texture_transfer::unpad_rows;

/// Minimal wgpu backend placeholder to satisfy trait; queries adapter limits when available.
pub struct WgpuBackend {
    /// dmabuf import state; declared first so it drops (destroying its Vulkan objects) while
    /// `device` is still alive.
    #[cfg_attr(
        not(all(feature = "gpu-dmabuf", target_os = "linux")),
        allow(dead_code)
    )]
    dmabuf: dmabuf::ImportState,
    adapter: GpuAdapterInfo,
    caps: GpuCapabilities,
    stats: Mutex<TransferStats>,
    _features: Features,
    _limits: Limits,
    device: wgpu::Device,
    /// The queue with the context's submit lock (see `shader::submit_lock`).
    queue: LockedQueue,
    resources: Arc<WgpuResources>,
    staging_pool: Mutex<StagingPool>,
    copy_limiter: CopyLimiter,
    submission_tracker: SubmissionTracker,
    device_key: usize,
    dmabuf_support: crate::ExternalImportSupport,
    /// Resolved at device creation: `Performance` or `MemoryUsage`.
    memory_hints: crate::GpuMemoryHints,
    /// Set by the device-lost callback: the context is then replaced by its source.
    lost: Arc<std::sync::atomic::AtomicBool>,
    lost_reason: Arc<Mutex<Option<String>>>,
    /// Uncaptured errors the device reported, returned by the next dispatch.
    pending: Arc<crate::shader::wait::PendingErrors>,
}

impl WgpuBackend {
    /// Create a wgpu backend using the synchronous compatibility path.
    ///
    /// This blocks while wgpu enumerates adapters and requests a device. Async callers should use
    /// [`WgpuBackend::new_async`] to avoid blocking executor threads.
    pub fn new() -> Result<Self, GpuError> {
        Self::new_with_backends(None, crate::GpuMemoryHints::Auto)
    }

    /// Synchronous [`WgpuBackend::new_with_backends_async`] (same blocking caveat as `new`).
    pub(crate) fn new_with_backends(
        requested: Option<GpuBackends>,
        memory_hints: crate::GpuMemoryHints,
    ) -> Result<Self, GpuError> {
        let res = std::panic::catch_unwind(|| {
            Self::new_with_backends_async(requested, memory_hints).block_on()
        });

        match res {
            Ok(ok) => ok,
            Err(_) => Err(GpuError::AdapterUnavailable),
        }
    }

    /// The wgpu backend on the best adapter of PCI vendor `vendor` (`0x10DE` is NVIDIA), for code
    /// that shares a device with another API (CUDA interop). Fails when no such adapter opens.
    /// Synchronous, like [`WgpuBackend::new`].
    pub fn new_on_vendor(vendor: u32) -> Result<Self, GpuError> {
        let staging_config = WgpuStagingPoolConfig::from_env().map_err(GpuError::Internal)?;
        let res = std::panic::catch_unwind(|| {
            Self::new_selected_async(
                staging_config,
                None,
                crate::GpuMemoryHints::Auto,
                Some(vendor),
            )
            .block_on()
        });
        match res {
            Ok(ok) => ok,
            Err(_) => Err(GpuError::AdapterUnavailable),
        }
    }

    /// Create a wgpu backend with explicit staging-pool config using the synchronous compatibility
    /// path.
    ///
    /// This blocks while wgpu enumerates adapters and requests a device. Async callers should use
    /// [`WgpuBackend::new_with_staging_pool_config_async`] to avoid blocking executor threads.
    pub fn new_with_staging_pool_config(config: WgpuStagingPoolConfig) -> Result<Self, GpuError> {
        let res = std::panic::catch_unwind(|| {
            Self::new_with_staging_pool_config_async(config).block_on()
        });

        match res {
            Ok(ok) => ok,
            Err(_) => Err(GpuError::AdapterUnavailable),
        }
    }

    /// Create a wgpu backend without blocking the current thread on async wgpu operations.
    pub async fn new_async() -> Result<Self, GpuError> {
        Self::new_with_backends_async(None, crate::GpuMemoryHints::Auto).await
    }

    /// Create a wgpu backend that enumerates the `requested` graphics APIs (see
    /// [`GpuOptions::backends`](crate::GpuOptions)); `WGPU_BACKEND` takes precedence when set.
    /// [`GpuOptions::memory_hints`](crate::GpuOptions) sets the device's `MemoryHints`.
    pub(crate) async fn new_with_backends_async(
        requested: Option<GpuBackends>,
        memory_hints: crate::GpuMemoryHints,
    ) -> Result<Self, GpuError> {
        let staging_config = WgpuStagingPoolConfig::from_env().map_err(GpuError::Internal)?;
        Self::new_selected_async(staging_config, requested, memory_hints, None).await
    }

    /// Creates the wgpu backend with a staging-pool configuration, asynchronously.
    pub async fn new_with_staging_pool_config_async(
        staging_config: WgpuStagingPoolConfig,
    ) -> Result<Self, GpuError> {
        Self::new_selected_async(staging_config, None, crate::GpuMemoryHints::Auto, None).await
    }

    async fn new_selected_async(
        staging_config: WgpuStagingPoolConfig,
        requested: Option<GpuBackends>,
        memory_hints: crate::GpuMemoryHints,
        vendor: Option<u32>,
    ) -> Result<Self, GpuError> {
        let adapters = {
            let driver = driver_lock();
            let preferred_backends = preferred_backends(requested);
            let instance = LockedInstance::new(&driver, preferred_backends);
            let mut adapters: Vec<Adapter> = instance.enumerate_adapters(preferred_backends).await;
            if adapters.is_empty() && preferred_backends != Backends::all() {
                adapters = instance.enumerate_adapters(Backends::all()).await;
            }
            if let Some(vendor) = vendor {
                adapters.retain(|a| a.get_info().vendor == vendor);
            }
            rank_adapters(adapters)
        };
        // The best adapter first; when its device cannot be created (out of memory on a GPU
        // another process fills, a lost device), the next one.
        let mut last = GpuError::AdapterUnavailable;
        for adapter in adapters {
            let name = adapter.get_info().name;
            match Self::with_adapter(adapter, staging_config, memory_hints).await {
                Ok(backend) => return Ok(backend),
                Err(err) => {
                    tracing::warn!(
                        target: "daedalus_gpu::wgpu",
                        adapter = %name,
                        error = %err,
                        "device request failed; trying the next adapter"
                    );
                    last = err;
                }
            }
        }
        Err(last)
    }

    /// Create the backend on a specific adapter.
    pub(crate) async fn with_adapter(
        adapter: Adapter,
        staging_config: WgpuStagingPoolConfig,
        memory_hints: crate::GpuMemoryHints,
    ) -> Result<Self, GpuError> {
        let memory_hints = memory_hints.resolve(adapter.get_info().device_type);
        let (info, features, limits) = build_info_from_adapter(&adapter);
        let mut caps = caps_from_adapter(Some(&adapter), &limits);

        let device_desc = wgpu::DeviceDescriptor {
            label: Some("wgpu-backend"),
            required_features: adapter.features() & capabilities::OPTIONAL_COMPUTE_FEATURES,
            required_limits: adapter.limits(),
            experimental_features: wgpu::ExperimentalFeatures::disabled(),
            memory_hints: memory_hints.to_wgpu(),
            trace: wgpu::Trace::default(),
        };
        // With `gpu-dmabuf` on Linux/Vulkan this also enables the dmabuf import extensions.
        let driver = driver_lock();
        let (device, queue) = dmabuf::request_device(&driver, &adapter, &device_desc)
            .await
            .map_err(|err| GpuError::Internal(format!("wgpu device request failed: {err}")))?;
        drop(driver);
        crate::selection::DEVICES_CREATED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        caps.compute.features = capabilities::compute_features(device.features());
        let queue = LockedQueue::new(queue, SubmitLock::default());
        let (dmabuf_support, dmabuf) = dmabuf::probe(&device, &queue);

        let device_key = crate::shader::register_device(&device);

        // Avoid process-level panics on uncaptured backend errors (OOM/validation).
        // We still surface the error for diagnostics, but keep the runtime alive.
        let lost = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let lost_reason = Arc::new(Mutex::new(None::<String>));
        {
            let lost = lost.clone();
            let lost_reason = lost_reason.clone();
            device.set_device_lost_callback(move |reason, message| {
                tracing::error!(
                    target: "daedalus_gpu::wgpu",
                    reason = ?reason,
                    message = %message,
                    "gpu device lost"
                );
                *lost_reason.lock() = Some(format!("device lost ({reason:?}): {message}"));
                lost.store(true, std::sync::atomic::Ordering::Release);
            });
        }
        let pending = Arc::new(crate::shader::wait::PendingErrors::default());
        {
            let pending = pending.clone();
            device.on_uncaptured_error(std::sync::Arc::new(move |err| {
                tracing::error!(
                    target: "daedalus_gpu::wgpu",
                    error = %err,
                    "uncaptured wgpu error"
                );
                pending.record(err.to_string());
            }));
        }

        Ok(Self {
            dmabuf,
            adapter: info,
            caps: caps.clone(),
            stats: Mutex::new(TransferStats::default()),
            _features: features,
            _limits: limits,
            device,
            queue,
            resources: Arc::new(WgpuResources::default()),
            staging_pool: Mutex::new(StagingPool::with_config(staging_config)),
            copy_limiter: CopyLimiter::new(caps.max_inflight_copies.max(1)),
            submission_tracker: SubmissionTracker::default(),
            device_key,
            dmabuf_support,
            memory_hints,
            lost,
            lost_reason,
            pending,
        })
    }

    /// The device's memory hints, `Auto` resolved for the adapter
    /// ([`GpuOptions::memory_hints`](crate::GpuOptions)).
    pub fn memory_hints(&self) -> crate::GpuMemoryHints {
        self.memory_hints
    }

    /// Set the default [`AcquireFenceMode`](crate::AcquireFenceMode) of dmabuf imports
    /// (`Auto` unless changed; `select_backend` applies `GpuOptions::acquire_fence_mode`).
    /// [`dmabuf_import_support`](GpuBackend::dmabuf_import_support) reports the wait it resolves
    /// to. Has no effect on a backend that cannot import.
    pub fn set_acquire_fence_mode(&mut self, mode: crate::AcquireFenceMode) {
        if let Some(support) = dmabuf::set_fence_mode(&mut self.dmabuf, mode) {
            self.dmabuf_support = support;
        }
    }

    pub(crate) fn device_queue(&self) -> (&wgpu::Device, &LockedQueue) {
        (&self.device, &self.queue)
    }

    pub fn staging_pool_stats(&self) -> WgpuStagingPoolStats {
        self.staging_pool.lock().stats()
    }

    fn stats_guard(&self) -> MutexGuard<'_, TransferStats> {
        self.stats.lock()
    }

    pub(crate) fn get_texture(
        &self,
        handle: &GpuImageHandle,
    ) -> Option<std::sync::Arc<wgpu::Texture>> {
        self.resources.textures.lock().get(&handle.id).cloned()
    }

    /// Track `texture` and return its handle; fails for wgpu formats with no [`GpuFormat`].
    pub(crate) fn register_texture(
        &self,
        texture: std::sync::Arc<wgpu::Texture>,
        format: wgpu::TextureFormat,
        width: u32,
        height: u32,
        usage: wgpu::TextureUsages,
    ) -> Result<GpuImageHandle, GpuError> {
        let format = gpu_format_from_wgpu(format).ok_or(GpuError::Unsupported)?;
        Ok(self.register_gpu_texture(texture, format, width, height, usage))
    }

    pub(crate) fn register_gpu_texture(
        &self,
        texture: std::sync::Arc<wgpu::Texture>,
        gpu_format: GpuFormat,
        width: u32,
        height: u32,
        usage: wgpu::TextureUsages,
    ) -> GpuImageHandle {
        self.register_gpu_texture_with(texture, gpu_format, width, height, usage, |token| token)
            .0
    }

    /// [`register_gpu_texture`](Self::register_gpu_texture) with `wrap` building the handle's drop
    /// token around the one that untracks the texture; also returns that token.
    fn register_gpu_texture_with<T: GpuDropToken + 'static>(
        &self,
        texture: std::sync::Arc<wgpu::Texture>,
        gpu_format: GpuFormat,
        width: u32,
        height: u32,
        usage: wgpu::TextureUsages,
        wrap: impl FnOnce(ResourceDropToken) -> T,
    ) -> (GpuImageHandle, Arc<T>) {
        let gpu_usage = gpu_usage_from_wgpu(usage);
        let mut handle =
            GpuImageHandle::new(gpu_format, width, height, GpuMemoryLocation::Gpu, gpu_usage);
        self.resources.textures.lock().insert(handle.id, texture);
        let token = Arc::new(wrap(ResourceDropToken {
            kind: ResourceKind::Texture {
                id: handle.id,
                recycle: None,
            },
            resources: Arc::downgrade(&self.resources),
        }));
        handle.drop_token = Some(token.clone() as Arc<dyn GpuDropToken>);
        (handle, token)
    }
}

impl Drop for WgpuBackend {
    fn drop(&mut self) {
        crate::shader::clear_pipeline_caches_for_device(self.device_key);
        crate::shader::clear_temp_pool_for_device(self.device_key);
        crate::shader::clear_gpu_state_pool_for_device(self.device_key);
        crate::shader::unregister_device(&self.device, self.device_key);
    }
}

impl GpuBackend for WgpuBackend {
    fn kind(&self) -> GpuBackendKind {
        GpuBackendKind::Wgpu
    }

    fn is_lost(&self) -> bool {
        self.lost.load(std::sync::atomic::Ordering::Acquire)
    }

    fn lost_reason(&self) -> Option<String> {
        self.lost_reason.lock().clone()
    }

    fn mark_lost(&self, reason: String) {
        *self.lost_reason.lock() = Some(reason);
        self.lost.store(true, std::sync::atomic::Ordering::Release);
    }

    fn take_uncaptured_error(&self) -> Option<String> {
        self.pending.take()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn adapter_info(&self) -> GpuAdapterInfo {
        self.adapter.clone()
    }

    fn capabilities(&self) -> GpuCapabilities {
        self.caps.clone()
    }

    fn select_adapter(&self, _opts: &GpuOptions) -> Result<GpuAdapterInfo, GpuError> {
        Ok(self.adapter.clone())
    }

    fn wgpu_device_queue(&self) -> Option<(&wgpu::Device, &wgpu::Queue)> {
        Some((&self.device, self.queue.raw()))
    }

    fn wgpu_submit_lock(&self) -> Option<&SubmitLock> {
        Some(self.queue.submit_lock())
    }

    fn dmabuf_import_support(&self) -> crate::ExternalImportSupport {
        self.dmabuf_support.clone()
    }

    #[cfg(target_os = "linux")]
    fn import_dmabuf(
        &self,
        desc: crate::ExternalFrameDescriptor,
    ) -> Result<GpuImageHandle, crate::ExternalImportError> {
        if let Some(reason) = self.dmabuf_support.reason() {
            return Err(crate::ExternalImportError::Unsupported {
                reason: reason.to_string(),
            });
        }
        dmabuf::import(self, desc)
    }

    fn wgpu_submission_tracker(&self) -> Option<&SubmissionTracker> {
        Some(&self.submission_tracker)
    }

    fn wgpu_device_key(&self) -> Option<usize> {
        Some(self.device_key)
    }

    fn wgpu_get_texture(&self, handle: &GpuImageHandle) -> Option<std::sync::Arc<wgpu::Texture>> {
        self.get_texture(handle)
    }

    fn wgpu_register_texture(
        &self,
        texture: std::sync::Arc<wgpu::Texture>,
        format: wgpu::TextureFormat,
        width: u32,
        height: u32,
        usage: wgpu::TextureUsages,
    ) -> Result<Option<GpuImageHandle>, GpuError> {
        self.register_texture(texture, format, width, height, usage)
            .map(Some)
    }

    fn create_buffer(&self, req: &GpuRequest) -> Result<GpuBufferHandle, GpuError> {
        if req.size_bytes > self.caps.max_buffer_size {
            return Err(GpuError::AllocationFailed);
        }
        if req.usage.is_empty() {
            return Err(GpuError::Unsupported);
        }
        {
            let mut stats = self.stats_guard();
            stats.record_upload(req.size_bytes);
        }
        let mut handle = GpuBufferHandle::new(req.size_bytes, GpuMemoryLocation::Gpu, req.usage);
        let usage = map_usage(req.usage);
        let buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some(&format!("buf-{}", handle.id.0)),
            size: req.size_bytes,
            usage,
            mapped_at_creation: false,
        });
        self.resources.buffers.lock().insert(handle.id, buffer);
        handle.drop_token = Some(Arc::new(ResourceDropToken {
            kind: ResourceKind::Buffer(handle.id),
            resources: Arc::downgrade(&self.resources),
        }) as Arc<dyn GpuDropToken>);
        Ok(handle)
    }

    fn create_image(&self, req: &GpuImageRequest) -> Result<GpuImageHandle, GpuError> {
        if req.width > self.caps.max_texture_dimension
            || req.height > self.caps.max_texture_dimension
        {
            return Err(GpuError::AllocationFailed);
        }
        let features = self
            .caps
            .format_features
            .iter()
            .find(|f| f.format == req.format)
            .ok_or(GpuError::Unsupported)?;
        if req.usage.contains(GpuUsage::RENDER_TARGET) && !features.renderable {
            return Err(GpuError::Unsupported);
        }
        if req.usage.contains(GpuUsage::STORAGE) && !features.storage {
            return Err(GpuError::Unsupported);
        }
        if req.samples > features.max_samples {
            return Err(GpuError::Unsupported);
        }
        if req.usage.is_empty() {
            return Err(GpuError::Unsupported);
        }
        let bpp = crate::format_bytes_per_pixel(req.format).ok_or(GpuError::Unsupported)? as u64;
        let bytes = (req.width as u64) * (req.height as u64) * bpp;
        {
            let mut stats = self.stats_guard();
            stats.record_upload(bytes);
        }
        let mut usage = map_texture_usage(req.usage);
        // Many example pipelines need to sample from uploaded textures and/or read them back.
        // Add permissive defaults to avoid wgpu validation errors when a texture is later used
        // as a bindable resource or copy source.
        usage |= wgpu::TextureUsages::COPY_SRC;
        if features.sampleable {
            usage |= wgpu::TextureUsages::TEXTURE_BINDING;
        }
        let format = map_format(req.format);
        let texture = self.device.create_texture(&wgpu::TextureDescriptor {
            label: Some("texture"),
            size: wgpu::Extent3d {
                width: req.width,
                height: req.height,
                depth_or_array_layers: 1,
            },
            mip_level_count: 1,
            sample_count: req.samples,
            dimension: wgpu::TextureDimension::D2,
            format,
            usage,
            view_formats: &[],
        });
        let mut handle = GpuImageHandle::new(
            req.format,
            req.width,
            req.height,
            GpuMemoryLocation::Gpu,
            req.usage,
        );
        self.resources
            .textures
            .lock()
            .insert(handle.id, Arc::new(texture));
        handle.drop_token = Some(Arc::new(ResourceDropToken {
            kind: ResourceKind::Texture {
                id: handle.id,
                recycle: None,
            },
            resources: Arc::downgrade(&self.resources),
        }) as Arc<dyn GpuDropToken>);
        Ok(handle)
    }

    fn stats(&self) -> TransferStats {
        *self.stats_guard()
    }

    fn take_stats(&self) -> TransferStats {
        self.stats_guard().take()
    }

    fn record_download(&self, bytes: u64) {
        let mut stats = self.stats_guard();
        stats.record_download(bytes);
    }

    fn upload_texture(
        &self,
        req: &GpuImageRequest,
        data: &[u8],
    ) -> Result<GpuImageHandle, GpuError> {
        validate_texture_bytes(req, &self.caps)?;
        let handle = self.create_image(req)?;
        if let Some(tex) = self.resources.textures.lock().get(&handle.id).cloned() {
            let bpp = format_bytes_per_pixel(req.format).ok_or(GpuError::Unsupported)?;
            let bytes_per_row = req.width.saturating_mul(bpp);
            let expected = (bytes_per_row as usize).saturating_mul(req.height as usize);
            if data.len() != expected {
                return Err(GpuError::AllocationFailed);
            }
            let align = self.caps.bytes_per_row_alignment.max(1);
            let padded_bpr = bytes_per_row.div_ceil(align) * align;

            let staged;
            let data = if padded_bpr == bytes_per_row {
                data
            } else {
                let mut tmp = vec![0u8; (padded_bpr as usize).saturating_mul(req.height as usize)];
                let row = bytes_per_row as usize;
                daedalus_kernels::copy_rows(
                    data,
                    row,
                    &mut tmp,
                    padded_bpr as usize,
                    row,
                    req.height as usize,
                );
                staged = tmp;
                &staged
            };

            let layout = wgpu::TexelCopyBufferLayout {
                offset: 0,
                bytes_per_row: Some(padded_bpr),
                rows_per_image: Some(req.height),
            };
            let size = wgpu::Extent3d {
                width: req.width,
                height: req.height,
                depth_or_array_layers: 1,
            };
            self.queue.raw().write_texture(
                wgpu::TexelCopyTextureInfo {
                    texture: &tex,
                    mip_level: 0,
                    origin: wgpu::Origin3d::ZERO,
                    aspect: wgpu::TextureAspect::All,
                },
                data,
                layout,
                size,
            );
        }
        Ok(handle)
    }

    fn read_texture(&self, handle: &GpuImageHandle) -> Result<Vec<u8>, GpuError> {
        let tex = self.texture_for_readback(handle)?;
        let (bytes_per_row, padded_bpr) = self.texture_readback_layout(handle)?;
        let size = padded_bpr as u64 * handle.height as u64;
        let staging = self.take_staging_buffer(size, "tex-readback");
        let _guard = self.copy_limiter.acquire();
        self.submit_texture_readback(&tex, &staging, handle, padded_bpr);
        let slice = staging.slice(..staging_size(size));
        let (tx, rx) = std::sync::mpsc::channel();
        self.queue
            .map_async(slice, wgpu::MapMode::Read, move |res| {
                let _ = tx.send(res);
            });
        // Synchronous texture readback is a compatibility API and may block the
        // current thread. Async runtimes should use `GpuAsyncBackend` readback
        // methods with the `gpu-async` feature instead.
        let _ = self.device.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });
        rx.recv()
            .map_err(|err| GpuError::Internal(format!("texture map canceled: {err}")))?
            .map_err(|err| GpuError::Internal(format!("texture map failed: {err:?}")))?;
        let data = {
            let mapped = slice.get_mapped_range()?;
            unpad_rows(&mapped, bytes_per_row, padded_bpr, handle.height as usize)
        };
        staging.unmap();
        self.record_download(data.len() as u64);
        self.return_staging_buffer(staging);
        Ok(data)
    }
}

#[cfg(all(feature = "gpu-async", feature = "gpu-wgpu"))]
#[async_trait]
impl crate::GpuAsyncBackend for WgpuBackend {
    async fn upload_buffer(
        &self,
        req: &GpuRequest,
        data: &[u8],
    ) -> Result<GpuBufferHandle, GpuError> {
        let _guard = self.copy_limiter.acquire_async().await;
        let handle = self.create_buffer(req)?;
        if let Some(buf) = self.resources.buffers.lock().get(&handle.id).cloned() {
            self.queue.raw().write_buffer(&buf, 0, data);
        }
        Ok(handle)
    }

    async fn read_buffer(&self, handle: &GpuBufferHandle) -> Result<Vec<u8>, GpuError> {
        let buf = self
            .resources
            .buffers
            .lock()
            .get(&handle.id)
            .cloned()
            .ok_or(GpuError::Unsupported)?;
        let staging = self.take_staging_buffer(handle.size_bytes, "readback");
        let _guard = self.copy_limiter.acquire_async().await;
        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("readback-encoder"),
            });
        encoder.copy_buffer_to_buffer(&buf, 0, &staging, 0, handle.size_bytes);
        self.queue.submit(Some(encoder.finish()));
        let buffer_slice = staging.slice(..staging_size(handle.size_bytes));
        crate::shader::map_read_async(&self.device, &self.queue, buffer_slice)
            .await
            .map_err(|err| GpuError::Internal(format!("map failed: {err}")))?;
        let data = {
            let mapped = buffer_slice.get_mapped_range()?;
            mapped[..handle.size_bytes as usize].to_vec()
        };
        staging.unmap();
        self.record_download(data.len() as u64);
        self.return_staging_buffer(staging);
        Ok(data)
    }

    async fn upload_texture_async(
        &self,
        req: &GpuImageRequest,
        data: &[u8],
    ) -> Result<GpuImageHandle, GpuError> {
        let _guard = self.copy_limiter.acquire_async().await;
        self.upload_texture(req, data)
    }

    async fn read_texture_async(&self, handle: &GpuImageHandle) -> Result<Vec<u8>, GpuError> {
        let tex = self.texture_for_readback(handle)?;
        let (bytes_per_row, padded_bpr) = self.texture_readback_layout(handle)?;
        let size = padded_bpr as u64 * handle.height as u64;
        let staging = self.take_staging_buffer(size, "tex-readback");
        let _guard = self.copy_limiter.acquire_async().await;
        self.submit_texture_readback(&tex, &staging, handle, padded_bpr);
        let buffer_slice = staging.slice(..staging_size(size));
        crate::shader::map_read_async(&self.device, &self.queue, buffer_slice)
            .await
            .map_err(|err| GpuError::Internal(format!("texture map failed: {err}")))?;
        let data = {
            let mapped = buffer_slice.get_mapped_range()?;
            unpad_rows(&mapped, bytes_per_row, padded_bpr, handle.height as usize)
        };
        staging.unmap();
        self.record_download(data.len() as u64);
        self.return_staging_buffer(staging);
        Ok(data)
    }
}

#[cfg(test)]
#[path = "wgpu_backend/tests.rs"]
mod tests;
