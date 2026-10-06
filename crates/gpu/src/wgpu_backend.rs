use parking_lot::{Mutex, MutexGuard};
use std::sync::Arc;

use crate::handles::{GpuBufferHandle, GpuDropToken, GpuImageHandle};
use crate::shader::SubmissionTracker;
use crate::traits::GpuBackend;
use crate::{
    GpuAdapterInfo, GpuBackendKind, GpuCapabilities, GpuError, GpuFormat, GpuImageRequest,
    GpuMemoryLocation, GpuOptions, GpuRequest, GpuUsage, buffer::TransferStats,
    format_bytes_per_pixel, validate_texture_bytes,
};
#[cfg(all(feature = "gpu-async", feature = "gpu-wgpu"))]
use async_trait::async_trait;
use pollster::FutureExt;
use wgpu::{Adapter, Backends, Features, Instance, Limits};

mod adapter_select;
mod capabilities;
mod copy_limiter;
mod dmabuf;
mod driver;
mod mapping;
mod resources;
mod staging;

use adapter_select::{preferred_backends, select_best_adapter};
use capabilities::{build_info_from_adapter, caps_from_adapter};
use copy_limiter::CopyLimiter;
pub use dmabuf::texture_plane_views;
pub(crate) use driver::{driver_lock, instance_descriptor};
pub(crate) use mapping::{gpu_format_from_wgpu, map_format};
use mapping::{gpu_usage_from_wgpu, map_texture_usage, map_usage};
use resources::{ResourceDropToken, ResourceKind, WgpuResources};
use staging::StagingPool;
pub use staging::{WgpuStagingPoolConfig, WgpuStagingPoolStats};

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
    queue: wgpu::Queue,
    resources: Arc<WgpuResources>,
    staging_pool: Mutex<StagingPool>,
    copy_limiter: CopyLimiter,
    submission_tracker: SubmissionTracker,
    device_key: usize,
    dmabuf_support: crate::ExternalImportSupport,
}

impl WgpuBackend {
    /// Create a wgpu backend using the synchronous compatibility path.
    ///
    /// This blocks while wgpu enumerates adapters and requests a device. Async callers should use
    /// [`WgpuBackend::new_async`] to avoid blocking executor threads.
    pub fn new() -> Result<Self, GpuError> {
        let res = std::panic::catch_unwind(|| Self::new_async().block_on());

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
        let staging_config = WgpuStagingPoolConfig::from_env().map_err(GpuError::Internal)?;
        Self::new_with_staging_pool_config_async(staging_config).await
    }

    pub async fn new_with_staging_pool_config_async(
        staging_config: WgpuStagingPoolConfig,
    ) -> Result<Self, GpuError> {
        let adapter = {
            let _driver = driver_lock();
            let preferred_backends = preferred_backends();
            let instance = Instance::new(instance_descriptor(preferred_backends));
            let mut adapters: Vec<Adapter> = instance.enumerate_adapters(preferred_backends).await;
            if adapters.is_empty() && preferred_backends != Backends::all() {
                adapters = instance.enumerate_adapters(Backends::all()).await;
            }
            select_best_adapter(adapters)
        };
        let adapter = adapter.ok_or(GpuError::AdapterUnavailable)?;
        Self::with_adapter(adapter, staging_config).await
    }

    /// Create the backend on a specific adapter.
    pub(crate) async fn with_adapter(
        adapter: Adapter,
        staging_config: WgpuStagingPoolConfig,
    ) -> Result<Self, GpuError> {
        let (info, features, limits) = build_info_from_adapter(&adapter);
        let caps = caps_from_adapter(Some(&adapter), &limits);

        let device_desc = wgpu::DeviceDescriptor {
            label: Some("wgpu-backend"),
            required_features: Features::empty(),
            required_limits: adapter.limits(),
            experimental_features: wgpu::ExperimentalFeatures::disabled(),
            memory_hints: wgpu::MemoryHints::default(),
            trace: wgpu::Trace::default(),
        };
        // With `gpu-dmabuf` on Linux/Vulkan this also enables the dmabuf import extensions.
        let driver = driver_lock();
        let (device, queue) = dmabuf::request_device(&adapter, &device_desc)
            .await
            .map_err(|err| GpuError::Internal(format!("wgpu device request failed: {err}")))?;
        drop(driver);
        let (dmabuf_support, dmabuf) = dmabuf::probe(&device, &queue);

        let device_key = crate::shader::register_device(&device);

        // Avoid process-level panics on uncaptured backend errors (OOM/validation).
        // We still surface the error for diagnostics, but keep the runtime alive.
        device.on_uncaptured_error(std::sync::Arc::new(|err| {
            tracing::error!(
                target: "daedalus_gpu::wgpu",
                error = %err,
                "uncaptured wgpu error"
            );
        }));

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
        })
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

    pub(crate) fn device_queue(&self) -> (&wgpu::Device, &wgpu::Queue) {
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
        Some((&self.device, &self.queue))
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
                let src_stride = bytes_per_row as usize;
                let dst_stride = padded_bpr as usize;
                for row in 0..(req.height as usize) {
                    let src = row * src_stride;
                    let dst = row * dst_stride;
                    tmp[dst..dst + src_stride].copy_from_slice(&data[src..src + src_stride]);
                }
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
            self.queue.write_texture(
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
        let tex = self
            .resources
            .textures
            .lock()
            .get(&handle.id)
            .cloned()
            .ok_or(GpuError::Unsupported)?;
        let bpp = format_bytes_per_pixel(handle.format).ok_or(GpuError::Unsupported)? as u64;
        let bytes_per_row = handle.width as u64 * bpp;
        let align = self.caps.bytes_per_row_alignment.max(1) as u64;
        let padded_bpr = bytes_per_row.div_ceil(align) * align;
        let size_bytes = padded_bpr * handle.height as u64;
        let staging = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("tex-readback"),
            size: size_bytes,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        let _guard = self.copy_limiter.acquire();
        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("tex-readback-encoder"),
            });
        encoder.copy_texture_to_buffer(
            wgpu::TexelCopyTextureInfo {
                texture: &tex,
                mip_level: 0,
                origin: wgpu::Origin3d::ZERO,
                aspect: wgpu::TextureAspect::All,
            },
            wgpu::TexelCopyBufferInfo {
                buffer: &staging,
                layout: wgpu::TexelCopyBufferLayout {
                    offset: 0,
                    bytes_per_row: Some(padded_bpr as u32),
                    rows_per_image: Some(handle.height),
                },
            },
            wgpu::Extent3d {
                width: handle.width,
                height: handle.height,
                depth_or_array_layers: 1,
            },
        );
        self.queue.submit(Some(encoder.finish()));
        let slice = staging.slice(..);
        let (tx, rx) = std::sync::mpsc::channel();
        slice.map_async(wgpu::MapMode::Read, move |res| {
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
        let raw = slice.get_mapped_range()?.to_vec();
        staging.unmap();
        let data = if padded_bpr == bytes_per_row {
            raw
        } else {
            let mut tight =
                vec![0u8; (bytes_per_row as usize).saturating_mul(handle.height as usize)];
            let src_stride = padded_bpr as usize;
            let dst_stride = bytes_per_row as usize;
            for row in 0..(handle.height as usize) {
                let src = row * src_stride;
                let dst = row * dst_stride;
                tight[dst..dst + dst_stride].copy_from_slice(&raw[src..src + dst_stride]);
            }
            tight
        };
        self.record_download(data.len() as u64);
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
            self.queue.write_buffer(&buf, 0, data);
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
        // Staging reuse
        let staging = {
            let mut pool = self.staging_pool.lock();
            pool.take(handle.size_bytes).unwrap_or_else(|| {
                self.device.create_buffer(&wgpu::BufferDescriptor {
                    label: Some("readback"),
                    size: handle.size_bytes,
                    usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
                    mapped_at_creation: false,
                })
            })
        };
        let _guard = self.copy_limiter.acquire_async().await;
        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("readback-encoder"),
            });
        encoder.copy_buffer_to_buffer(&buf, 0, &staging, 0, handle.size_bytes);
        self.queue.submit(Some(encoder.finish()));
        let buffer_slice = staging.slice(..);
        crate::shader::map_read_async(&self.device, buffer_slice)
            .await
            .map_err(|err| GpuError::Internal(format!("map failed: {err}")))?;
        let data = buffer_slice.get_mapped_range()?.to_vec();
        staging.unmap();
        self.record_download(data.len() as u64);
        // Return staging to pool
        {
            let mut pool = self.staging_pool.lock();
            pool.put(handle.size_bytes, staging);
        }
        Ok(data)
    }
}

#[cfg(test)]
#[path = "wgpu_backend/tests.rs"]
mod tests;
