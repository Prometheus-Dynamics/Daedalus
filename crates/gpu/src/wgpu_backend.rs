use std::sync::{Arc, Mutex, MutexGuard};

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
use wgpu::{Adapter, Backends, Features, Instance, InstanceDescriptor, Limits};

mod adapter_select;
mod capabilities;
mod copy_limiter;
mod resources;
mod staging;

use adapter_select::{preferred_backends, select_best_adapter};
use capabilities::{build_info_from_adapter, caps_from_adapter};
use copy_limiter::CopyLimiter;
use resources::{ResourceDropToken, ResourceKind, WgpuResources};
use staging::StagingPool;
pub use staging::{WgpuStagingPoolConfig, WgpuStagingPoolStats};

/// Minimal wgpu backend placeholder to satisfy trait; queries adapter limits when available.
pub struct WgpuBackend {
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
        let preferred_backends = preferred_backends();
        let mut instance_desc = InstanceDescriptor::new_without_display_handle();
        instance_desc.backends = preferred_backends;
        let instance = Instance::new(instance_desc);
        let mut adapters: Vec<Adapter> = instance.enumerate_adapters(preferred_backends).await;
        if adapters.is_empty() && preferred_backends != Backends::all() {
            adapters = instance.enumerate_adapters(Backends::all()).await;
        }
        let adapter = select_best_adapter(adapters).ok_or(GpuError::AdapterUnavailable)?;

        let (info, features, limits) = build_info_from_adapter(&adapter);
        let caps = caps_from_adapter(Some(&adapter), &limits);

        let (device, queue) = adapter
            .request_device(&wgpu::DeviceDescriptor {
                label: Some("wgpu-backend"),
                required_features: Features::empty(),
                required_limits: adapter.limits(),
                experimental_features: wgpu::ExperimentalFeatures::disabled(),
                memory_hints: wgpu::MemoryHints::default(),
                trace: wgpu::Trace::default(),
            })
            .await
            .map_err(|err| GpuError::Internal(format!("wgpu device request failed: {err}")))?;

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
        })
    }

    pub(crate) fn device_queue(&self) -> (&wgpu::Device, &wgpu::Queue) {
        (&self.device, &self.queue)
    }

    pub fn staging_pool_stats(&self) -> WgpuStagingPoolStats {
        self.staging_pool
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .stats()
    }

    fn stats_guard(&self) -> MutexGuard<'_, TransferStats> {
        self.stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn get_texture(
        &self,
        handle: &GpuImageHandle,
    ) -> Option<std::sync::Arc<wgpu::Texture>> {
        match self.resources.textures.lock() {
            Ok(textures) => textures.get(&handle.id).cloned(),
            Err(poisoned) => {
                tracing::warn!(
                    target: "daedalus_gpu::wgpu",
                    texture_id = %handle.id,
                    "wgpu texture registry lock poisoned while looking up texture"
                );
                poisoned.into_inner().get(&handle.id).cloned()
            }
        }
    }

    pub(crate) fn register_texture(
        &self,
        texture: std::sync::Arc<wgpu::Texture>,
        format: wgpu::TextureFormat,
        width: u32,
        height: u32,
        usage: wgpu::TextureUsages,
    ) -> GpuImageHandle {
        let gpu_format = match format {
            wgpu::TextureFormat::R8Unorm => GpuFormat::R8Unorm,
            wgpu::TextureFormat::Rgba8Unorm => GpuFormat::Rgba8Unorm,
            wgpu::TextureFormat::Rgba16Float => GpuFormat::Rgba16Float,
            _ => GpuFormat::Rgba8Unorm,
        };
        let mut gpu_usage = GpuUsage::empty();
        if usage.contains(wgpu::TextureUsages::STORAGE_BINDING) {
            gpu_usage |= GpuUsage::STORAGE;
        }
        if usage.contains(wgpu::TextureUsages::RENDER_ATTACHMENT) {
            gpu_usage |= GpuUsage::RENDER_TARGET;
        }
        if usage.contains(wgpu::TextureUsages::COPY_DST) {
            gpu_usage |= GpuUsage::UPLOAD;
        }
        if usage.contains(wgpu::TextureUsages::COPY_SRC) {
            gpu_usage |= GpuUsage::DOWNLOAD;
        }
        let mut handle =
            GpuImageHandle::new(gpu_format, width, height, GpuMemoryLocation::Gpu, gpu_usage);
        match self.resources.textures.lock() {
            Ok(mut map) => {
                map.insert(handle.id, texture);
            }
            Err(poisoned) => {
                tracing::warn!(
                    target: "daedalus_gpu::wgpu",
                    texture_id = %handle.id,
                    "wgpu texture registry lock poisoned while registering texture"
                );
                poisoned.into_inner().insert(handle.id, texture);
            }
        }
        handle.drop_token = Some(Arc::new(ResourceDropToken {
            kind: ResourceKind::Texture {
                id: handle.id,
                recycle: None,
            },
            resources: Arc::downgrade(&self.resources),
        }) as Arc<dyn GpuDropToken>);
        handle
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
    ) -> Option<GpuImageHandle> {
        Some(self.register_texture(texture, format, width, height, usage))
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
        self.resources
            .buffers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(handle.id, buffer);
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
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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
        if let Some(tex) = self
            .resources
            .textures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&handle.id)
            .cloned()
        {
            let bpp = format_bytes_per_pixel(req.format).ok_or(GpuError::Unsupported)? as u32;
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
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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
        let raw = slice.get_mapped_range().to_vec();
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

fn map_usage(usage: GpuUsage) -> wgpu::BufferUsages {
    let mut u = wgpu::BufferUsages::empty();
    if usage.contains(GpuUsage::UPLOAD) {
        u |= wgpu::BufferUsages::COPY_SRC | wgpu::BufferUsages::COPY_DST;
    }
    if usage.contains(GpuUsage::DOWNLOAD) {
        u |= wgpu::BufferUsages::COPY_SRC
            | wgpu::BufferUsages::COPY_DST
            | wgpu::BufferUsages::MAP_READ;
    }
    if usage.contains(GpuUsage::STORAGE) {
        u |= wgpu::BufferUsages::STORAGE
            | wgpu::BufferUsages::COPY_SRC
            | wgpu::BufferUsages::COPY_DST;
    }
    if u.is_empty() {
        u = wgpu::BufferUsages::COPY_SRC | wgpu::BufferUsages::COPY_DST;
    }
    u
}

fn map_texture_usage(usage: GpuUsage) -> wgpu::TextureUsages {
    let mut u = wgpu::TextureUsages::empty();
    if usage.contains(GpuUsage::RENDER_TARGET) {
        u |= wgpu::TextureUsages::RENDER_ATTACHMENT;
    }
    if usage.contains(GpuUsage::UPLOAD) {
        u |= wgpu::TextureUsages::COPY_DST;
    }
    if usage.contains(GpuUsage::DOWNLOAD) {
        u |= wgpu::TextureUsages::COPY_SRC;
    }
    if usage.contains(GpuUsage::STORAGE) {
        u |= wgpu::TextureUsages::STORAGE_BINDING
            | wgpu::TextureUsages::COPY_SRC
            | wgpu::TextureUsages::COPY_DST;
    }
    u
}

fn map_format(format: GpuFormat) -> wgpu::TextureFormat {
    match format {
        GpuFormat::R8Unorm => wgpu::TextureFormat::R8Unorm,
        GpuFormat::Rgba8Unorm => wgpu::TextureFormat::Rgba8Unorm,
        GpuFormat::Rgba16Float => wgpu::TextureFormat::Rgba16Float,
        GpuFormat::Depth24Stencil8 => wgpu::TextureFormat::Depth24PlusStencil8,
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
        if let Some(buf) = self
            .resources
            .buffers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&handle.id)
            .cloned()
        {
            self.queue.write_buffer(&buf, 0, data);
        }
        Ok(handle)
    }

    async fn read_buffer(&self, handle: &GpuBufferHandle) -> Result<Vec<u8>, GpuError> {
        let buf = self
            .resources
            .buffers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&handle.id)
            .cloned()
            .ok_or(GpuError::Unsupported)?;
        // Staging reuse
        let staging = {
            let mut pool = self
                .staging_pool
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
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
        let data = buffer_slice.get_mapped_range().to_vec();
        staging.unmap();
        self.record_download(data.len() as u64);
        // Return staging to pool
        {
            let mut pool = self
                .staging_pool
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            pool.put(handle.size_bytes, staging);
        }
        Ok(data)
    }
}

#[cfg(test)]
#[path = "wgpu_backend/tests.rs"]
mod tests;
