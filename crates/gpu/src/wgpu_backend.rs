use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};

use crate::handles::{GpuBufferHandle, GpuBufferId, GpuDropToken, GpuImageHandle, GpuImageId};
use crate::traits::GpuBackend;
use crate::{
    GpuAdapterInfo, GpuBackendKind, GpuBlockInfo, GpuCapabilities, GpuError, GpuFormat,
    GpuFormatFeatures, GpuImageRequest, GpuMemoryLocation, GpuOptions, GpuRequest, GpuUsage,
    buffer::TransferStats, format_bytes_per_pixel, validate_texture_bytes,
};
#[cfg(all(feature = "gpu-async", feature = "gpu-wgpu"))]
use async_trait::async_trait;
use pollster::FutureExt;
use wgpu::{Adapter, Backends, Features, Instance, InstanceDescriptor, Limits};

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
    _staging_pool: Mutex<HashMap<u64, Vec<wgpu::Buffer>>>,
    copy_limiter: CopyLimiter,
}

#[derive(Debug, Default)]
struct WgpuResources {
    buffers: Mutex<HashMap<GpuBufferId, wgpu::Buffer>>,
    textures: Mutex<HashMap<GpuImageId, Arc<wgpu::Texture>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResourceKind {
    Buffer(GpuBufferId),
    Texture {
        id: GpuImageId,
        recycle: Option<TextureRecycleMeta>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TextureRecycleMeta {
    device_key: usize,
    width: u32,
    height: u32,
    format: wgpu::TextureFormat,
    usage: wgpu::TextureUsages,
}

#[derive(Debug)]
struct ResourceDropToken {
    kind: ResourceKind,
    resources: Weak<WgpuResources>,
}

impl Drop for ResourceDropToken {
    fn drop(&mut self) {
        let Some(resources) = self.resources.upgrade() else {
            return;
        };
        match self.kind {
            ResourceKind::Buffer(id) => {
                if let Ok(mut buffers) = resources.buffers.lock() {
                    buffers.remove(&id);
                }
            }
            ResourceKind::Texture { id, recycle } => {
                if let Ok(mut textures) = resources.textures.lock() {
                    let texture = textures.remove(&id);
                    if let (Some(texture), Some(meta)) = (texture, recycle)
                        && let Ok(mut pool) = crate::shader::temp_pool().lock()
                    {
                        pool.put_texture(
                            meta.device_key,
                            meta.width,
                            meta.height,
                            meta.format,
                            meta.usage,
                            texture,
                        );
                    }
                }
            }
        }
    }
}

impl WgpuBackend {
    pub fn new() -> Result<Self, GpuError> {
        let res = std::panic::catch_unwind(|| {
            #[allow(clippy::if_same_then_else)]
            let preferred_backends = Backends::from_env().unwrap_or_else(|| {
                #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
                {
                    // Headless Linux ARM targets (CM5) are significantly more stable with Vulkan.
                    Backends::VULKAN
                }
                #[cfg(not(all(target_os = "linux", target_arch = "aarch64")))]
                {
                    Backends::all()
                }
            });
            let instance = Instance::new(&InstanceDescriptor {
                backends: preferred_backends,
                ..Default::default()
            });
            let mut adapters: Vec<Adapter> =
                instance.enumerate_adapters(preferred_backends).block_on();
            if adapters.is_empty() && preferred_backends != Backends::all() {
                adapters = instance.enumerate_adapters(Backends::all()).block_on();
            }
            let adapter = match select_best_adapter(adapters) {
                Some(a) => a,
                None => return Err(GpuError::AdapterUnavailable),
            };

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
                .block_on()
                .expect("wgpu device");

            // Defensive reset: if a prior backend/device died and was recreated, pointer-based
            // keys can alias stale pooled textures. Start each backend with a clean temp pool.
            crate::shader::clear_temp_pool();

            // Avoid process-level panics on uncaptured backend errors (OOM/validation).
            // We still surface the error for diagnostics, but keep the runtime alive.
            device.on_uncaptured_error(std::sync::Arc::new(|err| {
                eprintln!("daedalus-gpu: uncaptured wgpu error: {err}");
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
                _staging_pool: Mutex::new(HashMap::new()),
                copy_limiter: CopyLimiter::new(caps.max_inflight_copies.max(1)),
            })
        });

        match res {
            Ok(ok) => ok,
            Err(_) => Err(GpuError::AdapterUnavailable),
        }
    }

    pub(crate) fn device_queue(&self) -> (&wgpu::Device, &wgpu::Queue) {
        (&self.device, &self.queue)
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
        self.resources
            .textures
            .lock()
            .ok()?
            .get(&handle.id)
            .cloned()
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
        if let Ok(mut map) = self.resources.textures.lock() {
            map.insert(handle.id, texture);
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

fn select_best_adapter(adapters: Vec<Adapter>) -> Option<Adapter> {
    adapters.into_iter().max_by_key(adapter_score)
}

fn adapter_score(adapter: &Adapter) -> i64 {
    let info = adapter.get_info();
    let mut score: i64 = match info.backend {
        wgpu::Backend::Vulkan => 500,
        wgpu::Backend::Metal => 450,
        wgpu::Backend::Dx12 => 400,
        wgpu::Backend::Gl => 250,
        wgpu::Backend::BrowserWebGpu => 150,
        wgpu::Backend::Noop => 0,
    };

    score += match info.device_type {
        wgpu::DeviceType::DiscreteGpu => 120,
        wgpu::DeviceType::IntegratedGpu => 90,
        wgpu::DeviceType::VirtualGpu => 40,
        wgpu::DeviceType::Cpu => -300,
        wgpu::DeviceType::Other => 0,
    };

    // Strongly avoid software adapters when possible.
    let lower_name = info.name.to_ascii_lowercase();
    if lower_name.contains("llvmpipe")
        || lower_name.contains("lavapipe")
        || lower_name.contains("softpipe")
    {
        score -= 800;
    }

    // Prefer adapters that can run our storage-heavy image pipeline.
    let rgba = adapter.get_texture_format_features(wgpu::TextureFormat::Rgba8Unorm);
    if rgba
        .allowed_usages
        .contains(wgpu::TextureUsages::STORAGE_BINDING)
    {
        score += 90;
    } else {
        score -= 200;
    }
    if rgba
        .allowed_usages
        .contains(wgpu::TextureUsages::TEXTURE_BINDING)
    {
        score += 20;
    } else {
        score -= 50;
    }
    if rgba.allowed_usages.contains(wgpu::TextureUsages::COPY_SRC)
        && rgba.allowed_usages.contains(wgpu::TextureUsages::COPY_DST)
    {
        score += 20;
    } else {
        score -= 120;
    }

    score
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
        if self.caps.staging_alignment > 0
            && !req.size_bytes.is_multiple_of(self.caps.staging_alignment)
        {
            return Err(GpuError::AllocationFailed);
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
            .expect("buffers lock")
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
            .expect("textures lock")
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
            .expect("textures lock")
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
            .expect("textures lock")
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
        slice.map_async(wgpu::MapMode::Read, |_| {});
        let _ = self.device.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });
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

fn caps_from_adapter(adapter: Option<&Adapter>, limits: &Limits) -> GpuCapabilities {
    let formats = [
        GpuFormat::R8Unorm,
        GpuFormat::Rgba8Unorm,
        GpuFormat::Rgba16Float,
        GpuFormat::Depth24Stencil8,
    ];
    let mut format_features = Vec::new();
    for format in formats {
        let (sampleable, renderable, storage, max_samples) = if let Some(adapter) = adapter {
            let tf = match format {
                GpuFormat::R8Unorm => wgpu::TextureFormat::R8Unorm,
                GpuFormat::Rgba8Unorm => wgpu::TextureFormat::Rgba8Unorm,
                GpuFormat::Rgba16Float => wgpu::TextureFormat::Rgba16Float,
                GpuFormat::Depth24Stencil8 => wgpu::TextureFormat::Depth24PlusStencil8,
            };
            let features = adapter.get_texture_format_features(tf);
            let allowed = features.allowed_usages;
            let flags = features.flags;
            let max_samples = if flags.contains(wgpu::TextureFormatFeatureFlags::MULTISAMPLE_X8) {
                8
            } else if flags.contains(wgpu::TextureFormatFeatureFlags::MULTISAMPLE_X4) {
                4
            } else if flags.contains(wgpu::TextureFormatFeatureFlags::MULTISAMPLE_X2) {
                2
            } else {
                1
            };
            (
                allowed.contains(wgpu::TextureUsages::TEXTURE_BINDING),
                allowed.contains(wgpu::TextureUsages::RENDER_ATTACHMENT),
                allowed.contains(wgpu::TextureUsages::STORAGE_BINDING),
                max_samples,
            )
        } else {
            (true, true, format != GpuFormat::Depth24Stencil8, 8)
        };
        format_features.push(GpuFormatFeatures {
            format,
            sampleable,
            renderable,
            storage,
            max_samples,
        });
    }

    GpuCapabilities {
        supported_formats: formats.to_vec(),
        format_features,
        format_blocks: vec![
            GpuBlockInfo {
                format: GpuFormat::R8Unorm,
                block_width: 1,
                block_height: 1,
                bytes_per_block: 1,
            },
            GpuBlockInfo {
                format: GpuFormat::Rgba8Unorm,
                block_width: 1,
                block_height: 1,
                bytes_per_block: 4,
            },
            GpuBlockInfo {
                format: GpuFormat::Rgba16Float,
                block_width: 1,
                block_height: 1,
                bytes_per_block: 8,
            },
            GpuBlockInfo {
                format: GpuFormat::Depth24Stencil8,
                block_width: 1,
                block_height: 1,
                bytes_per_block: 4,
            },
        ],
        max_buffer_size: limits.max_buffer_size,
        max_texture_dimension: limits.max_texture_dimension_2d,
        max_texture_samples: limits.max_texture_dimension_2d.min(8),
        staging_alignment: limits.min_storage_buffer_offset_alignment as u64,
        max_inflight_copies: 8,
        queue_count: 1,
        min_buffer_copy_offset_alignment: wgpu::COPY_BUFFER_ALIGNMENT,
        bytes_per_row_alignment: wgpu::COPY_BYTES_PER_ROW_ALIGNMENT,
        rows_per_image_alignment: 1,
        // wgpu uses a unified queue that supports transfer+compute.
        has_transfer_queue: true,
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
        let _guard = self.copy_limiter.acquire();
        let handle = self.create_buffer(req)?;
        if let Some(buf) = self
            .resources
            .buffers
            .lock()
            .expect("buffers lock")
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
            .expect("buffers lock")
            .get(&handle.id)
            .cloned()
            .ok_or(GpuError::Unsupported)?;
        // Staging reuse
        let staging = {
            let mut pool = self
                ._staging_pool
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(list) = pool.get_mut(&handle.size_bytes) {
                list.pop()
            } else {
                None
            }
            .unwrap_or_else(|| {
                self.device.create_buffer(&wgpu::BufferDescriptor {
                    label: Some("readback"),
                    size: handle.size_bytes,
                    usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
                    mapped_at_creation: false,
                })
            })
        };
        let _guard = self.copy_limiter.acquire();
        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("readback-encoder"),
            });
        encoder.copy_buffer_to_buffer(&buf, 0, &staging, 0, handle.size_bytes);
        self.queue.submit(Some(encoder.finish()));
        let buffer_slice = staging.slice(..);
        buffer_slice.map_async(wgpu::MapMode::Read, |_| {});
        let _ = self.device.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });
        let data = buffer_slice.get_mapped_range().to_vec();
        staging.unmap();
        self.record_download(data.len() as u64);
        // Return staging to pool
        {
            let mut pool = self
                ._staging_pool
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            pool.entry(handle.size_bytes).or_default().push(staging);
        }
        Ok(data)
    }
}

fn build_info_from_adapter(adapter: &Adapter) -> (GpuAdapterInfo, Features, Limits) {
    let info = adapter.get_info();
    let features = adapter.features();
    let limits = adapter.limits();
    let name = format!("{} ({:?})", info.name, info.backend);
    (
        GpuAdapterInfo {
            name,
            backend: GpuBackendKind::Wgpu,
            device_id: Some(format!("{:x}", info.device)),
            vendor_id: Some(info.vendor.to_string()),
        },
        features,
        limits,
    )
}

/// Simple semaphore to cap inflight copy operations.
struct CopyLimiter {
    limit: u32,
    state: Mutex<u32>,
    cv: Condvar,
}

impl CopyLimiter {
    fn new(limit: u32) -> Self {
        Self {
            limit,
            state: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    fn acquire(&self) -> CopyGuard<'_> {
        let mut count = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *count >= self.limit {
            count = self
                .cv
                .wait(count)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *count += 1;
        CopyGuard { limiter: self }
    }

    fn release(&self) {
        let mut count = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *count = count.saturating_sub(1);
        self.cv.notify_one();
    }
}

struct CopyGuard<'a> {
    limiter: &'a CopyLimiter,
}

impl Drop for CopyGuard<'_> {
    fn drop(&mut self) {
        self.limiter.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wgpu_backend_creates_resources() {
        let backend = WgpuBackend::new().unwrap();
        let buf = backend
            .create_buffer(&GpuRequest {
                usage: GpuUsage::UPLOAD,
                format: None,
                size_bytes: 1024,
            })
            .unwrap();
        assert_eq!(buf.size_bytes, 1024);
        let img = backend
            .create_image(&GpuImageRequest {
                format: GpuFormat::Rgba8Unorm,
                width: 256,
                height: 256,
                samples: 1,
                usage: GpuUsage::RENDER_TARGET,
            })
            .unwrap();
        assert_eq!(img.width, 256);
    }
}
