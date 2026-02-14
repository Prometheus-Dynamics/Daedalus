use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

use super::{Access, BindingKind, BindingSpec};

static PIPE_CACHE_PER_DEVICE_LIMIT: AtomicUsize = AtomicUsize::new(128);
static BIND_GROUP_CACHE_PER_DEVICE_LIMIT: AtomicUsize = AtomicUsize::new(256);

/// Small per-device cache that tracks insertion/usage order for eviction.
struct DeviceCache<T> {
    entries: HashMap<u64, T>,
    order: VecDeque<u64>,
}

impl<T> DeviceCache<T> {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }
}

impl<T: Clone> DeviceCache<T> {
    fn get(&mut self, key: &u64) -> Option<T> {
        let value = self.entries.get(key).cloned()?;
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
            self.order.push_back(*key);
        }
        Some(value)
    }

    fn insert_with_limit(&mut self, key: u64, value: T, limit: usize) {
        if let Some(existing) = self.entries.get_mut(&key) {
            *existing = value;
            if let Some(pos) = self.order.iter().position(|k| *k == key) {
                self.order.remove(pos);
            }
            self.order.push_back(key);
            return;
        }
        while self.entries.len() >= limit {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }
        self.entries.insert(key, value);
        self.order.push_back(key);
    }
}

/// Set the maximum cached pipelines per device before eviction. Returns the previous limit.
pub fn set_pipeline_cache_limit(limit: usize) -> usize {
    PIPE_CACHE_PER_DEVICE_LIMIT.swap(limit.max(1), Ordering::Relaxed)
}

/// Set the maximum cached bind groups per device before eviction. Returns the previous limit.
pub fn set_bind_group_cache_limit(limit: usize) -> usize {
    BIND_GROUP_CACHE_PER_DEVICE_LIMIT.swap(limit.max(1), Ordering::Relaxed)
}

/// Current pipeline cache limit per device.
pub fn pipeline_cache_limit() -> usize {
    PIPE_CACHE_PER_DEVICE_LIMIT.load(Ordering::Relaxed).max(1)
}

/// Current bind group cache limit per device.
pub fn bind_group_cache_limit() -> usize {
    BIND_GROUP_CACHE_PER_DEVICE_LIMIT
        .load(Ordering::Relaxed)
        .max(1)
}

pub(crate) struct PipelineEntry {
    pub key: u64,
    pub bind_group_layout: wgpu::BindGroupLayout,
    pub pipeline: wgpu::ComputePipeline,
}

pub(crate) fn pipeline_entry(
    device: &wgpu::Device,
    shader_src: &str,
    spec: &super::ShaderSpec,
    layout_bindings: &[BindingSpec],
) -> Arc<PipelineEntry> {
    static PIPE_CACHE: OnceLock<Mutex<HashMap<usize, DeviceCache<Arc<PipelineEntry>>>>> =
        OnceLock::new();

    let device_key = device as *const _ as usize;
    let cache = PIPE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut hasher = DefaultHasher::new();
    spec.name.hash(&mut hasher);
    spec.entry.hash(&mut hasher);
    shader_src.hash(&mut hasher);
    for b in layout_bindings {
        b.binding.hash(&mut hasher);
        (b.kind as u8).hash(&mut hasher);
        (b.access as u8).hash(&mut hasher);
        if let Some(f) = b.texture_format {
            f.hash(&mut hasher);
        }
        if let Some(s) = b.sample_type {
            match s {
                wgpu::TextureSampleType::Float { filterable } => {
                    1u8.hash(&mut hasher);
                    filterable.hash(&mut hasher);
                }
                wgpu::TextureSampleType::Depth => 2u8.hash(&mut hasher),
                wgpu::TextureSampleType::Sint => 3u8.hash(&mut hasher),
                wgpu::TextureSampleType::Uint => 4u8.hash(&mut hasher),
            }
        }
        if let Some(v) = b.view_dimension {
            v.hash(&mut hasher);
        }
        if let Some(sk) = b.sampler_kind {
            (sk as u8).hash(&mut hasher);
        }
    }
    let key = hasher.finish();

    if let Some(entry) = cache
        .lock()
        .ok()
        .and_then(|mut m| m.get_mut(&device_key).and_then(|c| c.get(&key)))
    {
        return entry;
    }

    let bind_group_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
        label: Some("bgl"),
        entries: &layout_bindings
            .iter()
            .map(|b| {
                let ty = match b.kind {
                    BindingKind::Storage => wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage {
                            read_only: matches!(b.access, Access::ReadOnly),
                        },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    BindingKind::Uniform => wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Uniform,
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    BindingKind::Texture2D => wgpu::BindingType::Texture {
                        multisampled: false,
                        view_dimension: b.view_dimension.unwrap_or(wgpu::TextureViewDimension::D2),
                        sample_type: b
                            .sample_type
                            .unwrap_or(wgpu::TextureSampleType::Float { filterable: true }),
                    },
                    BindingKind::StorageTexture2D => {
                        let format = b.texture_format.unwrap_or(wgpu::TextureFormat::Rgba8Unorm);
                        let access = match b.access {
                            Access::ReadOnly => wgpu::StorageTextureAccess::ReadOnly,
                            Access::ReadWrite => wgpu::StorageTextureAccess::ReadWrite,
                            Access::WriteOnly => wgpu::StorageTextureAccess::WriteOnly,
                        };
                        wgpu::BindingType::StorageTexture {
                            access,
                            format,
                            view_dimension: b
                                .view_dimension
                                .unwrap_or(wgpu::TextureViewDimension::D2),
                        }
                    }
                    BindingKind::Sampler => {
                        let cmp = matches!(b.sampler_kind, Some(super::SamplerKind::Comparison));
                        wgpu::BindingType::Sampler(if cmp {
                            wgpu::SamplerBindingType::Comparison
                        } else {
                            wgpu::SamplerBindingType::Filtering
                        })
                    }
                };
                wgpu::BindGroupLayoutEntry {
                    binding: b.binding,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty,
                    count: None,
                }
            })
            .collect::<Vec<_>>(),
    });

    let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
        label: Some("pipeline_layout"),
        bind_group_layouts: &[&bind_group_layout],
        push_constant_ranges: &[],
    });

    let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("shader"),
        source: wgpu::ShaderSource::Wgsl(shader_src.into()),
    });

    let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
        label: Some("pipeline"),
        layout: Some(&pipeline_layout),
        module: &shader,
        entry_point: Some(spec.entry),
        compilation_options: wgpu::PipelineCompilationOptions::default(),
        cache: None,
    });

    let entry = Arc::new(PipelineEntry {
        key,
        bind_group_layout,
        pipeline,
    });
    if let Ok(mut m) = cache.lock() {
        let cache = m.entry(device_key).or_insert_with(DeviceCache::new);
        cache.insert_with_limit(key, entry.clone(), pipeline_cache_limit());
    }
    entry
}

pub(crate) fn bind_group(
    device: &wgpu::Device,
    layout: &wgpu::BindGroupLayout,
    prepared: &[super::prepare::Prepared],
    pipeline_key: u64,
) -> wgpu::BindGroup {
    static BIND_GROUP_CACHE: OnceLock<Mutex<HashMap<usize, DeviceCache<wgpu::BindGroup>>>> =
        OnceLock::new();
    let device_key = device as *const _ as usize;

    let entries: Vec<wgpu::BindGroupEntry> = prepared
        .iter()
        .map(|p| match p {
            super::prepare::Prepared::Buffer { spec, buffer, .. } => wgpu::BindGroupEntry {
                binding: spec.binding,
                resource: buffer.as_entire_binding(),
            },
            super::prepare::Prepared::Texture { spec, view, .. } => wgpu::BindGroupEntry {
                binding: spec.binding,
                resource: wgpu::BindingResource::TextureView(view),
            },
            super::prepare::Prepared::Sampler { spec, sampler } => wgpu::BindGroupEntry {
                binding: spec.binding,
                resource: wgpu::BindingResource::Sampler(sampler),
            },
        })
        .collect();

    let mut bind_hasher = DefaultHasher::new();
    pipeline_key.hash(&mut bind_hasher);
    let mut can_cache_bg = true;
    for p in prepared {
        match p {
            super::prepare::Prepared::Buffer { spec, buffer, .. } => {
                spec.binding.hash(&mut bind_hasher);
                (buffer as *const wgpu::Buffer as usize).hash(&mut bind_hasher);
            }
            super::prepare::Prepared::Texture {
                spec,
                texture,
                owned,
                ..
            } => {
                // IMPORTANT: Never cache bind groups that reference textures.
                //
                // In streaming graphs, textures are frequently per-frame values (new underlying GPU
                // allocations each tick). `wgpu::BindGroup` retains the referenced `TextureView`,
                // which retains the `Texture`. Caching such bind groups therefore pins old frame
                // textures and can quickly OOM embedded GPUs.
                //
                // We still cache pure-buffer bind groups (common for small GPU-state shaders).
                can_cache_bg = false;
                spec.binding.hash(&mut bind_hasher);
                (Arc::as_ptr(texture) as usize).hash(&mut bind_hasher);
                if *owned {
                    can_cache_bg = false;
                }
            }
            super::prepare::Prepared::Sampler { spec, sampler } => {
                spec.binding.hash(&mut bind_hasher);
                (sampler as *const wgpu::Sampler as usize).hash(&mut bind_hasher);
            }
        }
    }
    let bind_key = if can_cache_bg {
        Some(bind_hasher.finish())
    } else {
        None
    };
    if let Some(k) = bind_key
        && let Ok(mut m) = BIND_GROUP_CACHE
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
        && let Some(bg) = m.get_mut(&device_key).and_then(|c| c.get(&k))
    {
        return bg;
    }

    let bg = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("bind_group"),
        layout,
        entries: &entries,
    });
    if let Some(k) = bind_key
        && let Ok(mut m) = BIND_GROUP_CACHE
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
    {
        let cache = m.entry(device_key).or_insert_with(DeviceCache::new);
        cache.insert_with_limit(k, bg.clone(), bind_group_cache_limit());
    }
    bg
}
