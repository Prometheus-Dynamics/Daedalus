use std::collections::HashMap;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

// Default pool sizes must be conservative on embedded GPUs; large defaults can cause
// transient OOM/driver resets when shaders allocate full-frame intermediate textures.
static TEMP_POOL_BUFFER_LIMIT: AtomicUsize = AtomicUsize::new(16);
static TEMP_POOL_TEXTURE_LIMIT: AtomicUsize = AtomicUsize::new(6);

/// Set the buffer pool limit per device. Returns the previous limit.
pub fn set_temp_pool_buffer_limit(limit: usize) -> usize {
    TEMP_POOL_BUFFER_LIMIT.swap(limit.max(1), Ordering::Relaxed)
}

/// Set the texture pool limit per device. Returns the previous limit.
pub fn set_temp_pool_texture_limit(limit: usize) -> usize {
    TEMP_POOL_TEXTURE_LIMIT.swap(limit.max(1), Ordering::Relaxed)
}

pub fn temp_pool_buffer_limit() -> usize {
    TEMP_POOL_BUFFER_LIMIT.load(Ordering::Relaxed).max(1)
}

pub fn temp_pool_texture_limit() -> usize {
    TEMP_POOL_TEXTURE_LIMIT.load(Ordering::Relaxed).max(1)
}

/// Temporary allocations reused between dispatches (buffers/textures).
pub struct TempPool {
    per_device: HashMap<usize, DevicePool>,
}

impl TempPool {
    pub fn new() -> Self {
        Self {
            per_device: HashMap::new(),
        }
    }

    fn pool_mut(&mut self, device_key: usize) -> &mut DevicePool {
        self.per_device
            .entry(device_key)
            .or_insert_with(DevicePool::new)
    }

    pub fn take_buffer(
        &mut self,
        device_key: usize,
        size: u64,
        usage: wgpu::BufferUsages,
    ) -> Option<wgpu::Buffer> {
        self.pool_mut(device_key).take_buffer(size, usage)
    }

    pub fn put_buffer(&mut self, device_key: usize, size: u64, buffer: wgpu::Buffer) {
        self.pool_mut(device_key).put_buffer(size, buffer);
    }

    pub fn take_texture(
        &mut self,
        device_key: usize,
        width: u32,
        height: u32,
        format: wgpu::TextureFormat,
        usage: wgpu::TextureUsages,
    ) -> Option<Arc<wgpu::Texture>> {
        self.pool_mut(device_key)
            .take_texture(width, height, format, usage)
    }

    pub fn put_texture(
        &mut self,
        device_key: usize,
        width: u32,
        height: u32,
        format: wgpu::TextureFormat,
        usage: wgpu::TextureUsages,
        tex: Arc<wgpu::Texture>,
    ) {
        self.pool_mut(device_key)
            .put_texture(width, height, format, usage, tex);
    }

    pub fn clear_device(&mut self, device_key: usize) {
        self.per_device.remove(&device_key);
    }
}

struct DevicePool {
    buffers: Vec<(u64, wgpu::Buffer)>,
    textures: Vec<(
        u32,
        u32,
        wgpu::TextureFormat,
        wgpu::TextureUsages,
        Arc<wgpu::Texture>,
    )>,
}

impl DevicePool {
    fn new() -> Self {
        Self {
            buffers: Vec::new(),
            textures: Vec::new(),
        }
    }

    fn take_buffer(&mut self, size: u64, usage: wgpu::BufferUsages) -> Option<wgpu::Buffer> {
        if let Some(idx) = self
            .buffers
            .iter()
            .position(|(s, b)| *s >= size && b.usage().contains(usage))
        {
            let (_, buf) = self.buffers.swap_remove(idx);
            return Some(buf);
        }
        None
    }

    fn put_buffer(&mut self, size: u64, buffer: wgpu::Buffer) {
        if self.buffers.len() >= temp_pool_buffer_limit() {
            self.buffers.swap_remove(0);
        }
        self.buffers.push((size, buffer));
    }

    fn take_texture(
        &mut self,
        width: u32,
        height: u32,
        format: wgpu::TextureFormat,
        usage: wgpu::TextureUsages,
    ) -> Option<Arc<wgpu::Texture>> {
        // A pooled texture with *more* usage flags is still compatible with a request that needs
        // a subset (wgpu allows using a texture for any declared usage).
        if let Some(idx) = self.textures.iter().position(|(w, h, fmt, u, _)| {
            *w == width && *h == height && *fmt == format && u.contains(usage)
        }) {
            let (_, _, _, _, tex) = self.textures.swap_remove(idx);
            return Some(tex);
        }
        None
    }

    fn put_texture(
        &mut self,
        width: u32,
        height: u32,
        format: wgpu::TextureFormat,
        usage: wgpu::TextureUsages,
        tex: Arc<wgpu::Texture>,
    ) {
        if self.textures.len() >= temp_pool_texture_limit() {
            self.textures.swap_remove(0);
        }
        self.textures.push((width, height, format, usage, tex));
    }
}

static TEMP_POOL: OnceLock<Mutex<TempPool>> = OnceLock::new();

pub fn temp_pool() -> &'static Mutex<TempPool> {
    TEMP_POOL.get_or_init(|| Mutex::new(TempPool::new()))
}

pub(crate) fn clear_temp_pool_for_device(device_key: usize) {
    if let Ok(mut pool) = temp_pool().lock() {
        pool.clear_device(device_key);
    }
}
