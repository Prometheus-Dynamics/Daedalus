use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{
    Arc, OnceLock,
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

    /// Return a buffer to the pool. It is keyed by its real allocation size.
    pub fn put_buffer(&mut self, device_key: usize, buffer: wgpu::Buffer) {
        self.pool_mut(device_key).put_buffer(buffer);
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

/// Largest allocation a pooled buffer may be reused for, as a multiple of the requested size.
/// Keeps a small readback from pinning (and mapping) a much larger buffer.
const MAX_BUFFER_REUSE_FACTOR: u64 = 2;

/// Alignment of mapped readback ranges (`wgpu` requires map offsets and sizes to be multiples of 8).
pub(crate) const MAP_ALIGNMENT: u64 = 8;

/// Allocation size for a readback staging buffer that must hold `size` bytes: rounded up so the
/// whole buffer (and any range mapped from it) is a valid map range.
pub(crate) fn staging_size(size: u64) -> u64 {
    size.div_ceil(MAP_ALIGNMENT) * MAP_ALIGNMENT
}

/// Allocation sizes a pooled buffer may have and still serve a request for `size` bytes.
fn reuse_range(size: u64) -> std::ops::RangeInclusive<u64> {
    size..=size.saturating_mul(MAX_BUFFER_REUSE_FACTOR)
}

struct DevicePool {
    buffers: Vec<wgpu::Buffer>,
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

    /// Take a pooled buffer holding at least `size` bytes, but no more than
    /// `MAX_BUFFER_REUSE_FACTOR * size`.
    fn take_buffer(&mut self, size: u64, usage: wgpu::BufferUsages) -> Option<wgpu::Buffer> {
        let idx = self
            .buffers
            .iter()
            .position(|b| reuse_range(size).contains(&b.size()) && b.usage().contains(usage))?;
        Some(self.buffers.swap_remove(idx))
    }

    fn put_buffer(&mut self, buffer: wgpu::Buffer) {
        if self.buffers.len() >= temp_pool_buffer_limit() {
            self.buffers.swap_remove(0);
        }
        self.buffers.push(buffer);
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
    temp_pool().lock().clear_device(device_key);
}

#[cfg(test)]
mod tests {
    use super::{reuse_range, staging_size};

    #[test]
    fn staging_size_rounds_up_to_map_alignment() {
        assert_eq!(staging_size(0), 0);
        assert_eq!(staging_size(1), 8);
        assert_eq!(staging_size(8), 8);
        assert_eq!(staging_size(13), 16);
    }

    #[test]
    fn pooled_buffers_are_reused_only_between_one_and_twice_the_request() {
        let range = reuse_range(256);
        assert!(range.contains(&256));
        assert!(range.contains(&512));
        assert!(!range.contains(&255));
        // A 16 KiB buffer must not be handed out for a 256-byte readback.
        assert!(!range.contains(&(16 * 1024)));
    }
}
