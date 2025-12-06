use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::GpuError;

use super::{Access, BindingData, BindingKind, ShaderBinding, ctx};

/// Marker for GPU state structs; derive sets whether to zero-init and default readback.
pub trait GpuStateful: bytemuck::Pod + Copy {
    /// If true, `GpuState::new_stateful()` will zero-initialize instead of using Default.
    const ZEROED: bool = false;
    /// Default readback flag used by `binding_stateful`.
    const READBACK: bool = false;
}

struct StatePool {
    per_device: HashMap<usize, Vec<(u64, Arc<wgpu::Buffer>)>>,
}

/// Maximum pooled GPU state buffers per device before evicting oldest.
pub static GPU_STATE_POOL_LIMIT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(64);

pub fn set_gpu_state_pool_limit(limit: usize) -> usize {
    GPU_STATE_POOL_LIMIT.swap(limit.max(1), std::sync::atomic::Ordering::Relaxed)
}

pub fn gpu_state_pool_limit() -> usize {
    GPU_STATE_POOL_LIMIT
        .load(std::sync::atomic::Ordering::Relaxed)
        .max(1)
}

impl StatePool {
    fn new() -> Self {
        Self {
            per_device: HashMap::new(),
        }
    }

    fn pool_mut(&mut self, device_key: usize) -> &mut Vec<(u64, Arc<wgpu::Buffer>)> {
        self.per_device.entry(device_key).or_default()
    }

    fn take(
        &mut self,
        device_key: usize,
        size: u64,
        usage: wgpu::BufferUsages,
    ) -> Option<Arc<wgpu::Buffer>> {
        let pool = self.pool_mut(device_key);
        if let Some(idx) = pool
            .iter()
            .position(|(s, b)| *s >= size && b.usage().contains(usage))
        {
            let (_, buf) = pool.swap_remove(idx);
            return Some(buf);
        }
        None
    }

    fn put(&mut self, device_key: usize, size: u64, buffer: Arc<wgpu::Buffer>) {
        let pool = self.pool_mut(device_key);
        if pool.len()
            >= GPU_STATE_POOL_LIMIT
                .load(std::sync::atomic::Ordering::Relaxed)
                .max(1)
        {
            pool.swap_remove(0);
        }
        pool.push((size, buffer));
    }
}

static STATE_POOL: OnceLock<std::sync::Mutex<StatePool>> = OnceLock::new();

/// Persistent GPU-side state for small POD types; keeps a device buffer alive across dispatches.
pub struct GpuState<T: bytemuck::Pod + Copy> {
    buffer: Arc<wgpu::Buffer>,
    size: u64,
    device: wgpu::Device,
    queue: wgpu::Queue,
    device_key: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<T: bytemuck::Pod + Copy> GpuState<T> {
    pub fn new(initial: T) -> Result<Self, GpuError> {
        let ctx = ctx()?;
        Self::new_with_device(initial, ctx.device.as_ref(), ctx.queue.as_ref())
    }

    #[cfg(feature = "gpu-wgpu")]
    pub fn new_with_gpu(initial: T, gpu: &crate::GpuContextHandle) -> Result<Self, GpuError> {
        let backend = gpu.backend_ref();
        let wgpu_backend = backend
            .as_any()
            .downcast_ref::<crate::wgpu_backend::WgpuBackend>()
            .ok_or(GpuError::Unsupported)?;
        let (device, queue) = wgpu_backend.device_queue();
        Self::new_with_device(initial, device, queue)
    }

    fn new_with_device(
        initial: T,
        device: &wgpu::Device,
        queue: &wgpu::Queue,
    ) -> Result<Self, GpuError> {
        let size = std::mem::size_of::<T>() as u64;
        let usage = wgpu::BufferUsages::STORAGE
            | wgpu::BufferUsages::COPY_DST
            | wgpu::BufferUsages::COPY_SRC;
        let buffer = if let Some(buf) = STATE_POOL
            .get_or_init(|| std::sync::Mutex::new(StatePool::new()))
            .lock()
            .ok()
            .and_then(|mut p| p.take(device as *const _ as usize, size, usage))
        {
            queue.write_buffer(buf.as_ref(), 0, bytemuck::bytes_of(&initial));
            buf
        } else {
            let buf = device.create_buffer(&wgpu::BufferDescriptor {
                label: Some("gpu-state"),
                size,
                usage,
                mapped_at_creation: false,
            });
            queue.write_buffer(&buf, 0, bytemuck::bytes_of(&initial));
            Arc::new(buf)
        };
        Ok(Self {
            buffer,
            size,
            device: device.clone(),
            queue: queue.clone(),
            device_key: device as *const _ as usize,
            _marker: std::marker::PhantomData,
        })
    }

    /// Read back the current value from GPU.
    pub fn read(&self) -> Result<T, GpuError> {
        let device = &self.device;
        let staging = device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("gpu-state-readback"),
            size: self.size,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        let mut encoder = device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("gpu-state-readback-encoder"),
        });
        encoder.copy_buffer_to_buffer(self.buffer.as_ref(), 0, &staging, 0, self.size);
        self.queue.submit([encoder.finish()]);

        let slice = staging.slice(..);
        let (tx, rx) = std::sync::mpsc::channel();
        slice.map_async(wgpu::MapMode::Read, move |res| {
            let _ = tx.send(res);
        });
        let _ = device.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });
        rx.recv()
            .unwrap_or(Err(wgpu::BufferAsyncError))
            .map_err(|e| GpuError::Internal(format!("map failed: {e:?}")))?;
        let data = slice.get_mapped_range();
        let val = bytemuck::from_bytes::<T>(&data[..self.size as usize]).to_owned();
        drop(data);
        staging.unmap();
        Ok(val)
    }

    /// Write a new value to the GPU buffer.
    pub fn write(&self, value: T) -> Result<(), GpuError> {
        self.queue
            .write_buffer(&self.buffer, 0, bytemuck::bytes_of(&value));
        Ok(())
    }

    /// Produce a binding to use this state in a shader dispatch.
    pub fn binding(&self, binding: u32, access: Access, readback: bool) -> ShaderBinding<'static> {
        ShaderBinding {
            binding,
            kind: BindingKind::Storage,
            access,
            data: BindingData::BufferDevice {
                buffer: self.buffer.clone(),
                size: self.size,
                device_key: self.device_key,
            },
            readback,
        }
    }
}

impl<T> GpuState<T>
where
    T: GpuStateful + bytemuck::Zeroable + Default + Copy,
{
    /// Create a GPU state using either zeroed or Default depending on the derive flags.
    pub fn new_stateful() -> Result<Self, GpuError> {
        let init = if T::ZEROED { T::zeroed() } else { T::default() };
        Self::new(init)
    }

    #[cfg(feature = "gpu-wgpu")]
    pub fn new_stateful_with_gpu(gpu: &crate::GpuContextHandle) -> Result<Self, GpuError> {
        let init = if T::ZEROED { T::zeroed() } else { T::default() };
        Self::new_with_gpu(init, gpu)
    }

    /// Binding that uses the type's default readback flag.
    pub fn binding_stateful(&self, binding: u32, access: Access) -> ShaderBinding<'static> {
        self.binding(binding, access, T::READBACK)
    }
}

impl<T: bytemuck::Pod + Copy> Drop for GpuState<T> {
    fn drop(&mut self) {
        // Only return to pool if this is the last Arc reference.
        if Arc::strong_count(&self.buffer) == 1
            && let Ok(mut p) = STATE_POOL
                .get_or_init(|| std::sync::Mutex::new(StatePool::new()))
                .lock()
        {
            p.put(self.device_key, self.size, self.buffer.clone());
        }
    }
}
