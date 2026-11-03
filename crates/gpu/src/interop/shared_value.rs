//! One buffer that a wgpu node and a CUDA node both use, and the handoffs between them (Linux,
//! `gpu-vulkan-raw` and `gpu-cuda`).
//!
//! A [`SharedValue`] is one device-local Vulkan allocation ([`SharedBuffer`]) with three views of
//! it: a `wgpu::Buffer` (created with `create_buffer_from_hal` over the Vulkan buffer, so wgpu
//! binds the same memory), a CUDA import of its opaque fd (made on first use by a CUDA device), and
//! a Vulkan timeline semaphore that orders the CUDA work against the raw queue.
//!
//! # Handoffs (no copy at either)
//!
//! - [`SharedValue::enter_cuda`], wgpu to CUDA: the host waits for the wgpu work submitted so far
//!   (a host fence on wgpu's queue, `Device::poll`). A raw submit sets the timeline to a new value,
//!   and the CUDA stream waits for that value. Work the CUDA node queues after it runs on the GPU.
//! - [`SharedValue::leave_cuda`], CUDA to wgpu: the CUDA stream sets the next value after the work
//!   queued before it (the kernels). A raw submit waits for that value, and the host waits for the
//!   submit (a host fence), so wgpu's next submit sees the kernels' writes.
//!
//! wgpu cannot take a caller's semaphore in its own submits, which is why the wgpu boundaries are
//! host fences and only the CUDA side uses the timeline.
//!
//! # Lifetimes
//!
//! The `wgpu::Buffer` view and the CUDA imports refer to the Vulkan memory, which the
//! [`SharedBuffer`] owns. Fields drop in declaration order: the views first, then the buffer, then
//! the semaphore, then the context.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use wgpu::hal::{api::Vulkan, vulkan as hal_vk};

use super::{SharedBuffer, SharedSemaphore};
use crate::raw_vulkan::RawSubmitter;
use crate::{CudaBackend, CudaImport, CudaSemaphore, GpuContextHandle, GpuError};

/// Slots of the raw submitter the handoffs use (each handoff waits for the previous one's slot).
const RING_SLOTS: usize = 2;
/// The workgroup size [`SharedValue::dispatch_wgsl`] dispatches: the entry point must match it.
pub const WORKGROUP_SIZE: u32 = 64;

/// The CUDA side of a value on one device: the import of its memory and of its semaphore.
struct CudaSide {
    uuid: [u8; 16],
    import: CudaImport,
    sem: CudaSemaphore,
}

/// A buffer shared by wgpu and CUDA on one NVIDIA GPU, with the handoffs. See the module docs.
pub struct SharedValue {
    /// The CUDA import and semaphore, made by the first CUDA device that uses the value.
    cuda: Mutex<Option<CudaSide>>,
    /// The wgpu view of the memory, made on first use.
    wgpu: OnceLock<wgpu::Buffer>,
    /// Raw submits of the handoffs, on the context's queue under its submit lock.
    ring: Mutex<RawSubmitter>,
    /// The next timeline value to set (CUDA and Vulkan alternate values on one timeline).
    next: AtomicU64,
    /// The timeline semaphore, exported to CUDA on first use.
    semaphore: SharedSemaphore,
    /// The shared memory. Its staging buffer is only used by `read`.
    buffer: Mutex<SharedBuffer>,
    /// Declared last: the device outlives the objects above.
    ctx: GpuContextHandle,
}

impl std::fmt::Debug for SharedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedValue")
            .field("bytes", &self.bytes())
            .finish_non_exhaustive()
    }
}

fn wgpu_error(what: &str, err: impl std::fmt::Debug) -> GpuError {
    GpuError::Internal(format!("{what}: {err:?}"))
}

impl SharedValue {
    /// A shared value of `bytes` on `ctx`'s wgpu device. `Unsupported` unless `ctx` is wgpu on
    /// Vulkan with external memory and semaphores (the `gpu-dmabuf` device request).
    pub fn create(ctx: &GpuContextHandle, bytes: u64) -> Result<Self, GpuError> {
        let buffer = SharedBuffer::create(ctx, bytes)?;
        let semaphore = SharedSemaphore::create(ctx)?;
        let ring = RawSubmitter::with_slots(ctx, RING_SLOTS)?;
        Ok(Self {
            cuda: Mutex::new(None),
            wgpu: OnceLock::new(),
            ring: Mutex::new(ring),
            next: AtomicU64::new(0),
            semaphore,
            buffer: Mutex::new(buffer),
            ctx: ctx.clone(),
        })
    }

    /// The size in bytes.
    pub fn bytes(&self) -> u64 {
        self.buffer.lock().bytes()
    }

    /// The memory as a `wgpu::Buffer` (storage, copy source and destination). Created once, on
    /// the first call. wgpu binds the Vulkan buffer without owning its memory.
    pub fn wgpu_buffer(&self) -> Result<&wgpu::Buffer, GpuError> {
        if let Some(buffer) = self.wgpu.get() {
            return Ok(buffer);
        }
        let (device, _) = self.ctx.wgpu().ok_or(GpuError::Unsupported)?;
        let bytes = self.bytes();
        let vk_buffer = self.buffer.lock().vk_buffer();
        // SAFETY: `vk_buffer` is a buffer of this device, created with storage and transfer usages
        // and bound to memory by `SharedBuffer::create`. `from_raw_externally_owned` makes wgpu
        // destroy neither the buffer nor its memory (`from_raw` would have wgpu call
        // `vkDestroyBuffer` when the view drops, a double destroy with the `SharedBuffer`). The
        // `SharedBuffer` destroys both, and outlives this view (fields drop in order, see the module
        // docs). wgpu's init tracker for a hal buffer has no uninitialized ranges, so it does not
        // clear the data.
        let buffer = unsafe {
            device.create_buffer_from_hal::<Vulkan>(
                hal_vk::Buffer::from_raw_externally_owned(vk_buffer, Box::new(|| {})),
                &wgpu::BufferDescriptor {
                    label: Some("daedalus shared buffer"),
                    size: bytes,
                    usage: wgpu::BufferUsages::STORAGE
                        | wgpu::BufferUsages::COPY_SRC
                        | wgpu::BufferUsages::COPY_DST,
                    mapped_at_creation: false,
                },
            )
        };
        Ok(self.wgpu.get_or_init(|| buffer))
    }

    /// Runs `source`'s compute entry point `entry` over the memory on wgpu, then waits for it (a
    /// host fence). `source` must declare `@group(0) @binding(0) var<storage, read_write>
    /// data: array<f32>;` and the entry must use `@workgroup_size(WORKGROUP_SIZE)` and guard its
    /// index with `arrayLength(&data)`.
    pub fn dispatch_wgsl(&self, source: &str, entry: &str) -> Result<(), GpuError> {
        let buffer = self.wgpu_buffer()?;
        let (device, queue) = self.ctx.wgpu().ok_or(GpuError::Unsupported)?;
        let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some(entry),
            source: wgpu::ShaderSource::Wgsl(source.into()),
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some(entry),
            layout: None,
            module: &module,
            entry_point: Some(entry),
            compilation_options: Default::default(),
            cache: None,
        });
        let layout = pipeline.get_bind_group_layout(0);
        let bind = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some(entry),
            layout: &layout,
            entries: &[wgpu::BindGroupEntry {
                binding: 0,
                resource: buffer.as_entire_binding(),
            }],
        });
        let count = (self.bytes() / 4) as u32;
        let mut encoder = device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_compute_pass(&Default::default());
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, &bind, &[]);
            pass.dispatch_workgroups(count.div_ceil(WORKGROUP_SIZE), 1, 1);
        }
        queue.submit(Some(encoder.finish()));
        self.wgpu_fence()
    }

    /// The wgpu to CUDA boundary: waits for the wgpu work submitted so far, then makes CUDA device
    /// `cuda` wait on the timeline for this handoff. CUDA work queued after this call runs after
    /// the wgpu work. The first call on a device imports the memory and the semaphore into it.
    pub fn enter_cuda(&self, cuda: &CudaBackend) -> Result<(), GpuError> {
        self.wgpu_fence()?;
        let value = self.next.fetch_add(1, Ordering::AcqRel) + 1;
        self.with_cuda(cuda, |side| {
            {
                let ring = self.ring.lock();
                // SAFETY: records no commands; the semaphore is owned by this value and outlives
                // the submit's use of it (the submit completes before `ring` is reused).
                unsafe {
                    ring.submit_synced(|_, _| {}, &[], &[(self.semaphore.raw_handle(), value)])
                }?;
            }
            side.sem.wait(value)
        })
    }

    /// Queues `data[i] *= factor` on the CUDA device's stream, in place in the shared memory. Call
    /// between [`enter_cuda`](Self::enter_cuda) and [`leave_cuda`](Self::leave_cuda). Not waited.
    pub fn cuda_scale(&self, cuda: &CudaBackend, factor: f32) -> Result<(), GpuError> {
        self.with_cuda(cuda, |side| side.import.scale_f32(factor).map(drop))
    }

    /// The CUDA to wgpu boundary: sets the timeline after the CUDA work queued so far, and waits
    /// on the host for that, so wgpu's next submit sees the CUDA writes.
    pub fn leave_cuda(&self, cuda: &CudaBackend) -> Result<(), GpuError> {
        let value = self.next.fetch_add(1, Ordering::AcqRel) + 1;
        self.with_cuda(cuda, |side| {
            side.sem.signal(value)?;
            let ring = self.ring.lock();
            // SAFETY: as in `enter_cuda`; the ticket is waited on before the semaphore is used again.
            let ticket = unsafe {
                ring.submit_synced(|_, _| {}, &[(self.semaphore.raw_handle(), value)], &[])
            }?;
            ticket.wait();
            Ok(())
        })
    }

    /// The bytes of the memory, copied back to the host (a Vulkan copy into staging and a host
    /// read). Use after the last handoff; it is a check, not part of a graph.
    pub fn read(&self) -> Result<Vec<u8>, GpuError> {
        self.buffer.lock().read()
    }

    /// Waits on the host for all wgpu work submitted so far (the host fence of the wgpu side).
    fn wgpu_fence(&self) -> Result<(), GpuError> {
        let (device, _) = self.ctx.wgpu().ok_or(GpuError::Unsupported)?;
        device
            .poll(wgpu::PollType::Wait {
                submission_index: None,
                timeout: None,
            })
            .map(|_| ())
            .map_err(|e| wgpu_error("wgpu poll", e))
    }

    /// Runs `f` on this value's CUDA side for `cuda`, importing the memory and the semaphore first
    /// when this is the first use on the device. `InvalidInput` when the value lives on another GPU.
    fn with_cuda<R>(
        &self,
        cuda: &CudaBackend,
        f: impl FnOnce(&CudaSide) -> Result<R, GpuError>,
    ) -> Result<R, GpuError> {
        let uuid = cuda.device_uuid()?;
        let mut side = self.cuda.lock();
        match side.as_ref() {
            Some(existing) if existing.uuid != uuid => {
                return Err(GpuError::InvalidInput(
                    "the shared value's CUDA side is on another device".into(),
                ));
            }
            Some(_) => {}
            None => {
                let buffer = self.buffer.lock();
                if buffer.device_uuid() != uuid {
                    return Err(GpuError::InvalidInput(
                        "the shared value lives on another GPU than this CUDA device".into(),
                    ));
                }
                let import = cuda.import_opaque_fd(buffer.export_fd()?, buffer.bytes())?;
                drop(buffer);
                let sem = cuda.import_timeline_semaphore(self.semaphore.export_fd()?)?;
                *side = Some(CudaSide { uuid, import, sem });
            }
        }
        f(side.as_ref().expect("the CUDA side was made above"))
    }
}
