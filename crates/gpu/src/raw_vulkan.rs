//! Raw Vulkan handles behind a wgpu [`GpuContextHandle`] and its [`GpuStorage`], so kernels
//! written outside Daedalus (precompiled SPIR-V, such as Styx's gpuisp) run on Daedalus's device
//! against its buffers and imported camera frames (feature `gpu-vulkan-raw`, Linux).
//!
//! # Sync contract
//!
//! - **Daedalus submits.** wgpu serializes its own `vkQueueSubmit` calls behind a lock nothing
//!   outside it can take, and Vulkan requires a queue's submissions to be externally
//!   synchronized, so never call `vkQueueSubmit` (or `vkQueueWaitIdle`) on
//!   [`RawVulkan::queue`] yourself. Record commands through [`GpuContextHandle::submit_raw`]
//!   (one command buffer of wgpu's, submitted on Daedalus's queue in order with Daedalus's own
//!   work) or through a [`RawSubmitter`] (its own ring of command buffers, no allocation per
//!   submit).
//! - **One submit lock.** Daedalus takes one process-wide lock around every `vkQueueSubmit` it
//!   makes: `queue.submit`, and the wgpu calls that submit internally when `write_buffer` data is
//!   pending (`on_submitted_work_done`, `map_async`). A `RawSubmitter` takes the same lock around
//!   its own `vkQueueSubmit`, so the two never overlap. Code outside Daedalus that submits on
//!   Daedalus's device must not do so while a `RawSubmitter` exists. That includes an app
//!   sharing the device with Styx, for example through styx-wgpu's `Gpu::from_device`, when the
//!   app calls `queue.submit` itself.
//! - **Uploads.** `queue.write_buffer` and `write_texture` data is flushed only by wgpu's next
//!   submit, so raw commands do not see it. Call [`GpuContextHandle::flush_uploads`] after
//!   them (once at setup for `GpuStorage::with_data`), or upload inside your own recording (a
//!   copy from a staging buffer).
//! - **Relay semaphores.** wgpu-hal chains its submits with relay semaphores (each waits on the
//!   previous one's signal). A raw submit neither waits on nor signals them, so the chain is
//!   unchanged. The dmabuf acquire path stages wait semaphores on wgpu-hal's list for its next
//!   wgpu submit; a raw submit does not consume that list, so the wait still attaches to that
//!   submit.
//! - **Barriers around the recording.** wgpu does not see raw commands, so it cannot place
//!   barriers for them. `submit_raw` brackets them with full memory barriers (all earlier
//!   device writes, Daedalus's included, visible to them; theirs visible to all later work and
//!   to host reads of mapped memory). Inside the recording, barriers between your own commands
//!   are yours.
//! - **Waiting.** The returned [`GpuTicket`] completes when the commands have finished:
//!   `try_complete` polls without blocking, `wait` blocks. Work Daedalus submits afterwards
//!   (`GpuKernels::run`, `GpuTicket::submit`) is ordered after yours on the same queue; no
//!   semaphore is needed. A [`RawSubmitter`]'s [`RawTicket`] has the same two calls.
//! - **Lifetimes.** wgpu does not keep alive what raw commands use. Keep every `GpuStorage`,
//!   `GpuFrameInput` (it holds the frame back from its producer) and Vulkan object you made
//!   (pipelines, descriptor sets and pools) until the ticket completes.
//! - **Object ownership.** Objects you create with [`RawVulkan::device`] are yours to destroy;
//!   never destroy what Daedalus or wgpu own (the device, the queue, a [`RawVkBuffer`]). The
//!   handles stay valid while the context (and, for a buffer, its `GpuStorage`) lives.
//! - **Timestamps.** Query pools are yours like any other object: reset them
//!   (`vkCmdResetQueryPool`) and write timestamps inside the recording, then read the results
//!   (`vkGetQueryPoolResults`) once the ticket completes. [`RawVulkan::timestamp_valid_bits`]
//!   is 0 when the queue cannot write them.
//! - **Device features.** Beyond what wgpu enables, Daedalus enables 8-bit storage-buffer access
//!   where the device has it ([`RawVulkan::storage_buffer_8bit`]). Check it (and anything else
//!   your shaders need) before creating pipelines: a feature the device was not created with
//!   makes the pipeline invalid.

use std::cell::Cell;

use ash::vk;
use wgpu::hal::api::Vulkan;

pub use ash;

use crate::shader::submit_lock::SubmitLock;
use crate::{GpuContextHandle, GpuError, GpuStorage, GpuTicket};

#[cfg(test)]
#[path = "raw_vulkan_tests.rs"]
mod tests;

/// wgpu's Vulkan objects behind a context (see the module docs for what may be done with them).
/// The `ash` tables are copies sharing wgpu's handles.
#[derive(Clone)]
pub struct RawVulkan {
    pub entry: ash::Entry,
    pub instance: ash::Instance,
    pub physical_device: vk::PhysicalDevice,
    pub device: ash::Device,
    /// Daedalus's queue: never submit on it directly ([`GpuContextHandle::submit_raw`]).
    pub queue: vk::Queue,
    pub queue_family_index: u32,
    pub queue_index: u32,
    /// `storageBuffer8BitAccess` is enabled: shaders may use 8-bit types in storage buffers.
    pub storage_buffer_8bit: bool,
    /// `uniformAndStorageBuffer8BitAccess` is enabled.
    pub uniform_and_storage_buffer_8bit: bool,
    /// The queue family's `timestampValidBits` (0: no timestamps on this queue).
    pub timestamp_valid_bits: u32,
    /// Nanoseconds per timestamp tick (`timestampPeriod`).
    pub timestamp_period_ns: f32,
}

/// A [`GpuStorage`]'s `VkBuffer` and the range of it the storage covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RawVkBuffer {
    pub buffer: vk::Buffer,
    /// Byte offset of the storage in `buffer` (bind descriptors at `offset`, `size`).
    pub offset: u64,
    pub size: u64,
}

impl GpuContextHandle {
    /// The context's Vulkan instance, physical device, device and queue. `None` when the
    /// context is not wgpu on Vulkan.
    ///
    /// # Safety
    /// Follow the module docs' sync contract: never submit on the queue directly, never destroy
    /// the returned objects, and use them only while the context lives.
    pub unsafe fn raw_vulkan(&self) -> Option<RawVulkan> {
        let (device, queue) = self.wgpu()?;
        // SAFETY: only reads handles; the caller upholds their use.
        unsafe {
            let hal = device.as_hal::<Vulkan>()?;
            let hal_queue = queue.as_hal::<Vulkan>()?;
            let shared = hal.shared_instance();
            let instance = shared.raw_instance();
            let physical_device = hal.raw_physical_device();
            let family = hal.queue_family_index();
            let timestamp_valid_bits = instance
                .get_physical_device_queue_family_properties(physical_device)
                .get(family as usize)
                .map_or(0, |props| props.timestamp_valid_bits);
            let timestamp_period_ns = instance
                .get_physical_device_properties(physical_device)
                .limits
                .timestamp_period;
            let eight_bit =
                crate::wgpu_backend::dmabuf::enabled_eight_bit(hal.raw_device().handle());
            Some(RawVulkan {
                entry: shared.entry().clone(),
                instance: shared.raw_instance().clone(),
                physical_device: hal.raw_physical_device(),
                device: hal.raw_device().clone(),
                queue: hal_queue.as_raw(),
                queue_family_index: family,
                queue_index: hal.queue_index(),
                storage_buffer_8bit: eight_bit.storage_buffer,
                uniform_and_storage_buffer_8bit: eight_bit.uniform_and_storage_buffer,
                timestamp_valid_bits,
                timestamp_period_ns,
            })
        }
    }

    /// Record raw Vulkan commands with `record` (given the device and a command buffer in the
    /// recording state) and submit them on Daedalus's queue, bracketed by full memory barriers;
    /// see the module docs. Do not begin, end or submit the command buffer.
    ///
    /// # Safety
    /// The recorded commands must be valid Vulkan usage on this device, and everything they use
    /// must stay alive until the returned ticket completes.
    pub unsafe fn submit_raw(
        &self,
        record: impl FnOnce(&ash::Device, vk::CommandBuffer),
    ) -> Result<GpuTicket, GpuError> {
        let (device, queue) = self.wgpu_locked().ok_or(GpuError::Unsupported)?;
        // SAFETY: the device handle is only used to record into wgpu's open command buffer.
        let raw = unsafe { device.as_hal::<Vulkan>() }
            .ok_or(GpuError::Unsupported)?
            .raw_device()
            .clone();
        let mut encoder = device.create_command_encoder(&Default::default());
        // SAFETY: wgpu hands over its open command buffer; nothing else touches the encoder
        // while it records, and it is neither ended nor submitted here.
        let recorded = unsafe {
            encoder.as_hal_mut::<Vulkan, _, _>(|encoder| {
                let cmd = encoder?.raw_handle();
                full_barrier(&raw, cmd);
                record(&raw, cmd);
                full_barrier(&raw, cmd);
                Some(())
            })
        };
        recorded.ok_or(GpuError::Unsupported)?;
        Ok(GpuTicket::submit_on(device, &queue, [encoder.finish()]))
    }
}

/// Every earlier write available and visible to every later access, host reads included.
fn full_barrier(device: &ash::Device, cmd: vk::CommandBuffer) {
    let barrier = vk::MemoryBarrier::default()
        .src_access_mask(vk::AccessFlags::MEMORY_WRITE)
        .dst_access_mask(
            vk::AccessFlags::MEMORY_READ
                | vk::AccessFlags::MEMORY_WRITE
                | vk::AccessFlags::HOST_READ,
        );
    // SAFETY: `cmd` is recording on `device`.
    unsafe {
        device.cmd_pipeline_barrier(
            cmd,
            vk::PipelineStageFlags::ALL_COMMANDS,
            vk::PipelineStageFlags::ALL_COMMANDS | vk::PipelineStageFlags::HOST,
            vk::DependencyFlags::empty(),
            &[barrier],
            &[],
            &[],
        )
    };
}

impl GpuStorage {
    /// The storage's `VkBuffer` and range. `None` off Vulkan.
    ///
    /// # Safety
    /// Do not destroy the buffer; use it only while this storage lives, and from commands
    /// recorded through [`GpuContextHandle::submit_raw`] (see the module docs).
    pub unsafe fn raw_vk_buffer(&self) -> Option<RawVkBuffer> {
        // SAFETY: only reads the handle.
        let buffer = unsafe { self.buffer().as_hal::<Vulkan>()?.raw_handle() };
        Some(RawVkBuffer {
            buffer,
            offset: 0,
            size: self.size(),
        })
    }
}

/// Slots a [`RawSubmitter`] has unless told otherwise.
pub const DEFAULT_RAW_SLOTS: usize = 3;

/// A ring of primary command buffers on Daedalus's queue, each with its own fence, for raw
/// submits that allocate nothing in steady state (see the module docs for the sync contract).
/// [`GpuContextHandle::submit_raw`] stays the simple path; use this one per frame.
///
/// # Ring
/// Submit `i` uses slot `i mod slots`. A submit first waits for that slot's previous submission
/// (`vkWaitForFences`, which returns at once when it has finished). So with `slots` submissions in
/// flight the next submit blocks until the oldest finishes. Size the ring to the frames in flight;
/// to avoid the wait, call [`RawTicket::try_complete`] on the oldest ticket before submitting.
///
/// # Lifetimes
/// The submitter holds a clone of the context, so the device outlives it. Dropping it waits for
/// every slot's last submission, then destroys its fences and command pool. Tickets borrow it.
pub struct RawSubmitter {
    /// Keeps the device alive for as long as this submitter's Vulkan objects exist.
    _ctx: GpuContextHandle,
    /// The context's submit lock, taken around each `vkQueueSubmit`.
    lock: SubmitLock,
    device: ash::Device,
    queue: vk::Queue,
    pool: vk::CommandPool,
    slots: Box<[Slot]>,
    next: Cell<usize>,
}

struct Slot {
    cmd: vk::CommandBuffer,
    /// Signaled when the slot is free; unsignaled from its submit until the work finishes.
    fence: vk::Fence,
    /// Bumped by each submit into the slot. A ticket whose generation is older is complete: the
    /// slot is reused only after its fence signaled.
    generation: Cell<u64>,
}

/// A submission of a [`RawSubmitter`]: [`RawTicket::try_complete`] polls it without blocking,
/// [`RawTicket::wait`] blocks until it has finished. A ticket whose slot has been reused reports
/// complete, because the reuse waited for the work.
pub struct RawTicket<'a> {
    submitter: &'a RawSubmitter,
    slot: usize,
    generation: u64,
}

impl RawSubmitter {
    /// A submitter with [`DEFAULT_RAW_SLOTS`] slots. `Unsupported` off Vulkan.
    pub fn new(ctx: &GpuContextHandle) -> Result<Self, GpuError> {
        Self::with_slots(ctx, DEFAULT_RAW_SLOTS)
    }

    /// A submitter with `slots` command buffers (at least one). `Unsupported` off Vulkan.
    pub fn with_slots(ctx: &GpuContextHandle, slots: usize) -> Result<Self, GpuError> {
        // SAFETY: only reads the handles; this submitter uses them under the sync contract.
        let raw = unsafe { ctx.raw_vulkan() }.ok_or(GpuError::Unsupported)?;
        let lock = ctx.submit_lock().ok_or(GpuError::Unsupported)?;
        let count = slots.max(1);
        let d = &raw.device;
        // SAFETY: valid create infos on the context's live device; each object created here is
        // destroyed in `Drop` (or on the error paths below).
        unsafe {
            let pool_info = vk::CommandPoolCreateInfo::default()
                .flags(vk::CommandPoolCreateFlags::RESET_COMMAND_BUFFER)
                .queue_family_index(raw.queue_family_index);
            let pool = d.create_command_pool(&pool_info, None).map_err(vk_error)?;
            let alloc = vk::CommandBufferAllocateInfo::default()
                .command_pool(pool)
                .level(vk::CommandBufferLevel::PRIMARY)
                .command_buffer_count(count as u32);
            let cmds = match d.allocate_command_buffers(&alloc) {
                Ok(cmds) => cmds,
                Err(err) => {
                    d.destroy_command_pool(pool, None);
                    return Err(vk_error(err));
                }
            };
            let mut built: Vec<Slot> = Vec::with_capacity(count);
            for cmd in cmds {
                let info = vk::FenceCreateInfo::default().flags(vk::FenceCreateFlags::SIGNALED);
                match d.create_fence(&info, None) {
                    Ok(fence) => built.push(Slot {
                        cmd,
                        fence,
                        generation: Cell::new(0),
                    }),
                    Err(err) => {
                        for slot in &built {
                            d.destroy_fence(slot.fence, None);
                        }
                        d.destroy_command_pool(pool, None);
                        return Err(vk_error(err));
                    }
                }
            }
            Ok(Self {
                _ctx: ctx.clone(),
                lock,
                device: d.clone(),
                queue: raw.queue,
                pool,
                slots: built.into_boxed_slice(),
                next: Cell::new(0),
            })
        }
    }

    /// The number of slots (in-flight submissions the ring holds).
    pub fn slots(&self) -> usize {
        self.slots.len()
    }

    /// Record `record` (given the device and a command buffer in the recording state) between two
    /// full memory barriers, and submit the buffer on Daedalus's queue with the next slot's fence.
    /// Waits first for that slot's previous submission (see the struct docs). Do not begin, end or
    /// submit the command buffer. `record` runs outside the submit lock, so it may submit through
    /// Daedalus.
    ///
    /// # Safety
    /// The recorded commands must be valid Vulkan usage on this device, and everything they use
    /// must stay alive until the returned ticket completes. Do not submit on the queue directly.
    pub unsafe fn submit(
        &self,
        record: impl FnOnce(&ash::Device, vk::CommandBuffer),
    ) -> Result<RawTicket<'_>, GpuError> {
        let index = self.next.get();
        let slot = &self.slots[index];
        let d = &self.device;
        let cmd = slot.cmd;
        // SAFETY: the slot belongs to this submitter. Once its previous submission has finished,
        // its fence and command buffer are free to reuse.
        unsafe {
            d.wait_for_fences(&[slot.fence], true, u64::MAX)
                .map_err(vk_error)?;
            d.reset_command_buffer(cmd, vk::CommandBufferResetFlags::empty())
                .map_err(vk_error)?;
            let begin = vk::CommandBufferBeginInfo::default()
                .flags(vk::CommandBufferUsageFlags::ONE_TIME_SUBMIT);
            d.begin_command_buffer(cmd, &begin).map_err(vk_error)?;
            full_barrier(d, cmd);
            record(d, cmd);
            full_barrier(d, cmd);
            d.end_command_buffer(cmd).map_err(vk_error)?;
            d.reset_fences(&[slot.fence]).map_err(vk_error)?;
        }
        let commands = [cmd];
        let submits = [vk::SubmitInfo::default().command_buffers(&commands)];
        let submitted = {
            let _lock = self.lock.lock();
            // SAFETY: the queue is Daedalus's; the context's submit lock serializes it with every
            // other submit on it.
            unsafe { d.queue_submit(self.queue, &submits, slot.fence) }
        };
        if let Err(err) = submitted {
            // Nothing ran, so the fence would never signal and the slot would stay busy. An empty
            // submission signals it once the queue's earlier work has finished.
            let _lock = self.lock.lock();
            // SAFETY: as above; the empty submission only signals the fence.
            let _ = unsafe { d.queue_submit(self.queue, &[], slot.fence) };
            return Err(vk_error(err));
        }
        let generation = slot.generation.get() + 1;
        slot.generation.set(generation);
        self.next.set((index + 1) % self.slots.len());
        Ok(RawTicket {
            submitter: self,
            slot: index,
            generation,
        })
    }
}

impl RawTicket<'_> {
    /// Whether this ticket's slot still holds its submission (not reused since).
    fn current(&self) -> bool {
        self.submitter.slots[self.slot].generation.get() == self.generation
    }

    /// `Some(())` once the work has finished. Never blocks (`vkGetFenceStatus`).
    pub fn try_complete(&self) -> Option<()> {
        if !self.current() {
            return Some(());
        }
        let fence = self.submitter.slots[self.slot].fence;
        // SAFETY: a fence of this submitter's device.
        match unsafe { self.submitter.device.get_fence_status(fence) } {
            Ok(true) => Some(()),
            _ => None,
        }
    }

    /// Block until the work has finished (`vkWaitForFences`).
    pub fn wait(&self) {
        if !self.current() {
            return;
        }
        let fence = self.submitter.slots[self.slot].fence;
        // SAFETY: as in `try_complete`. An error (device lost) means nothing more will finish.
        let _ = unsafe {
            self.submitter
                .device
                .wait_for_fences(&[fence], true, u64::MAX)
        };
    }
}

impl Drop for RawSubmitter {
    fn drop(&mut self) {
        // SAFETY: waits for every slot's last submission, so nothing on the GPU still uses the
        // objects, then destroys the ones this submitter created. The context outlives this.
        unsafe {
            for slot in self.slots.iter() {
                let _ = self.device.wait_for_fences(&[slot.fence], true, u64::MAX);
                self.device.destroy_fence(slot.fence, None);
            }
            self.device.destroy_command_pool(self.pool, None);
        }
    }
}

fn vk_error(err: vk::Result) -> GpuError {
    GpuError::Internal(format!("Vulkan: {err:?}"))
}
