//! Raw Vulkan handles behind a wgpu [`GpuContextHandle`] and its [`GpuStorage`], so kernels
//! written outside Daedalus (precompiled SPIR-V, such as Styx's gpuisp) run on Daedalus's device
//! against its buffers and imported camera frames (feature `gpu-vulkan-raw`, Linux).
//!
//! # Sync contract
//!
//! - **Daedalus submits.** wgpu serializes its own `vkQueueSubmit` calls behind a lock nothing
//!   outside it can take, and Vulkan requires a queue's submissions to be externally
//!   synchronized, so never call `vkQueueSubmit` (or `vkQueueWaitIdle`) on
//!   [`RawVulkan::queue`] yourself. Record commands through [`GpuContextHandle::submit_raw`]:
//!   they go into a command buffer of wgpu's, submitted on Daedalus's queue in order with
//!   Daedalus's own work.
//! - **Barriers around the recording.** wgpu does not see raw commands, so it cannot place
//!   barriers for them. `submit_raw` brackets them with full memory barriers (all earlier
//!   device writes, Daedalus's included, visible to them; theirs visible to all later work and
//!   to host reads of mapped memory). Inside the recording, barriers between your own commands
//!   are yours.
//! - **Waiting.** The returned [`GpuTicket`] completes when the commands have finished:
//!   `try_complete` polls without blocking, `wait` blocks. Work Daedalus submits afterwards
//!   (`GpuKernels::run`, `GpuTicket::submit`) is ordered after yours on the same queue; no
//!   semaphore is needed.
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

use ash::vk;
use wgpu::hal::api::Vulkan;

pub use ash;

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
        let (device, queue) = self.wgpu().ok_or(GpuError::Unsupported)?;
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
        Ok(GpuTicket::submit_on(device, queue, [encoder.finish()]))
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
