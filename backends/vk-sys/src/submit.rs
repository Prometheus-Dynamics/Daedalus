//! Commands the caller records, submitted on wgpu's queue.
//!
//! - Every submit here holds the device's [`SubmitLock`], so it never overlaps one of wgpu's.
//! - wgpu doesn't see these commands and places no barriers for them. Each recording is put
//!   between two full memory barriers: every earlier write is visible to it, and its writes are
//!   visible to everything later, host reads of mapped memory included.
//! - Data given to `Queue::write_buffer` reaches the device only with wgpu's next submit, so a
//!   recording doesn't see it until then.
//! - wgpu doesn't keep alive what a recording uses. Keep it until the ticket is done.

use std::cell::Cell;
use std::time::Duration;

use ash::vk;
use wgpu::hal::api::Vulkan;

use crate::device::Raw;
use crate::semaphore::Semaphore;
use crate::{Error, SubmitLock};

/// The most semaphores one submit waits on.
pub const MAX_WAITS: usize = 4;

/// A ring of command buffers on a device's queue, each with a fence. A submit allocates nothing.
///
/// Submit `i` uses slot `i mod slots`, and first waits for that slot's last submit to finish. So
/// with every slot in flight, the next submit blocks on the oldest.
pub struct Submitter {
    lock: SubmitLock,
    device: ash::Device,
    queue: vk::Queue,
    pool: vk::CommandPool,
    slots: Box<[Slot]>,
    next: Cell<usize>,
    /// Everything above is the device's.
    _keep: (wgpu::Device, wgpu::Queue),
}

struct Slot {
    commands: vk::CommandBuffer,
    /// Signaled while the slot is free.
    fence: vk::Fence,
    /// Counts submits into the slot. A ticket from an earlier one is done.
    generation: Cell<u64>,
}

/// One submit. Done when its work has finished, or when its slot has been used again.
pub struct Ticket<'a> {
    submitter: &'a Submitter,
    slot: usize,
    generation: u64,
}

impl Submitter {
    /// A ring of `slots` command buffers, at least one. `lock` is the device's submit lock.
    pub fn new(
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        lock: &SubmitLock,
        slots: usize,
    ) -> Result<Self, Error> {
        let raw = Raw::of(device).ok_or(Error::NotVulkan)?;
        // SAFETY: reads the queue's handle.
        let raw_queue = unsafe { queue.as_hal::<Vulkan>() }
            .ok_or(Error::NotVulkan)?
            .as_raw();
        let count = u32::try_from(slots.max(1)).map_err(|_| Error::Range)?;
        let vk_device = raw.device;
        let pool_info = vk::CommandPoolCreateInfo::default()
            .flags(vk::CommandPoolCreateFlags::RESET_COMMAND_BUFFER)
            .queue_family_index(raw.family);
        // SAFETY: valid create infos on a live device. Everything made here is destroyed in
        // `Drop`, or on the error paths below.
        unsafe {
            let pool = vk_device.create_command_pool(&pool_info, None)?;
            let allocate = vk::CommandBufferAllocateInfo::default()
                .command_pool(pool)
                .level(vk::CommandBufferLevel::PRIMARY)
                .command_buffer_count(count);
            let buffers = match vk_device.allocate_command_buffers(&allocate) {
                Ok(buffers) => buffers,
                Err(error) => {
                    vk_device.destroy_command_pool(pool, None);
                    return Err(error.into());
                }
            };
            let mut built = Vec::with_capacity(buffers.len());
            for commands in buffers {
                let info = vk::FenceCreateInfo::default().flags(vk::FenceCreateFlags::SIGNALED);
                match vk_device.create_fence(&info, None) {
                    Ok(fence) => built.push(Slot {
                        commands,
                        fence,
                        generation: Cell::new(0),
                    }),
                    Err(error) => {
                        for slot in &built {
                            vk_device.destroy_fence(slot.fence, None);
                        }
                        vk_device.destroy_command_pool(pool, None);
                        return Err(error.into());
                    }
                }
            }
            Ok(Submitter {
                lock: lock.clone(),
                device: vk_device,
                queue: raw_queue,
                pool,
                slots: built.into_boxed_slice(),
                next: Cell::new(0),
                _keep: (device.clone(), queue.clone()),
            })
        }
    }

    pub fn slots(&self) -> usize {
        self.slots.len()
    }

    /// Records with `record`, which is given the device and a command buffer that is recording,
    /// and submits it. The submit first waits on each of `waits`, at most [`MAX_WAITS`]. `record`
    /// must not begin, end or submit the command buffer. It runs outside the submit lock.
    ///
    /// # Safety
    ///
    /// What `record` records must be valid Vulkan on this device, and everything it uses,
    /// `waits` included, must stay alive until the ticket is done.
    // SAFETY: the contract is in the doc comment above.
    pub unsafe fn submit(
        &self,
        record: impl FnOnce(&ash::Device, vk::CommandBuffer),
        waits: &[&Semaphore],
    ) -> Result<Ticket<'_>, Error> {
        if waits.len() > MAX_WAITS {
            return Err(Error::Range);
        }
        let mut semaphores = [vk::Semaphore::null(); MAX_WAITS];
        for (out, wait) in semaphores.iter_mut().zip(waits) {
            *out = wait.raw;
        }
        // SAFETY: the caller's contract, passed on.
        unsafe { self.submit_waiting(record, semaphores, waits.len()) }
    }

    /// As [`Submitter::submit`], waiting on at most one semaphore given by handle, for this
    /// crate's own imports.
    ///
    /// # Safety
    ///
    /// As [`Submitter::submit`]. The semaphore, when there is one, must be live until the
    /// submit has finished.
    // SAFETY: the contract is in the doc comment above.
    pub(crate) unsafe fn submit_raw(
        &self,
        record: impl FnOnce(&ash::Device, vk::CommandBuffer),
        wait: Option<vk::Semaphore>,
    ) -> Result<(), Error> {
        let mut semaphores = [vk::Semaphore::null(); MAX_WAITS];
        semaphores[0] = wait.unwrap_or(vk::Semaphore::null());
        // SAFETY: the caller's contract, passed on.
        unsafe { self.submit_waiting(record, semaphores, usize::from(wait.is_some())) }.map(drop)
    }

    /// # Safety
    ///
    /// As [`Submitter::submit`], for the first `count` of `semaphores`.
    // SAFETY: the contract is in the doc comment above.
    unsafe fn submit_waiting(
        &self,
        record: impl FnOnce(&ash::Device, vk::CommandBuffer),
        semaphores: [vk::Semaphore; MAX_WAITS],
        count: usize,
    ) -> Result<Ticket<'_>, Error> {
        let index = self.next.get();
        let slot = self.slots.get(index).ok_or(Error::Range)?;
        let vk_device = &self.device;
        // SAFETY: the slot is this submitter's. Once its last submit has finished, its fence
        // and command buffer are free to use again. The recording is the caller's contract.
        unsafe {
            vk_device.wait_for_fences(&[slot.fence], true, u64::MAX)?;
            vk_device.reset_command_buffer(slot.commands, vk::CommandBufferResetFlags::empty())?;
            let begin = vk::CommandBufferBeginInfo::default()
                .flags(vk::CommandBufferUsageFlags::ONE_TIME_SUBMIT);
            vk_device.begin_command_buffer(slot.commands, &begin)?;
            full_barrier(vk_device, slot.commands);
            record(vk_device, slot.commands);
            full_barrier(vk_device, slot.commands);
            vk_device.end_command_buffer(slot.commands)?;
            vk_device.reset_fences(&[slot.fence])?;
        }
        let stages = [vk::PipelineStageFlags::ALL_COMMANDS; MAX_WAITS];
        let commands = [slot.commands];
        let info = vk::SubmitInfo::default()
            .command_buffers(&commands)
            .wait_semaphores(semaphores.get(..count).unwrap_or(&[]))
            .wait_dst_stage_mask(stages.get(..count).unwrap_or(&[]));
        let submitted = {
            let _held = self.lock.lock();
            // SAFETY: the queue is the device's, and the submit lock keeps this from
            // overlapping any other submit on it.
            unsafe { vk_device.queue_submit(self.queue, &[info], slot.fence) }
        };
        if let Err(error) = submitted {
            // Nothing ran, so the fence would never signal and the slot would stay busy. An
            // empty submit signals it once the queue's earlier work is done.
            let _held = self.lock.lock();
            // SAFETY: as above. The empty submit only signals the fence.
            let _ = unsafe { vk_device.queue_submit(self.queue, &[], slot.fence) };
            return Err(error.into());
        }
        let generation = slot.generation.get() + 1;
        slot.generation.set(generation);
        self.next.set((index + 1) % self.slots.len());
        Ok(Ticket {
            submitter: self,
            slot: index,
            generation,
        })
    }
}

/// Every earlier write is visible to every later access, host reads included.
///
/// # Safety
///
/// `commands` must be recording on `device`.
unsafe fn full_barrier(device: &ash::Device, commands: vk::CommandBuffer) {
    let barrier = vk::MemoryBarrier::default()
        .src_access_mask(vk::AccessFlags::MEMORY_WRITE)
        .dst_access_mask(
            vk::AccessFlags::MEMORY_READ
                | vk::AccessFlags::MEMORY_WRITE
                | vk::AccessFlags::HOST_READ,
        );
    // SAFETY: the caller's contract.
    unsafe {
        device.cmd_pipeline_barrier(
            commands,
            vk::PipelineStageFlags::ALL_COMMANDS,
            vk::PipelineStageFlags::ALL_COMMANDS | vk::PipelineStageFlags::HOST,
            vk::DependencyFlags::empty(),
            &[barrier],
            &[],
            &[],
        );
    }
}

impl Ticket<'_> {
    fn fence(&self) -> Option<vk::Fence> {
        let slot = self.submitter.slots.get(self.slot)?;
        (slot.generation.get() == self.generation).then_some(slot.fence)
    }

    /// Whether the work has finished. Never blocks.
    pub fn is_done(&self) -> bool {
        let Some(fence) = self.fence() else {
            return true;
        };
        // SAFETY: a fence of this submitter's device.
        let status = unsafe { self.submitter.device.get_fence_status(fence) };
        matches!(status, Ok(true))
    }

    /// Waits up to `timeout` for the work to finish, and says whether it has. An error, such as
    /// a lost device, is `false`: nothing more will finish.
    pub fn wait(&self, timeout: Duration) -> bool {
        let Some(fence) = self.fence() else {
            return true;
        };
        let nanos = u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX);
        // SAFETY: as in `is_done`.
        unsafe {
            self.submitter
                .device
                .wait_for_fences(&[fence], true, nanos)
                .is_ok()
        }
    }
}

impl Drop for Submitter {
    fn drop(&mut self) {
        // SAFETY: waits for each slot's last submit, so nothing on the device still uses these,
        // then destroys what this submitter made. The device outlives it through `_keep`.
        unsafe {
            for slot in &self.slots {
                let _ = self.device.wait_for_fences(&[slot.fence], true, u64::MAX);
                self.device.destroy_fence(slot.fence, None);
            }
            self.device.destroy_command_pool(self.pool, None);
        }
    }
}
