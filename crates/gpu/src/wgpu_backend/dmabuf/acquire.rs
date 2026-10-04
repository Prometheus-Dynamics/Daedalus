//! Handing an imported image from its producer to wgpu: a queue family ownership acquire and,
//! where the device can import `sync_file`s, a GPU-side wait on the acquire fence.
//!
//! Every import makes one small submission right after the texture is registered:
//!
//! - A raw command buffer holds a `VkImageMemoryBarrier` acquiring the image from
//!   `VK_QUEUE_FAMILY_FOREIGN_EXT` (or `VK_QUEUE_FAMILY_EXTERNAL` without
//!   `VK_EXT_queue_family_foreign`) into wgpu's queue family, `GENERAL -> SHADER_READ_ONLY_OPTIMAL`
//!   (the convention Mesa-based compositors use for dmabufs, which keeps the contents instead of
//!   discarding them like a transition from `UNDEFINED`). Stages are `ALL_COMMANDS` on both sides
//!   and the destination access is `MEMORY_READ | MEMORY_WRITE`, so it chains with the fence wait
//!   before it and with whatever barrier wgpu records for the first real use after it.
//! - The texture is registered with wgpu in `TextureUses::RESOURCE` (which wgpu-hal maps to
//!   `SHADER_READ_ONLY_OPTIMAL`), and a second command buffer in the same submission calls
//!   `transition_resources` to that state (wgpu 30 forbids mixing raw and wgpu commands in one
//!   encoder): wgpu records no barrier for it, but tracks the texture in this submission, so a
//!   texture dropped before its acquire executed is not destroyed (nor the dmabuf released) early.
//! - With [`AcquireFenceWait::Gpu`] the acquire fence is imported into a binary semaphore
//!   (`VK_KHR_external_semaphore_fd`, `SYNC_FD`, temporary import) and staged with wgpu-hal 30's
//!   `vulkan::Queue::add_wait_semaphore` (`ALL_COMMANDS`) for this submission. The semaphore goes
//!   back to a pool once the submission completes (the temporary payload is consumed by the wait).
//!
//! `add_wait_semaphore` stages the wait for the *next* hal submission on the queue. If another
//! thread submits in between, that submission takes the wait; it is still earlier in submission
//! order than the acquire, and wgpu chains consecutive submissions with semaphores, so the acquire
//! (and everything after it) still runs after the fence. Either way the queue serializes behind the
//! fence: work submitted after an import does not start before the producer finished.

use std::ffi::CStr;
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd};
use std::sync::{Arc, Weak};

use ash::{ext, khr, vk};
use parking_lot::Mutex;
use wgpu::hal::api::Vulkan;
use wgpu::hal::vulkan as hal_vk;

use crate::{AcquireFenceWait, ExternalImportError};

/// Device extensions enabled when available: GPU-side fence waits and foreign queue ownership.
pub(super) const OPTIONAL_EXTENSIONS: [&CStr; 2] = [
    khr::external_semaphore_fd::NAME,
    ext::queue_family_foreign::NAME,
];

const SYNC_FD: vk::ExternalSemaphoreHandleTypeFlags = vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD;
const ALL_COMMANDS: vk::PipelineStageFlags = vk::PipelineStageFlags::ALL_COMMANDS;

/// Per-device state for handing imported images to wgpu.
pub(in crate::wgpu_backend) struct Acquire {
    device: ash::Device,
    /// Queue family the producer owns the image in.
    src_family: u32,
    /// wgpu's queue family.
    dst_family: u32,
    /// Present when acquire fences are waited for on the GPU.
    semaphores: Option<Arc<SemaphorePool>>,
}

impl Acquire {
    pub(super) fn new(hal_dev: &hal_vk::Device) -> Self {
        let enabled = hal_dev.enabled_device_extensions();
        let src_family = if enabled.contains(&ext::queue_family_foreign::NAME) {
            vk::QUEUE_FAMILY_FOREIGN_EXT
        } else {
            vk::QUEUE_FAMILY_EXTERNAL
        };
        let device = hal_dev.raw_device().clone();
        let semaphores = (enabled.contains(&khr::external_semaphore_fd::NAME)
            && sync_fd_importable(hal_dev))
        .then(|| {
            let instance = hal_dev.shared_instance().raw_instance();
            Arc::new(SemaphorePool {
                fd_api: khr::external_semaphore_fd::Device::new(instance, &device),
                device: device.clone(),
                free: Mutex::new(Vec::new()),
            })
        });
        Self {
            device,
            src_family,
            dst_family: hal_dev.queue_family_index(),
            semaphores,
        }
    }

    pub(in crate::wgpu_backend) fn fence_wait(&self) -> AcquireFenceWait {
        if self.semaphores.is_some() {
            AcquireFenceWait::Gpu
        } else {
            AcquireFenceWait::Cpu
        }
    }

    /// Fall back to blocking CPU waits (tests exercise both paths on one device).
    #[cfg(test)]
    pub(in crate::wgpu_backend) fn disable_gpu_wait(&mut self) {
        self.semaphores = None;
    }

    /// Turn a pending `sync_file` into a semaphore the acquire submission waits on.
    ///
    /// Returns the fd back when the GPU cannot wait for it (CPU-wait mode, or the fd is not a
    /// `sync_file`) so the caller waits on the CPU instead.
    pub(super) fn import_fence(&self, fence: OwnedFd) -> Result<GpuFence, OwnedFd> {
        let Some(pool) = &self.semaphores else {
            return Err(fence);
        };
        let Ok(semaphore) = pool.take() else {
            return Err(fence);
        };
        let raw = fence.into_raw_fd();
        let info = vk::ImportSemaphoreFdInfoKHR::default()
            .semaphore(semaphore)
            .flags(vk::SemaphoreImportFlags::TEMPORARY)
            .handle_type(SYNC_FD)
            .fd(raw);
        // SAFETY: `semaphore` is idle (pooled semaphores have no pending operations); on success
        // Vulkan owns `raw`.
        match unsafe { pool.fd_api.import_semaphore_fd(&info) } {
            Ok(()) => Ok(GpuFence {
                semaphore,
                pool: Some(pool.clone()),
            }),
            Err(err) => {
                pool.put(semaphore);
                tracing::debug!(
                    target: "daedalus_gpu::dmabuf",
                    error = %err,
                    "acquire fence is not an importable sync_file; waiting on the CPU"
                );
                // SAFETY: Vulkan does not take ownership of the fd when the import fails.
                Err(unsafe { OwnedFd::from_raw_fd(raw) })
            }
        }
    }

    /// Submit the ownership acquire for `texture`, waiting on `fence` first when given.
    pub(super) fn submit(
        &self,
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        texture: &wgpu::Texture,
        fence: Option<GpuFence>,
    ) -> Result<(), ExternalImportError> {
        let failed = |reason: &str| ExternalImportError::ImportFailed {
            reason: format!("cannot record the queue family acquire: {reason}"),
        };
        // SAFETY: the raw handle is only used to record a barrier; wgpu keeps the image alive
        // through the `transition_resources` use below.
        let image = unsafe { texture.as_hal::<Vulkan>() }
            .map(|hal| unsafe { hal.raw_handle() })
            .ok_or_else(|| failed("not a Vulkan texture"))?;
        let barrier = vk::ImageMemoryBarrier::default()
            .src_access_mask(vk::AccessFlags::empty())
            .dst_access_mask(vk::AccessFlags::MEMORY_READ | vk::AccessFlags::MEMORY_WRITE)
            .old_layout(vk::ImageLayout::GENERAL)
            .new_layout(vk::ImageLayout::SHADER_READ_ONLY_OPTIMAL)
            .src_queue_family_index(self.src_family)
            .dst_queue_family_index(self.dst_family)
            .image(image)
            .subresource_range(vk::ImageSubresourceRange {
                aspect_mask: vk::ImageAspectFlags::COLOR,
                base_mip_level: 0,
                level_count: 1,
                base_array_layer: 0,
                layer_count: 1,
            });

        // wgpu does not allow raw and wgpu commands in one encoder: the barrier gets its own.
        let mut raw = device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("dmabuf-acquire"),
        });
        // SAFETY: only a pipeline barrier is recorded into the open command buffer.
        let recorded = unsafe {
            raw.as_hal_mut::<Vulkan, _, _>(|hal| {
                hal.map(|hal| {
                    self.device.cmd_pipeline_barrier(
                        hal.raw_handle(),
                        ALL_COMMANDS,
                        ALL_COMMANDS,
                        vk::DependencyFlags::empty(),
                        &[],
                        &[],
                        &[barrier],
                    );
                })
                .is_some()
            })
        };
        if !recorded {
            return Err(failed("no Vulkan command encoder"));
        }
        let mut track = device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("dmabuf-acquire-track"),
        });
        track.transition_resources(
            std::iter::empty(),
            std::iter::once(wgpu::TextureTransition {
                texture,
                selector: None,
                state: wgpu::wgt::TextureUses::RESOURCE,
            }),
        );
        let commands = [raw.finish(), track.finish()];

        let Some(fence) = fence else {
            queue.submit(commands);
            return Ok(());
        };
        {
            // SAFETY: the wait is consumed by the next submission (the one below, or an earlier
            // one from another thread; see the module docs).
            let hal_queue =
                unsafe { queue.as_hal::<Vulkan>() }.ok_or_else(|| failed("not a Vulkan queue"))?;
            hal_queue.add_wait_semaphore(fence.semaphore, None, ALL_COMMANDS);
        }
        queue.submit(commands);
        let (semaphore, pool) = fence.into_parts();
        queue.on_submitted_work_done(move || {
            // A pool that is gone (backend dropped) leaks the semaphore rather than touching a
            // device that may already be destroyed.
            if let Some(pool) = pool.upgrade() {
                pool.put(semaphore);
            }
        });
        Ok(())
    }
}

/// An acquire fence imported into a pooled semaphore, not yet submitted.
pub(super) struct GpuFence {
    semaphore: vk::Semaphore,
    /// `None` once submitted.
    pool: Option<Arc<SemaphorePool>>,
}

impl GpuFence {
    /// Hand the submitted semaphore over to its completion callback.
    fn into_parts(mut self) -> (vk::Semaphore, Weak<SemaphorePool>) {
        let pool = self
            .pool
            .take()
            .map_or_else(Weak::new, |pool| Arc::downgrade(&pool));
        (self.semaphore, pool)
    }
}

impl Drop for GpuFence {
    /// Never submitted (the import failed later): the next import replaces the payload.
    fn drop(&mut self) {
        if let Some(pool) = &self.pool {
            pool.put(self.semaphore);
        }
    }
}

/// Binary semaphores for `SYNC_FD` imports, reused once their wait completed.
struct SemaphorePool {
    device: ash::Device,
    fd_api: khr::external_semaphore_fd::Device,
    free: Mutex<Vec<vk::Semaphore>>,
}

impl SemaphorePool {
    fn take(&self) -> Result<vk::Semaphore, vk::Result> {
        if let Some(semaphore) = self.free.lock().pop() {
            return Ok(semaphore);
        }
        // SAFETY: plain binary semaphore creation on a live device.
        unsafe {
            self.device
                .create_semaphore(&vk::SemaphoreCreateInfo::default(), None)
        }
    }

    fn put(&self, semaphore: vk::Semaphore) {
        self.free.lock().push(semaphore);
    }
}

impl Drop for SemaphorePool {
    fn drop(&mut self) {
        for semaphore in self.free.get_mut().drain(..) {
            // SAFETY: pooled semaphores have no pending operations; the pool is owned by the
            // backend, which drops it before its wgpu device.
            unsafe { self.device.destroy_semaphore(semaphore, None) };
        }
    }
}

/// Whether the device can import `sync_file`s into semaphores.
fn sync_fd_importable(hal_dev: &hal_vk::Device) -> bool {
    let instance = hal_dev.shared_instance().raw_instance();
    let info = vk::PhysicalDeviceExternalSemaphoreInfo::default().handle_type(SYNC_FD);
    let mut props = vk::ExternalSemaphoreProperties::default();
    // SAFETY: core 1.1 query (the import path requires a 1.1 instance) on the device's own
    // physical device.
    unsafe {
        instance.get_physical_device_external_semaphore_properties(
            hal_dev.raw_physical_device(),
            &info,
            &mut props,
        )
    };
    props
        .external_semaphore_features
        .contains(vk::ExternalSemaphoreFeatureFlags::IMPORTABLE)
}
