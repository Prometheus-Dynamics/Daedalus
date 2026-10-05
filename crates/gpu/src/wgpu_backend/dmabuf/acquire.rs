//! Handing an imported image from its producer to wgpu and back: queue family ownership transfers
//! and the GPU-side wait on the acquire fence.
//!
//! Every import makes one small submission right after the texture is registered:
//!
//! - A raw command buffer holds a `VkImageMemoryBarrier` acquiring the image from
//!   `VK_QUEUE_FAMILY_FOREIGN_EXT` (or `VK_QUEUE_FAMILY_EXTERNAL` without
//!   `VK_EXT_queue_family_foreign`) into wgpu's queue family, `GENERAL -> SHADER_READ_ONLY_OPTIMAL`
//!   (see "Layout on arrival" in `vulkan.rs`). Stages are `ALL_COMMANDS` on both sides and the
//!   destination access is `MEMORY_READ | MEMORY_WRITE`, so it chains with the fence wait before it
//!   and with whatever barrier wgpu records for the first real use after it.
//! - The texture is registered with wgpu in `TextureUses::RESOURCE` (which wgpu-hal maps to
//!   `SHADER_READ_ONLY_OPTIMAL`), and a second command buffer in the same submission calls
//!   `transition_resources` to that state (wgpu 30 forbids mixing raw and wgpu commands in one
//!   encoder): wgpu records no barrier for it, but tracks the texture in this submission, so a
//!   texture dropped before its acquire executed is not destroyed (nor the dmabuf released) early.
//! - The fence wait is staged with wgpu-hal 30's `vulkan::Queue::add_wait_semaphore`
//!   (`ALL_COMMANDS`): a timeline value the watcher (`watcher.rs`) signals
//!   ([`AcquireFenceWait::Timeline`]), or the `sync_file` imported (temporarily) into a pooled
//!   binary semaphore ([`AcquireFenceWait::SyncFd`]).
//!
//! When the last handle is dropped, [`ImportDropToken`] submits the mirror image: wgpu transitions
//! the texture back to `RESOURCE`, then a raw barrier releases it `SHADER_READ_ONLY_OPTIMAL ->
//! GENERAL` to the foreign family, after all earlier work on the queue. wgpu keeps the texture
//! alive until that submission completed, so the keepalive (dropped by the texture's drop
//! callback) is only released once the producer may take the buffer back.
//!
//! `add_wait_semaphore` stages the wait for the *next* hal submission on the queue. If another
//! thread submits in between, that submission takes the wait; it is still earlier in submission
//! order than the acquire, and wgpu chains consecutive submissions with semaphores, so the acquire
//! (and everything after it) still runs after the fence. Either way the queue serializes behind the
//! fence: work submitted after an import does not start before the producer finished (or, on the
//! timeline path, the timeout passed).

use std::ffi::CStr;
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use ash::{ext, khr, vk};
use parking_lot::Mutex;
use wgpu::hal::api::Vulkan;
use wgpu::hal::vulkan as hal_vk;

use super::watcher::Watcher;
use crate::handles::GpuDropToken;
use crate::wgpu_backend::resources::ResourceDropToken;
use crate::{AcquireFenceWait, AcquireStatus, ExternalImportError};

/// Device extensions enabled when available: `SYNC_FD` fence waits and foreign queue ownership.
pub(super) const OPTIONAL_EXTENSIONS: [&CStr; 2] = [
    khr::external_semaphore_fd::NAME,
    ext::queue_family_foreign::NAME,
];

const SYNC_FD: vk::ExternalSemaphoreHandleTypeFlags = vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD;
const ALL_COMMANDS: vk::PipelineStageFlags = vk::PipelineStageFlags::ALL_COMMANDS;

/// Per-device state for handing imported images to wgpu.
pub(in crate::wgpu_backend) struct Acquire {
    handoff: Arc<Handoff>,
    mode: AcquireFenceWait,
    /// Present when the device has timeline semaphores.
    timeline: Option<Watcher>,
    /// Present when the device imports `SYNC_FD` semaphores.
    semaphores: Option<Arc<SemaphorePool>>,
    /// Status cells of fenced imports, reused once their image is gone.
    cells: Mutex<Vec<StatusCell>>,
}

/// A pending acquire fence, resolved for the active wait mode.
pub(super) enum GpuFence {
    /// Watched by the timeline watcher.
    Timeline(OwnedFd, Duration),
    /// Imported into a pooled binary semaphore, not yet submitted.
    SyncFd(PooledSemaphore),
}

impl Acquire {
    pub(super) fn new(
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        hal_dev: &hal_vk::Device,
    ) -> Self {
        let enabled = hal_dev.enabled_device_extensions();
        let foreign = if enabled.contains(&ext::queue_family_foreign::NAME) {
            vk::QUEUE_FAMILY_FOREIGN_EXT
        } else {
            vk::QUEUE_FAMILY_EXTERNAL
        };
        let raw = hal_dev.raw_device().clone();
        let semaphores = (enabled.contains(&khr::external_semaphore_fd::NAME)
            && sync_fd_importable(hal_dev))
        .then(|| {
            let instance = hal_dev.shared_instance().raw_instance();
            Arc::new(SemaphorePool {
                fd_api: khr::external_semaphore_fd::Device::new(instance, &raw),
                device: raw.clone(),
                free: Mutex::new(Vec::new()),
            })
        });
        let timeline = Watcher::new(hal_dev, device);
        let mode = if timeline.is_some() {
            AcquireFenceWait::Timeline
        } else if semaphores.is_some() {
            AcquireFenceWait::SyncFd
        } else {
            AcquireFenceWait::Cpu
        };
        Self {
            handoff: Arc::new(Handoff {
                device: device.clone(),
                queue: queue.clone(),
                raw,
                foreign,
                family: hal_dev.queue_family_index(),
            }),
            mode,
            timeline,
            semaphores,
            cells: Mutex::new(Vec::new()),
        }
    }

    pub(in crate::wgpu_backend) fn fence_wait(&self) -> AcquireFenceWait {
        self.mode
    }

    /// Switch to another wait mode the device supports (tests exercise every path on one device).
    #[cfg(test)]
    pub(in crate::wgpu_backend) fn set_fence_wait(&mut self, mode: AcquireFenceWait) -> bool {
        let available = match mode {
            AcquireFenceWait::Timeline => self.timeline.is_some(),
            AcquireFenceWait::SyncFd => self.semaphores.is_some(),
            AcquireFenceWait::Cpu => true,
        };
        if available {
            self.mode = mode;
        }
        available
    }

    /// Turn a pending fence into a GPU-side wait.
    ///
    /// An unbounded `timeout` (`Duration::MAX`) prefers the `SyncFd` wait where the device has one:
    /// no deadline to enforce, and on kernel drivers a wait on the fence itself blocks no thread.
    /// Returns the fd back when the GPU cannot wait for it (CPU-wait mode, a `SyncFd` device given
    /// an fd that is not a `sync_file`, or no watcher thread) so the caller waits on the CPU.
    pub(super) fn gpu_fence(
        &self,
        mut fence: OwnedFd,
        timeout: Duration,
    ) -> Result<GpuFence, OwnedFd> {
        if self.mode == AcquireFenceWait::Timeline
            && timeout == Duration::MAX
            && let Some(pool) = &self.semaphores
        {
            match pool.import(fence) {
                Ok(semaphore) => return Ok(GpuFence::SyncFd(semaphore)),
                // Not a sync_file: the watcher can still wait for it (without a deadline).
                Err(returned) => fence = returned,
            }
        }
        match self.mode {
            AcquireFenceWait::Timeline => match self.timeline.as_ref().map(Watcher::start) {
                Some(Ok(())) => Ok(GpuFence::Timeline(fence, timeout)),
                _ => Err(fence),
            },
            AcquireFenceWait::SyncFd => match &self.semaphores {
                Some(pool) => pool.import(fence).map(GpuFence::SyncFd),
                None => Err(fence),
            },
            AcquireFenceWait::Cpu => Err(fence),
        }
    }

    /// The drop token for an imported texture; `fenced` ones report [`AcquireStatus::Pending`]
    /// until the fence resolves.
    pub(super) fn token(&self, resource: ResourceDropToken, fenced: bool) -> ImportDropToken {
        ImportDropToken {
            status: fenced.then(|| self.cell()),
            release: OnceLock::new(),
            _resource: resource,
        }
    }

    /// A `Pending` status cell: a pooled one no image uses any more, or a new one.
    fn cell(&self) -> StatusCell {
        let mut cells = self.cells.lock();
        let cell = match cells.iter().find(|cell| Arc::strong_count(&cell.0) == 1) {
            Some(cell) => cell.clone(),
            None => {
                let cell = StatusCell(Arc::new(AtomicU8::new(0)));
                cells.push(cell.clone());
                cell
            }
        };
        cell.set(AcquireStatus::Pending);
        cell
    }

    /// Submit the ownership acquire for `texture`, waiting on `fence` first when given, and arm the
    /// release on `token`.
    pub(super) fn submit(
        &self,
        texture: &Arc<wgpu::Texture>,
        token: &Arc<ImportDropToken>,
        fence: Option<GpuFence>,
    ) -> Result<(), ExternalImportError> {
        let handoff = &self.handoff;
        let commands = handoff.transfer(texture, Transfer::Acquire)?;
        let status = || token.status.clone().unwrap_or_else(|| self.cell());
        let queue = &handoff.queue;
        let stage_wait = |semaphore: vk::Semaphore, value: Option<u64>| {
            // SAFETY: the wait is consumed by the next submission (the one below, or an earlier
            // one from another thread; see the module docs).
            let hal_queue = unsafe { queue.as_hal::<Vulkan>() }.ok_or_else(not_vulkan)?;
            hal_queue.add_wait_semaphore(semaphore, value, ALL_COMMANDS);
            Ok::<_, ExternalImportError>(())
        };
        match fence {
            None => {
                queue.submit(commands);
            }
            Some(GpuFence::Timeline(fence, timeout)) => {
                let watcher = self.timeline.as_ref().ok_or_else(not_vulkan)?;
                let value = watcher.watch(fence, timeout, status());
                stage_wait(watcher.semaphore(), Some(value))?;
                queue.submit(commands);
            }
            Some(GpuFence::SyncFd(semaphore)) => {
                stage_wait(semaphore.semaphore, None)?;
                queue.submit(commands);
                let (semaphore, pool) = semaphore.into_parts();
                let status = status();
                queue.on_submitted_work_done(move || {
                    status.set(AcquireStatus::Ready);
                    // A pool that is gone (backend dropped) leaks the semaphore rather than
                    // touching a device that may already be destroyed.
                    if let Some(pool) = pool.upgrade() {
                        pool.put(semaphore);
                    }
                });
            }
        }
        let _ = token
            .release
            .set((Arc::downgrade(handoff), Arc::clone(texture)));
        Ok(())
    }
}

fn not_vulkan() -> ExternalImportError {
    ExternalImportError::ImportFailed {
        reason: "the wgpu device is not using the Vulkan backend".into(),
    }
}

/// What a queue family transfer needs; import tokens hold it weakly for the release.
pub(super) struct Handoff {
    device: wgpu::Device,
    queue: wgpu::Queue,
    raw: ash::Device,
    /// Queue family the producer owns the image in.
    foreign: u32,
    /// wgpu's queue family.
    family: u32,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Transfer {
    Acquire,
    Release,
}

impl Handoff {
    /// The command buffers moving `texture` between the foreign family and wgpu, in submission
    /// order: the raw barrier first for an acquire, last for a release.
    fn transfer(
        &self,
        texture: &wgpu::Texture,
        direction: Transfer,
    ) -> Result<[wgpu::CommandBuffer; 2], ExternalImportError> {
        let failed = |reason: &str| ExternalImportError::ImportFailed {
            reason: format!("cannot record the queue family transfer: {reason}"),
        };
        // SAFETY: the raw handle is only used to record a barrier; wgpu keeps the image alive
        // through the `transition_resources` use below.
        let image = unsafe { texture.as_hal::<Vulkan>() }
            .map(|hal| unsafe { hal.raw_handle() })
            .ok_or_else(|| failed("not a Vulkan texture"))?;
        let (read_only, general) = (
            vk::ImageLayout::SHADER_READ_ONLY_OPTIMAL,
            vk::ImageLayout::GENERAL,
        );
        let barrier = vk::ImageMemoryBarrier::default()
            .image(image)
            .subresource_range(vk::ImageSubresourceRange {
                aspect_mask: vk::ImageAspectFlags::COLOR,
                base_mip_level: 0,
                level_count: 1,
                base_array_layer: 0,
                layer_count: 1,
            });
        let barrier = match direction {
            Transfer::Acquire => barrier
                .dst_access_mask(vk::AccessFlags::MEMORY_READ | vk::AccessFlags::MEMORY_WRITE)
                .old_layout(general)
                .new_layout(read_only)
                .src_queue_family_index(self.foreign)
                .dst_queue_family_index(self.family),
            Transfer::Release => barrier
                .src_access_mask(vk::AccessFlags::MEMORY_WRITE)
                .old_layout(read_only)
                .new_layout(general)
                .src_queue_family_index(self.family)
                .dst_queue_family_index(self.foreign),
        };

        // wgpu does not allow raw and wgpu commands in one encoder: the barrier gets its own.
        let mut raw = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("dmabuf-transfer"),
            });
        // SAFETY: only a pipeline barrier is recorded into the open command buffer.
        let recorded = unsafe {
            raw.as_hal_mut::<Vulkan, _, _>(|hal| {
                hal.map(|hal| {
                    self.raw.cmd_pipeline_barrier(
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
        let mut track = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("dmabuf-transfer-track"),
            });
        track.transition_resources(
            std::iter::empty(),
            std::iter::once(wgpu::TextureTransition {
                texture,
                selector: None,
                state: wgpu::wgt::TextureUses::RESOURCE,
            }),
        );
        Ok(match direction {
            Transfer::Acquire => [raw.finish(), track.finish()],
            Transfer::Release => [track.finish(), raw.finish()],
        })
    }
}

/// Drop token of an imported image: carries its [`AcquireStatus`] and releases the image to the
/// foreign queue family when the last handle goes away.
#[derive(Debug)]
pub(in crate::wgpu_backend) struct ImportDropToken {
    /// `None` without a pending fence.
    status: Option<StatusCell>,
    /// Set once the acquire was submitted (only then is there anything to release).
    release: OnceLock<(Weak<Handoff>, Arc<wgpu::Texture>)>,
    /// Untracks the texture; dropped after the release was submitted.
    _resource: ResourceDropToken,
}

impl GpuDropToken for ImportDropToken {
    fn acquire_status(&self) -> AcquireStatus {
        self.status
            .as_ref()
            .map_or(AcquireStatus::Ready, StatusCell::get)
    }
}

/// The acquire status of one image, shared with whatever resolves its fence (the watcher thread or
/// a submission callback) so that they never own the drop token.
#[derive(Debug, Clone)]
pub(super) struct StatusCell(Arc<AtomicU8>);

impl StatusCell {
    fn get(&self) -> AcquireStatus {
        match self.0.load(Ordering::Acquire) {
            0 => AcquireStatus::Ready,
            1 => AcquireStatus::Pending,
            _ => AcquireStatus::TimedOut,
        }
    }

    pub(super) fn set(&self, status: AcquireStatus) {
        self.0.store(status as u8, Ordering::Release);
    }
}

impl Drop for ImportDropToken {
    fn drop(&mut self) {
        let Some((handoff, texture)) = self.release.take() else {
            return;
        };
        // A backend that is gone has already waited for its queue; nothing to hand back.
        let Some(handoff) = handoff.upgrade() else {
            return;
        };
        match handoff.transfer(&texture, Transfer::Release) {
            Ok(commands) => {
                handoff.queue.submit(commands);
            }
            Err(err) => tracing::warn!(
                target: "daedalus_gpu::dmabuf",
                error = %err,
                "cannot release the imported image to the foreign queue family"
            ),
        }
    }
}

/// A `sync_file` imported into a pooled binary semaphore, not yet submitted.
pub(super) struct PooledSemaphore {
    semaphore: vk::Semaphore,
    /// `None` once submitted.
    pool: Option<Arc<SemaphorePool>>,
}

impl PooledSemaphore {
    /// Hand the submitted semaphore over to its completion callback.
    fn into_parts(mut self) -> (vk::Semaphore, Weak<SemaphorePool>) {
        let pool = self
            .pool
            .take()
            .map_or_else(Weak::new, |pool| Arc::downgrade(&pool));
        (self.semaphore, pool)
    }
}

impl Drop for PooledSemaphore {
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
    /// Import `fence` into a pooled semaphore; the fd comes back when it is not a `sync_file`.
    fn import(self: &Arc<Self>, fence: OwnedFd) -> Result<PooledSemaphore, OwnedFd> {
        let Ok(semaphore) = self.take() else {
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
        match unsafe { self.fd_api.import_semaphore_fd(&info) } {
            Ok(()) => Ok(PooledSemaphore {
                semaphore,
                pool: Some(self.clone()),
            }),
            Err(err) => {
                self.put(semaphore);
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
