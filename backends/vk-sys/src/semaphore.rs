//! A sync fd as a semaphore a submit can wait on.

use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd};

use ash::{khr, vk};
use wgpu::hal::api::Vulkan;

use crate::device::Raw;
use crate::{Error, SubmitLock};

/// A binary semaphore holding one imported fence. Waiting on it in a submit consumes the fence.
pub struct Semaphore {
    pub(crate) raw: vk::Semaphore,
    device: ash::Device,
    /// The semaphore is the device's.
    _keep: wgpu::Device,
}

impl Semaphore {
    /// Imports `fence`, a sync fd, or an already signaled fence when there is none. The
    /// semaphore owns the fd from here.
    pub fn import_sync_fd(device: &wgpu::Device, fence: Option<OwnedFd>) -> Result<Self, Error> {
        let raw = Raw::of(device).ok_or(Error::NotVulkan)?;
        if !raw.has(khr::external_semaphore_fd::NAME) {
            return Err(Error::NoSyncFdImport);
        }
        // SAFETY: a plain binary semaphore on a live device, destroyed in `Drop` or below.
        let semaphore = unsafe {
            raw.device
                .create_semaphore(&vk::SemaphoreCreateInfo::default(), None)
        }?;
        let fd = fence.map_or(-1, IntoRawFd::into_raw_fd);
        let info = vk::ImportSemaphoreFdInfoKHR::default()
            .semaphore(semaphore)
            .flags(vk::SemaphoreImportFlags::TEMPORARY)
            .handle_type(vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD)
            .fd(fd);
        let table = khr::external_semaphore_fd::Device::new(&raw.instance, &raw.device);
        // SAFETY: the extension is enabled, and the fd is this call's, or -1 for a fence that
        // has already signaled. On success Vulkan owns the fd.
        if let Err(error) = unsafe { table.import_semaphore_fd(&info) } {
            // SAFETY: the import failed, so the fd is still ours to close, and the semaphore
            // was made above.
            unsafe {
                if fd >= 0 {
                    drop(<OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(fd));
                }
                raw.device.destroy_semaphore(semaphore, None);
            }
            return Err(error.into());
        }
        Ok(Semaphore {
            raw: semaphore,
            device: raw.device,
            _keep: device.clone(),
        })
    }
}

impl Drop for Semaphore {
    fn drop(&mut self) {
        // SAFETY: made by this device and owned here. A submit that waits on it borrows it
        // until its ticket is done.
        unsafe { self.device.destroy_semaphore(self.raw, None) };
    }
}

/// Hands out sync fds that signal once everything submitted to a queue before them has finished:
/// an empty submit on wgpu's queue signals a semaphore made for export, and the semaphore's
/// payload is taken out as a sync_file. A semaphore signal waits for all earlier work in
/// submission order, so the fd covers wgpu's submits and raw ones alike.
///
/// A semaphore can't be destroyed while the submit that signalled it may still be running, so
/// each one comes with a fence and is used again only once that fence has signalled.
pub struct SyncFdSignal {
    device: ash::Device,
    table: khr::external_semaphore_fd::Device,
    queue: vk::Queue,
    /// Semaphores with the fence of their last submit, free once that fence has signalled.
    slots: std::sync::Mutex<Vec<(vk::Semaphore, vk::Fence)>>,
    /// The semaphores are the device's.
    _keep: wgpu::Device,
}

/// The most semaphores a signaller keeps. Past it the oldest is waited for.
const MAX_SLOTS: usize = 64;

impl SyncFdSignal {
    /// A signaller for `queue`. Refused with [`Error::NoSyncFdExport`] where the device can't
    /// export a sync fd from a binary semaphore.
    pub fn new(device: &wgpu::Device, queue: &wgpu::Queue) -> Result<Self, Error> {
        let raw = Raw::of(device).ok_or(Error::NotVulkan)?;
        if !raw.has(khr::external_semaphore_fd::NAME) || !exports_sync_fd(&raw) {
            return Err(Error::NoSyncFdExport);
        }
        // SAFETY: reads the queue's handle.
        let queue_handle = unsafe { queue.as_hal::<Vulkan>() }
            .ok_or(Error::NotVulkan)?
            .as_raw();
        Ok(SyncFdSignal {
            table: khr::external_semaphore_fd::Device::new(&raw.instance, &raw.device),
            device: raw.device,
            queue: queue_handle,
            slots: std::sync::Mutex::new(Vec::new()),
            _keep: device.clone(),
        })
    }

    /// A sync fd that signals once everything submitted to the queue before this call has
    /// finished. `None` when the driver says the signal has already happened, which a sync fd
    /// may: then there is nothing left to wait for. It takes `lock` for the submit; don't hold
    /// it when calling.
    pub fn signal(&self, lock: &SubmitLock) -> Result<Option<OwnedFd>, Error> {
        let (semaphore, fence) = self.take()?;
        let signals = [semaphore];
        let submit = vk::SubmitInfo::default().signal_semaphores(&signals);
        let submitted = {
            let _held = lock.lock();
            // SAFETY: the queue is the device's, and the submit lock keeps this from overlapping
            // any other submit on it. The submit records nothing and only signals; the fence
            // was reset in `take`.
            unsafe { self.device.queue_submit(self.queue, &[submit], fence) }
        };
        if let Err(error) = submitted {
            // Nothing was submitted: both can go at once.
            self.destroy(semaphore, fence);
            return Err(error.into());
        }
        let get = vk::SemaphoreGetFdInfoKHR::default()
            .semaphore(semaphore)
            .handle_type(vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD);
        // SAFETY: the semaphore was made for sync fd export and has a signal pending. Taking
        // the fd out moves the payload into it, as a wait would, and leaves the semaphore
        // unsignalled with nothing pending, ready to be signalled again.
        let taken = unsafe { self.table.get_semaphore_fd(&get) };
        match taken {
            Ok(fd) => {
                self.give_back(semaphore, fence);
                // -1 means the payload had already signalled: a sync fd export may say so.
                if fd < 0 {
                    return Ok(None);
                }
                // SAFETY: the call returned a new fd that nothing else owns.
                Ok(Some(unsafe { OwnedFd::from_raw_fd(fd) }))
            }
            Err(error) => {
                // The signal is still on the semaphore: once its submit is done, both go.
                // SAFETY: the fence of the submit above.
                let _ = unsafe { self.device.wait_for_fences(&[fence], true, u64::MAX) };
                self.destroy(semaphore, fence);
                Err(error.into())
            }
        }
    }

    /// A free semaphore and its fence, reset: one whose last submit is done, or a new pair. With
    /// `MAX_SLOTS` kept and none free, the oldest is waited for.
    fn take(&self) -> Result<(vk::Semaphore, vk::Fence), Error> {
        let mut slots = self
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // SAFETY: fences of this device.
        let free = slots
            .iter()
            .position(|(_, fence)| unsafe { self.device.get_fence_status(*fence) } == Ok(true));
        let at = match (free, slots.len() >= MAX_SLOTS) {
            (Some(at), _) => Some(at),
            (None, true) => {
                // SAFETY: a fence of this device, with its submit made.
                unsafe { self.device.wait_for_fences(&[slots[0].1], true, u64::MAX) }?;
                Some(0)
            }
            (None, false) => None,
        };
        if let Some(at) = at {
            let (semaphore, fence) = slots.swap_remove(at);
            // SAFETY: the fence has signalled, so nothing uses it.
            unsafe { self.device.reset_fences(&[fence]) }?;
            return Ok((semaphore, fence));
        }
        drop(slots);
        let mut export = vk::ExportSemaphoreCreateInfo::default()
            .handle_types(vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD);
        let info = vk::SemaphoreCreateInfo::default().push_next(&mut export);
        // SAFETY: a binary semaphore and a fence on a live device, destroyed in `destroy` or
        // `Drop` once their submit is done.
        let semaphore = unsafe { self.device.create_semaphore(&info, None) }?;
        // SAFETY: as above.
        let fence = unsafe {
            self.device
                .create_fence(&vk::FenceCreateInfo::default(), None)
        };
        match fence {
            Ok(fence) => Ok((semaphore, fence)),
            Err(error) => {
                // SAFETY: made above and never submitted.
                unsafe { self.device.destroy_semaphore(semaphore, None) };
                Err(error.into())
            }
        }
    }

    fn give_back(&self, semaphore: vk::Semaphore, fence: vk::Fence) {
        self.slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((semaphore, fence));
    }

    fn destroy(&self, semaphore: vk::Semaphore, fence: vk::Fence) {
        // SAFETY: the caller has made sure no submit that uses either is still running.
        unsafe {
            self.device.destroy_semaphore(semaphore, None);
            self.device.destroy_fence(fence, None);
        }
    }

    /// How many semaphores the signaller keeps, for tests.
    pub fn kept(&self) -> usize {
        self.slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

impl Drop for SyncFdSignal {
    fn drop(&mut self) {
        let slots = std::mem::take(
            self.slots
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for (semaphore, fence) in slots {
            // SAFETY: waits for the submit that last used the pair, then destroys both.
            let _ = unsafe { self.device.wait_for_fences(&[fence], true, u64::MAX) };
            self.destroy(semaphore, fence);
        }
    }
}

/// Whether the physical device can export a sync fd from a binary semaphore.
fn exports_sync_fd(raw: &Raw) -> bool {
    let info = vk::PhysicalDeviceExternalSemaphoreInfo::default()
        .handle_type(vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD);
    let mut properties = vk::ExternalSemaphoreProperties::default();
    // SAFETY: a property query on the device's own physical device.
    unsafe {
        raw.instance
            .get_physical_device_external_semaphore_properties(
                raw.physical,
                &info,
                &mut properties,
            );
    }
    properties
        .external_semaphore_features
        .contains(vk::ExternalSemaphoreFeatureFlags::EXPORTABLE)
}
