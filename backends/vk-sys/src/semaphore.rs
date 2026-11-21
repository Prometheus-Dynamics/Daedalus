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

/// A sync fd that signals once everything submitted to `queue` before it has finished: an empty
/// submit on wgpu's queue signals a semaphore made for export, and the semaphore's payload is
/// taken out as a sync_file. A semaphore signal waits for all earlier work in submission order,
/// so the fd covers wgpu's submits and raw ones alike. It takes `lock` for the submit; don't hold
/// it when calling.
///
/// `None` when the driver says the signal has already happened, which a sync fd may: then there
/// is nothing left to wait for. Refused with [`Error::NoSyncFdExport`] where the device can't
/// export a sync fd from a binary semaphore.
pub fn signal_sync_fd(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    lock: &SubmitLock,
) -> Result<Option<OwnedFd>, Error> {
    let raw = Raw::of(device).ok_or(Error::NotVulkan)?;
    if !raw.has(khr::external_semaphore_fd::NAME) || !exports_sync_fd(&raw) {
        return Err(Error::NoSyncFdExport);
    }
    // SAFETY: reads the queue's handle.
    let raw_queue = unsafe { queue.as_hal::<Vulkan>() }
        .ok_or(Error::NotVulkan)?
        .as_raw();
    let mut export = vk::ExportSemaphoreCreateInfo::default()
        .handle_types(vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD);
    let info = vk::SemaphoreCreateInfo::default().push_next(&mut export);
    // SAFETY: a binary semaphore on a live device, destroyed below on every path.
    let semaphore = unsafe { raw.device.create_semaphore(&info, None) }?;
    let signals = [semaphore];
    let submit = vk::SubmitInfo::default().signal_semaphores(&signals);
    let submitted = {
        let _held = lock.lock();
        // SAFETY: the queue is the device's, and the submit lock keeps this from overlapping
        // any other submit on it. The submit records nothing and only signals.
        unsafe {
            raw.device
                .queue_submit(raw_queue, &[submit], vk::Fence::null())
        }
    };
    let table = khr::external_semaphore_fd::Device::new(&raw.instance, &raw.device);
    let taken = submitted.map_err(Error::from).and_then(|()| {
        let get = vk::SemaphoreGetFdInfoKHR::default()
            .semaphore(semaphore)
            .handle_type(vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD);
        // SAFETY: the semaphore was made for sync fd export and has a signal pending. Taking
        // the fd out moves the payload into it, as a wait would, and leaves nothing pending.
        unsafe { table.get_semaphore_fd(&get) }.map_err(Error::from)
    });
    if taken.is_err() && submitted.is_ok() {
        // The signal is still pending on the semaphore: let the queue finish it first.
        let _held = lock.lock();
        // SAFETY: the queue is the device's, under the submit lock.
        let _ = unsafe { raw.device.queue_wait_idle(raw_queue) };
    }
    // SAFETY: made above; nothing is pending on it now.
    unsafe { raw.device.destroy_semaphore(semaphore, None) };
    let fd = taken?;
    // -1 means the payload had already signalled: a sync fd export may say so.
    if fd < 0 {
        return Ok(None);
    }
    // SAFETY: the call returned a new fd that nothing else owns.
    Ok(Some(unsafe { OwnedFd::from_raw_fd(fd) }))
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
