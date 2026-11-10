//! A sync fd as a semaphore a submit can wait on.

use std::os::fd::{IntoRawFd, OwnedFd};

use ash::{khr, vk};

use crate::Error;
use crate::device::Raw;

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
