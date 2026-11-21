//! Device work the host holds back until it says go.
//!
//! A gate submits, on the device's queue, an empty batch that waits for a timeline semaphore only
//! the host signals. A fence or semaphore signalled by a later submit covers everything earlier in
//! submission order, so until the gate opens nothing signalled after it signals. That makes "work
//! still pending" a state a test sets and ends, instead of a race against how fast the device is.
//!
//! A timeline semaphore may be waited on before anything signals it; an event may not be set by the
//! host while a command waits on it, so an event can't do this.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ash::vk;
use wgpu::hal::api::Vulkan;

use crate::device::Raw;
use crate::{Error, SubmitLock};

/// A closed gate on a device's queue. See the module doc.
pub struct HostGate {
    /// Set when the host opens the gate, or on drop.
    opened: AtomicBool,
    device: ash::Device,
    queue: vk::Queue,
    /// The timeline the wait is on: 0 while closed, 1 once open.
    timeline: vk::Semaphore,
    /// Signalled once the wait has passed.
    passed: vk::Fence,
    /// Whether a wait was submitted, so drop knows whether to wait for it.
    held: AtomicBool,
    /// The semaphore and fence are the device's.
    _keep: wgpu::Device,
}

impl HostGate {
    /// A closed gate. Nothing waits on it until [`HostGate::hold`].
    pub fn new(device: &wgpu::Device, queue: &wgpu::Queue) -> Result<Self, Error> {
        let raw = Raw::of(device).ok_or(Error::NotVulkan)?;
        // SAFETY: reads the queue's handle.
        let queue_handle = unsafe { queue.as_hal::<Vulkan>() }
            .ok_or(Error::NotVulkan)?
            .as_raw();
        let mut kind = vk::SemaphoreTypeCreateInfo::default()
            .semaphore_type(vk::SemaphoreType::TIMELINE)
            .initial_value(0);
        let info = vk::SemaphoreCreateInfo::default().push_next(&mut kind);
        // SAFETY: a timeline semaphore on a live device, destroyed in `Drop` once the wait on it
        // has passed.
        let timeline = unsafe { raw.device.create_semaphore(&info, None) }?;
        // SAFETY: a fence on a live device, destroyed in `Drop`.
        let passed = match unsafe {
            raw.device
                .create_fence(&vk::FenceCreateInfo::default(), None)
        } {
            Ok(fence) => fence,
            Err(error) => {
                // SAFETY: made above and never used.
                unsafe { raw.device.destroy_semaphore(timeline, None) };
                return Err(error.into());
            }
        };
        Ok(HostGate {
            opened: AtomicBool::new(false),
            device: raw.device,
            queue: queue_handle,
            timeline,
            passed,
            held: AtomicBool::new(false),
            _keep: device.clone(),
        })
    }

    /// Submits the wait: from here, everything submitted on the queue after it signals only once
    /// the gate opens. Once per gate.
    pub fn hold(&self, lock: &SubmitLock) -> Result<(), Error> {
        if self.held.swap(true, Ordering::AcqRel) {
            return Err(Error::Range);
        }
        let waits = [self.timeline];
        let values = [1u64];
        let stages = [vk::PipelineStageFlags::ALL_COMMANDS];
        let mut timeline =
            vk::TimelineSemaphoreSubmitInfo::default().wait_semaphore_values(&values);
        let submit = vk::SubmitInfo::default()
            .wait_semaphores(&waits)
            .wait_dst_stage_mask(&stages)
            .push_next(&mut timeline);
        let _held = lock.lock();
        // SAFETY: the queue is the device's, under its submit lock. The batch is empty and only
        // waits; the semaphore and fence outlive it (drop opens the gate and waits for the fence).
        let submitted = unsafe { self.device.queue_submit(self.queue, &[submit], self.passed) };
        if let Err(error) = submitted {
            self.held.store(false, Ordering::Release);
            return Err(error.into());
        }
        Ok(())
    }

    /// Opens the gate: the wait passes, and what was submitted after it can finish.
    pub fn open(&self) -> Result<(), Error> {
        self.opened.store(true, Ordering::Release);
        let info = vk::SemaphoreSignalInfo::default()
            .semaphore(self.timeline)
            .value(1);
        // SAFETY: a timeline semaphore of this device, signalled from the host to a value above
        // its current one.
        unsafe { self.device.signal_semaphore(&info) }?;
        Ok(())
    }

    /// Whether the wait has passed, up to `timeout`. True when nothing was held.
    pub fn passed(&self, timeout: Duration) -> bool {
        if !self.held.load(Ordering::Acquire) {
            return true;
        }
        let nanos = u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX);
        // SAFETY: the fence of the wait's submit.
        unsafe { self.device.wait_for_fences(&[self.passed], true, nanos) }.is_ok()
    }
}

impl Drop for HostGate {
    fn drop(&mut self) {
        // A gate dropped closed opens, so the queue isn't held forever.
        if !self.opened.load(Ordering::Acquire) {
            let _ = self.open();
        }
        if self.held.load(Ordering::Acquire) {
            // SAFETY: the fence of the wait's submit, which can now pass.
            let _ = unsafe { self.device.wait_for_fences(&[self.passed], true, u64::MAX) };
        }
        // SAFETY: nothing uses either now.
        unsafe {
            self.device.destroy_fence(self.passed, None);
            self.device.destroy_semaphore(self.timeline, None);
        }
    }
}
