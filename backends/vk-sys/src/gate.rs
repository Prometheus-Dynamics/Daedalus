//! Device work the host holds back until it says go.
//!
//! A gate submits, on the device's queue, a command buffer that waits for an event only the host
//! sets. A fence or semaphore signalled by a later submit covers everything earlier in submission
//! order, so until the gate opens nothing signalled after it signals. That makes "work still
//! pending" a state a test sets and ends, instead of a race against how fast the device is.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ash::vk;

use crate::device::Raw;
use crate::submit::Submitter;
use crate::{Error, SubmitLock};

/// A closed gate on a device's queue. See the module doc.
pub struct HostGate {
    /// Set when the host opens the gate, or on drop.
    opened: AtomicBool,
    /// Submits the waits. Declared before `kept`: its drop waits for every wait to finish, and
    /// only then is the event destroyed.
    submitter: Submitter,
    kept: Event,
}

/// Destroys the event once nothing waits on it.
struct Event {
    raw: vk::Event,
    device: ash::Device,
    /// The event is the device's.
    _keep: wgpu::Device,
}

impl Drop for Event {
    fn drop(&mut self) {
        // SAFETY: made in `HostGate::new`. The submitter, dropped first, waited for every
        // command buffer that waits on it.
        unsafe { self.device.destroy_event(self.raw, None) };
    }
}

impl HostGate {
    /// A closed gate. Nothing waits on it until [`HostGate::hold`].
    pub fn new(
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        lock: &SubmitLock,
    ) -> Result<Self, Error> {
        let raw = Raw::of(device).ok_or(Error::NotVulkan)?;
        let submitter = Submitter::new(device, queue, lock, 1)?;
        // SAFETY: a plain event on a live device, destroyed by `Event`'s drop.
        let event = unsafe {
            raw.device
                .create_event(&vk::EventCreateInfo::default(), None)
        }?;
        Ok(HostGate {
            opened: AtomicBool::new(false),
            submitter,
            kept: Event {
                raw: event,
                device: raw.device,
                _keep: device.clone(),
            },
        })
    }

    /// Submits the wait: from here, everything submitted on the queue after it signals only once
    /// the gate opens. One wait at a time; a gate already open lets the wait straight through.
    pub fn hold(&self) -> Result<(), Error> {
        let event = self.kept.raw;
        let record = |device: &ash::Device, buffer: vk::CommandBuffer| {
            // SAFETY: recording into the submitter's command buffer. The event is set only
            // from the host, which the HOST source stage says.
            unsafe {
                device.cmd_wait_events(
                    buffer,
                    &[event],
                    vk::PipelineStageFlags::HOST,
                    vk::PipelineStageFlags::ALL_COMMANDS,
                    &[vk::MemoryBarrier::default()
                        .src_access_mask(vk::AccessFlags::HOST_WRITE)
                        .dst_access_mask(vk::AccessFlags::MEMORY_READ)],
                    &[],
                    &[],
                );
            }
        };
        // SAFETY: the recording waits on an event this gate keeps alive until the submitter,
        // dropped first, has waited for the submit.
        unsafe { self.submitter.submit(record, &[]) }.map(|_| ())
    }

    /// Opens the gate: what waits on it runs, and what was submitted after it can finish.
    pub fn open(&self) -> Result<(), Error> {
        self.opened.store(true, Ordering::Release);
        // SAFETY: an event of this device, set from the host.
        unsafe { self.kept.device.set_event(self.kept.raw) }?;
        Ok(())
    }

    /// Whether the wait has finished, up to `timeout`.
    pub fn passed(&self, timeout: Duration) -> bool {
        self.submitter.wait_idle_for(timeout)
    }
}

impl Drop for HostGate {
    fn drop(&mut self) {
        // A gate dropped closed opens, so the queue isn't held forever.
        if !self.opened.load(Ordering::Acquire) {
            let _ = self.open();
        }
    }
}
