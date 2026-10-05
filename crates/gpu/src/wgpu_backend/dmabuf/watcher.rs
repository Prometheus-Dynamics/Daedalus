//! Acquire-fence waits with a timeout ([`AcquireFenceWait::Timeline`](crate::AcquireFenceWait)).
//!
//! Each device has one timeline semaphore (Vulkan 1.2 `timelineSemaphore`, which wgpu-hal enables
//! when the device has it) and, from the first pending fence on, one watcher thread. An import
//! reserves the next timeline value, hands its fence to the watcher, and stages a GPU wait for
//! that value (`add_wait_semaphore(semaphore, Some(value), ..)`), so the import does not wait for
//! its fence. The watcher `poll`s the pending fences plus an `eventfd` for new ones and
//! host-signals (`vkSignalSemaphore`) the timeline once a fence signals or its `acquire_timeout`
//! passes; a timed-out (or erroring) fence marks the image [`AcquireStatus::TimedOut`]. The hop
//! from fence to released GPU wait measures about 10 us on a desktop.
//!
//! Values are released strictly in order: signaling value `n` satisfies every wait for `<= n`, so
//! the watcher only advances past fences that all resolved. That costs nothing, because the waits
//! sit on one queue and execute in submission order anyway.
//!
//! Mesa caveat: a submission that waits for an unsignaled timeline value is a wait-before-signal,
//! which Mesa's common queue code (measured on RADV and lavapipe) hands to a submit thread. wgpu-hal
//! chains every submission to the previous one with binary semaphores, and Mesa only accepts a
//! binary wait once the signaling submission reached the kernel, so the next submission to the
//! device blocks its thread until the watcher releases the value. The bound still holds (at most
//! the timeout); the CPU wait just moves from the import to the next submission.
//!
//! Alternatives: a binary `SYNC_FD` semaphore ([`AcquireFenceWait::SyncFd`]) is a kernel fence,
//! so nothing blocks on kernel drivers, but it cannot be cancelled once submitted (no timeout);
//! `SYNC_FD` payloads cannot be imported into timeline semaphores; host-set `VkEvent`s must be set
//! before the waiting submission; a thread (or a blocking wait) per import costs more than one
//! shared `poll` loop, and the CPU wait blocks the importing thread. The watcher only polls fds, so
//! it also works on devices without `VK_KHR_external_semaphore_fd`, and for fence fds that are not
//! `sync_file`s. Because of the Mesa caveat, `SyncFd` is the default
//! ([`AcquireFenceMode::Auto`](crate::AcquireFenceMode::Auto)); this path is opt-in
//! ([`AcquireFenceMode::Timeline`](crate::AcquireFenceMode::Timeline)), and even then imports that
//! need no bound (`acquire_timeout == Duration::MAX`) use `SyncFd` where available.
//!
//! Lifetime: the watcher thread holds the shared state and the images' status cells, never the
//! backend or a drop token (dropping the last token submits a release, which could wait for a
//! value only the watcher signals). Dropping the [`Watcher`] (with the backend) stops and joins
//! the thread, signals every reserved value so no GPU wait is left hanging, waits for the queue to
//! go idle, and only then destroys the semaphore; its `wgpu::Device` clone keeps the device alive
//! until then. Steady state allocates nothing: the pending queue, the poll set and the status
//! cells are reused.
//!
//! [`AcquireFenceWait::SyncFd`]: crate::AcquireFenceWait::SyncFd

use std::collections::VecDeque;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use ash::vk;
use parking_lot::Mutex;
use wgpu::hal::vulkan as hal_vk;

use super::acquire::StatusCell;
use crate::AcquireStatus;

pub(super) struct Watcher {
    shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
    device: wgpu::Device,
}

struct Shared {
    raw: ash::Device,
    semaphore: vk::Semaphore,
    /// `eventfd` waking the thread for new fences and shutdown.
    wake: OwnedFd,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Last timeline value handed out.
    issued: u64,
    /// Last value signaled.
    signaled: u64,
    shutdown: bool,
    /// In value order.
    pending: VecDeque<Entry>,
}

struct Entry {
    value: u64,
    fence: OwnedFd,
    deadline: Option<Instant>,
    outcome: Option<AcquireStatus>,
    status: StatusCell,
}

impl Watcher {
    /// `None` when the device has no timeline semaphores (or the `eventfd` cannot be created).
    pub(super) fn new(hal_dev: &hal_vk::Device, device: &wgpu::Device) -> Option<Self> {
        if !timeline_supported(hal_dev) {
            return None;
        }
        let raw = hal_dev.raw_device().clone();
        // SAFETY: plain eventfd creation; the fd is owned right away.
        let wake = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if wake < 0 {
            return None;
        }
        // SAFETY: fresh fd we own.
        let wake = unsafe { OwnedFd::from_raw_fd(wake) };
        let mut kind = vk::SemaphoreTypeCreateInfo::default()
            .semaphore_type(vk::SemaphoreType::TIMELINE)
            .initial_value(0);
        // SAFETY: timeline semaphores are enabled (checked above); destroyed in `drop`.
        let semaphore = unsafe {
            raw.create_semaphore(
                &vk::SemaphoreCreateInfo::default().push_next(&mut kind),
                None,
            )
        }
        .ok()?;
        Some(Self {
            shared: Arc::new(Shared {
                raw,
                semaphore,
                wake,
                state: Mutex::new(State::default()),
            }),
            thread: Mutex::new(None),
            device: device.clone(),
        })
    }

    pub(super) fn semaphore(&self) -> vk::Semaphore {
        self.shared.semaphore
    }

    /// Start the watcher thread if it is not running yet.
    pub(super) fn start(&self) -> std::io::Result<()> {
        let mut thread = self.thread.lock();
        if thread.is_none() {
            let shared = self.shared.clone();
            *thread = Some(
                std::thread::Builder::new()
                    .name("daedalus-dmabuf-fences".into())
                    .spawn(move || shared.run())?,
            );
        }
        Ok(())
    }

    /// Watch `fence` (the thread must be [`start`](Self::start)ed) and return the timeline value
    /// the GPU waits for; `status` learns how the wait ended.
    pub(super) fn watch(&self, fence: OwnedFd, timeout: Duration, status: StatusCell) -> u64 {
        let mut state = self.shared.state.lock();
        state.issued += 1;
        let value = state.issued;
        state.pending.push_back(Entry {
            value,
            fence,
            deadline: Instant::now().checked_add(timeout),
            outcome: None,
            status,
        });
        drop(state);
        self.shared.wake();
        value
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.shared.state.lock().shutdown = true;
        self.shared.wake();
        if let Some(thread) = self.thread.get_mut().take() {
            let _ = thread.join();
        }
        // Release every GPU wait that is still queued; the images' contents are undefined.
        let state = std::mem::take(&mut *self.shared.state.lock());
        for entry in &state.pending {
            entry.status.set(AcquireStatus::TimedOut);
        }
        if state.issued > state.signaled {
            self.shared.signal(state.issued);
        }
        // The semaphore may only go once no submission waits on it.
        let _ = self.device.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });
        // SAFETY: the thread is joined and the queue is idle, so nothing uses the semaphore.
        unsafe {
            self.shared
                .raw
                .destroy_semaphore(self.shared.semaphore, None)
        };
    }
}

impl Shared {
    fn wake(&self) {
        let one = 1u64;
        // SAFETY: an 8-byte write to our eventfd; a full counter (EAGAIN) still wakes the reader.
        unsafe { libc::write(self.wake.as_raw_fd(), (&raw const one).cast(), 8) };
    }

    fn signal(&self, value: u64) {
        let info = vk::SemaphoreSignalInfo::default()
            .semaphore(self.semaphore)
            .value(value);
        // SAFETY: `value` is above every value signaled before (only the watcher, and its owner
        // after joining it, signal); the device is alive while the `Watcher` exists.
        if let Err(err) = unsafe { self.raw.signal_semaphore(&info) } {
            tracing::error!(
                target: "daedalus_gpu::dmabuf",
                error = %err,
                value,
                "vkSignalSemaphore failed; GPU work waiting on dmabuf acquire fences may stall"
            );
        }
    }

    /// The watcher thread: poll the wake fd and the unresolved fences, resolve what signaled or
    /// timed out, and advance the timeline past the resolved prefix.
    fn run(&self) {
        let mut fds: Vec<libc::pollfd> = Vec::new();
        let mut resolved: Vec<(StatusCell, AcquireStatus)> = Vec::new();
        let pollfd = |fd| libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        loop {
            let timeout = {
                let state = self.state.lock();
                if state.shutdown {
                    return;
                }
                fds.clear();
                fds.push(pollfd(self.wake.as_raw_fd()));
                let mut next: Option<Instant> = None;
                for entry in &state.pending {
                    // Resolved entries stay until the prefix before them resolves; a negative fd
                    // is ignored by `poll`.
                    let watched = entry.outcome.is_none();
                    if watched && let Some(deadline) = entry.deadline {
                        next = Some(next.map_or(deadline, |next| next.min(deadline)));
                    }
                    fds.push(pollfd(if watched { entry.fence.as_raw_fd() } else { -1 }));
                }
                next.map_or(-1, |deadline| {
                    let left = deadline.saturating_duration_since(Instant::now());
                    left.as_nanos().div_ceil(1_000_000).min(i32::MAX as u128) as i32
                })
            };
            // SAFETY: `fds` holds valid pollfds (owned by entries that only this thread removes).
            unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout) };
            if fds[0].revents != 0 {
                let mut count = 0u64;
                // SAFETY: an 8-byte read from our non-blocking eventfd resets it.
                unsafe { libc::read(self.wake.as_raw_fd(), (&raw mut count).cast(), 8) };
            }

            let release = {
                let mut state = self.state.lock();
                let now = Instant::now();
                // Entries appended since the snapshot have no pollfd yet (`zip` stops early).
                for (entry, fd) in state.pending.iter_mut().zip(&fds[1..]) {
                    if entry.outcome.is_some() {
                        continue;
                    }
                    entry.outcome = if fd.revents & libc::POLLIN != 0 {
                        Some(AcquireStatus::Ready)
                    } else if fd.revents != 0 || entry.deadline.is_some_and(|d| now >= d) {
                        tracing::warn!(
                            target: "daedalus_gpu::dmabuf",
                            revents = fd.revents,
                            "dmabuf acquire fence did not signal in time; the GPU goes ahead \
                             and the image contents are undefined"
                        );
                        Some(AcquireStatus::TimedOut)
                    } else {
                        None
                    };
                }
                while let Some(outcome) = state.pending.front().and_then(|e| e.outcome) {
                    if let Some(entry) = state.pending.pop_front() {
                        state.signaled = entry.value;
                        resolved.push((entry.status, outcome));
                    }
                }
                (!resolved.is_empty()).then_some(state.signaled)
            };
            // Statuses first, so a handle never reads `Pending` once the GPU went ahead. The thread
            // never owns tokens: dropping the last one submits a release, which could wait for a
            // value only this thread signals.
            for (cell, status) in resolved.drain(..) {
                cell.set(status);
            }
            if let Some(value) = release {
                self.signal(value);
            }
        }
    }
}

/// Whether the device has timeline semaphores enabled: wgpu-hal enables the Vulkan 1.2
/// `timelineSemaphore` feature whenever the device supports it.
fn timeline_supported(hal_dev: &hal_vk::Device) -> bool {
    let shared = hal_dev.shared_instance();
    let instance = shared.raw_instance();
    let phys = hal_dev.raw_physical_device();
    // SAFETY: `phys` belongs to `instance`; queries only.
    let api = unsafe { instance.get_physical_device_properties(phys) }.api_version;
    if shared.instance_api_version() < vk::API_VERSION_1_2 || api < vk::API_VERSION_1_2 {
        return false;
    }
    let mut v12 = vk::PhysicalDeviceVulkan12Features::default();
    let mut features = vk::PhysicalDeviceFeatures2::default().push_next(&mut v12);
    // SAFETY: as above, with a valid out-structure chain.
    unsafe { instance.get_physical_device_features2(phys, &mut features) };
    v12.timeline_semaphore == vk::TRUE
}
