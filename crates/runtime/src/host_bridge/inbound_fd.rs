//! Pollable inbound readiness (Linux, `std`): an `eventfd` hosts wait on with `poll(2)` or
//! `epoll` next to their own descriptors (a camera, a socket) instead of blocking in
//! [`InboundWaiter`](super::InboundWaiter).
//!
//! The fd is created on first request ([`HostBridgeHandle::inbound_fd`]); until then a push pays
//! one atomic load. Afterwards a push signals it only when it is not signalled already, so a burst
//! of pushes costs one `write(2)` until the consumer clears it.
//!
//! Protocol (no lost wakeups): wait until the fd is readable, [`InboundFd::clear`] it, then take
//! the pending input (`tick_until_idle`, `take_inbound_into`, or one tick followed by
//! [`InboundFd::rearm`], which `HostGraph::tick_ready` does). Input pushed after `clear` signals
//! the fd again; input pushed before it is already visible to the drain that follows.

use core::sync::atomic::{AtomicBool, Ordering};
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};

use crate::portable::Arc;

use super::{HostBridgeHandle, HostBridgeShared, has_pending_inbound_locked};

/// The eventfd behind [`InboundFd`] and whether it is signalled.
pub(crate) struct Readiness {
    fd: OwnedFd,
    signaled: AtomicBool,
}

impl Readiness {
    fn new() -> io::Result<Self> {
        // SAFETY: `eventfd` takes no pointers; a non-negative result is a new descriptor we own.
        let raw = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            // SAFETY: `raw` is a fresh, open descriptor owned by nobody else.
            fd: unsafe { OwnedFd::from_raw_fd(raw) },
            signaled: AtomicBool::new(false),
        })
    }

    /// Make the fd readable unless it already is.
    pub(super) fn signal(&self) {
        if self.signaled.swap(true, Ordering::SeqCst) {
            return;
        }
        let one = 1_u64;
        // SAFETY: writes 8 bytes from a live `u64` to our open eventfd. It cannot block
        // (`EFD_NONBLOCK`) or overflow the counter: at most one write lands per `clear`.
        unsafe { libc::write(self.fd.as_raw_fd(), (&raw const one).cast(), 8) };
    }

    /// Reset the fd. Reading first and dropping the flag second means a push racing this either
    /// signals again or happened before the flag dropped, so the caller's drain sees it.
    fn clear(&self) {
        let mut count = 0_u64;
        // SAFETY: reads 8 bytes into a live `u64` from our open, non-blocking eventfd; `EAGAIN`
        // (not signalled) is the expected outcome when there is nothing to reset.
        unsafe { libc::read(self.fd.as_raw_fd(), (&raw mut count).cast(), 8) };
        self.signaled.store(false, Ordering::SeqCst);
    }
}

impl HostBridgeShared {
    /// Signal the inbound fd, if one was requested. Call after releasing the buffer lock.
    #[inline]
    pub(super) fn signal_inbound_fd(&self) {
        if let Some(readiness) = self.inbound_fd.get() {
            readiness.signal();
        }
    }
}

/// A file descriptor that becomes readable when the host bridge has inbound work: a queued
/// (non-held) payload, a batch, a closed bridge, or [`HostBridgeHandle::wake_inbound_waiters`]
/// (so a stop request wakes a `poll` loop too). Held pushes never signal it, as they never
/// trigger a tick.
///
/// Readiness is edge-like with a level re-check: [`Self::clear`] resets it, and
/// [`Self::rearm`] signals it again while input is still pending or the bridge is closed. Wait
/// with `poll(2)`/`epoll` for `POLLIN`/`EPOLLIN`; never `read` the fd yourself. Clones share one
/// descriptor, which lives as long as the bridge.
///
/// ```no_run
/// # fn f(host: &daedalus_runtime::host_bridge::HostBridgeHandle) -> std::io::Result<()> {
/// use std::os::fd::AsRawFd;
/// let inbound = host.inbound_fd()?;
/// let mut out = Vec::new();
/// let mut fds = [libc::pollfd { fd: inbound.as_raw_fd(), events: libc::POLLIN, revents: 0 }];
/// while !host.is_closed() {
///     // SAFETY: `fds` is a valid array of one pollfd.
///     unsafe { libc::poll(fds.as_mut_ptr(), 1, -1) };
///     inbound.clear(); // before draining: later pushes signal again
///     host.take_inbound_into(&mut out);
///     out.clear();
/// }
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct InboundFd {
    shared: Arc<HostBridgeShared>,
}

impl core::fmt::Debug for InboundFd {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("InboundFd").field(&self.as_raw_fd()).finish()
    }
}

impl InboundFd {
    fn readiness(&self) -> &Readiness {
        self.shared
            .inbound_fd
            .get()
            .expect("an InboundFd exists only once its eventfd is set")
    }

    /// Reset readiness before taking the pending input. Anything pushed from now on makes the fd
    /// readable again.
    pub fn clear(&self) {
        self.readiness().clear();
    }

    /// Make the fd readable now (for example to wake the `poll` loop from another thread).
    pub fn signal(&self) {
        self.readiness().signal();
    }

    /// Signal the fd if inbound input is pending or the bridge is closed, and report whether it
    /// did. Call after taking only part of the input (one tick) so the next `poll` returns at
    /// once.
    pub fn rearm(&self) -> bool {
        let ready = {
            let guard = self.shared.buffers.lock();
            guard.closed || has_pending_inbound_locked(&guard)
        };
        if ready {
            self.signal();
        }
        ready
    }
}

impl AsFd for InboundFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.readiness().fd.as_fd()
    }
}

impl AsRawFd for InboundFd {
    fn as_raw_fd(&self) -> RawFd {
        self.readiness().fd.as_raw_fd()
    }
}

impl HostBridgeHandle {
    /// The bridge's pollable inbound fd, created on first call (later calls share it). Readable
    /// at once when input is already pending. See [`InboundFd`] for the clear/drain protocol.
    pub fn inbound_fd(&self) -> io::Result<InboundFd> {
        let fd = InboundFd {
            shared: self.shared.clone(),
        };
        if self.shared.inbound_fd.get().is_none() {
            // A racing first call may win; its eventfd is kept and this one closed.
            if self.shared.inbound_fd.set(Readiness::new()?).is_ok() {
                fd.rearm();
            }
        }
        Ok(fd)
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::{Duration, Instant};

    use daedalus_transport::{FreshnessPolicy, PressurePolicy};

    use super::*;
    use crate::host_bridge::{HostBridgeHandle, HostBridgeManager};

    /// Wait up to `timeout` for `fd` to become readable.
    fn readable(fd: &InboundFd, timeout: Duration) -> bool {
        let mut pollfd = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let millis = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
        // SAFETY: one valid pollfd.
        let ready = unsafe { libc::poll(&raw mut pollfd, 1, millis) };
        assert!(ready >= 0, "poll: {}", io::Error::last_os_error());
        ready == 1
    }

    fn drain(host: &HostBridgeHandle) -> usize {
        let mut out = Vec::new();
        host.take_inbound_into(&mut out);
        out.len()
    }

    #[test]
    fn signals_on_queued_input_close_and_wake_but_not_held_pushes() {
        let host = HostBridgeManager::new().ensure_handle("host");
        host.push("in", 1_i64);
        let fd = host.inbound_fd().expect("eventfd");
        assert!(readable(&fd, Duration::ZERO), "pending before creation");
        fd.clear();
        assert!(!readable(&fd, Duration::ZERO));
        assert_eq!(drain(&host), 1);

        host.set_held_input("imu");
        host.push("imu", 2_i64);
        assert!(!readable(&fd, Duration::ZERO), "held pushes never signal");

        host.batch()
            .push("a", 1_i64)
            .push("b", 2_i64)
            .commit()
            .expect("batch");
        assert!(readable(&fd, Duration::ZERO));
        fd.clear();
        drain(&host);
        assert!(!fd.rearm(), "only the held value is left");

        host.wake_inbound_waiters();
        assert!(readable(&fd, Duration::ZERO));
        fd.clear();
        host.close();
        assert!(readable(&fd, Duration::ZERO));
        fd.clear();
        assert!(fd.rearm(), "a closed bridge stays ready");
        assert_eq!(
            host.inbound_fd().expect("shared").as_raw_fd(),
            fd.as_raw_fd()
        );
    }

    #[test]
    fn producers_lose_no_wakeups_and_the_consumer_never_spins() {
        const PRODUCERS: usize = 4;
        const PUSHES: usize = 2_000;
        let host = HostBridgeManager::new().ensure_handle("host");
        host.set_default_input_policy(PressurePolicy::BufferAll, FreshnessPolicy::default())
            .expect("buffer every push");
        let fd = host.inbound_fd().expect("eventfd");
        let producers: Vec<_> = (0..PRODUCERS)
            .map(|id| {
                let host = host.clone();
                thread::spawn(move || {
                    let port = crate::handles::PortId::new(format!("p{id}"));
                    for n in 0..PUSHES {
                        host.push(port.clone(), n as i64);
                        if n % 64 == 0 {
                            thread::sleep(Duration::from_micros(50));
                        }
                    }
                })
            })
            .collect();
        let (mut received, mut wakeups, mut empty) = (0, 0, 0);
        let deadline = Instant::now() + Duration::from_secs(30);
        while received < PRODUCERS * PUSHES {
            assert!(
                readable(&fd, Duration::from_secs(5)),
                "lost wakeup: {received} of {} received, {} pending",
                PRODUCERS * PUSHES,
                host.pending_inbound()
            );
            assert!(Instant::now() < deadline);
            wakeups += 1;
            fd.clear();
            let taken = drain(&host);
            empty += usize::from(taken == 0);
            received += taken;
        }
        for producer in producers {
            producer.join().expect("producer");
        }
        // A spurious wakeup needs a push racing `clear`; it never outnumbers real ones.
        assert!(
            empty <= wakeups - empty,
            "{empty} of {wakeups} wakeups were empty"
        );
        fd.clear();
        assert!(
            !readable(&fd, Duration::from_millis(20)),
            "idle bridge must not stay readable"
        );
    }
}
