//! A small eventfd and a `poll` over descriptors (Linux, std). The host crate has no `unsafe`, so
//! the syscalls live here and are safe to call.

#![cfg(feature = "std")]

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::vec::Vec;

/// An eventfd: readable while it holds a count.
#[derive(Debug)]
pub struct EventFd(OwnedFd);

impl EventFd {
    /// Creates a nonblocking, close-on-exec eventfd.
    pub fn new() -> io::Result<EventFd> {
        // SAFETY: `eventfd` takes no pointers. A non-negative result is a new descriptor we own.
        let raw = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `raw` is a fresh, open descriptor owned by nobody else.
        Ok(EventFd(unsafe { OwnedFd::from_raw_fd(raw) }))
    }

    /// Makes the descriptor readable.
    pub fn signal(&self) {
        let one = 1u64.to_ne_bytes();
        // SAFETY: the buffer is valid for 8 bytes and the descriptor is open. A full counter
        // can only mean it is already readable, which is the state asked for.
        let _ = unsafe { libc::write(self.0.as_raw_fd(), one.as_ptr().cast(), 8) };
    }

    /// Drains the count, so the descriptor stops being readable.
    pub fn clear(&self) {
        let mut buf = [0u8; 8];
        // SAFETY: the buffer is valid for 8 bytes and the descriptor is open. A would-block on
        // an empty counter is the state asked for.
        let _ = unsafe { libc::read(self.0.as_raw_fd(), buf.as_mut_ptr().cast(), 8) };
    }

    pub fn as_fd(&self) -> BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.0)
    }
}

/// Waits until at least one of `fds` is readable, up to `timeout_ms` (-1 waits forever), and
/// returns one flag per descriptor.
pub fn poll_readable(fds: &[BorrowedFd<'_>], timeout_ms: i32) -> io::Result<Vec<bool>> {
    let mut polls: Vec<libc::pollfd> = fds
        .iter()
        .map(|fd| libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        })
        .collect();
    // SAFETY: `polls` is a valid array of `polls.len()` entries for the duration of the call.
    let ready = unsafe { libc::poll(polls.as_mut_ptr(), polls.len() as libc::nfds_t, timeout_ms) };
    if ready < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(polls
        .iter()
        .map(|entry| entry.revents & libc::POLLIN != 0)
        .collect())
}
