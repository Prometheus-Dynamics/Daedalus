//! Fences: sync_file fds that signal when work on a buffer is done.

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::time::Duration;

use crate::uapi::{SYNC_IOC_FILE_INFO, SyncFileInfo};
use crate::{Error, check};

/// Whether the kernel says `fd` is a sync_file. Anything else must never reach a GPU driver as a
/// fence: a pipe, for one, never signals.
pub fn is_sync_file(fd: BorrowedFd<'_>) -> bool {
    let mut info = SyncFileInfo::default();
    // SAFETY: the argument is the struct this ioctl fills in, alive for the call. With
    // `num_fences` zero the kernel writes nothing beyond it.
    let result = unsafe { libc::ioctl(fd.as_raw_fd(), SYNC_IOC_FILE_INFO as _, &raw mut info) };
    result == 0
}

/// A fence. Only an fd the kernel confirms is a sync_file becomes one.
#[derive(Debug)]
pub struct SyncFence {
    fd: OwnedFd,
}

impl SyncFence {
    /// Wraps `fd`, or gives it back closed with [`Error::NotSyncFile`].
    pub fn from_fd(fd: OwnedFd) -> Result<SyncFence, Error> {
        if !is_sync_file(fd.as_fd()) {
            return Err(Error::NotSyncFile);
        }
        Ok(SyncFence { fd })
    }

    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    pub fn into_fd(self) -> OwnedFd {
        self.fd
    }

    /// Waits up to `timeout` for the fence. `Ok(true)` when it signaled, `Ok(false)` on timeout.
    pub fn wait(&self, timeout: Duration) -> Result<bool, Error> {
        let mut poll = libc::pollfd {
            fd: self.fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let millis = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
        loop {
            // SAFETY: one `pollfd`, alive for the call.
            match check(unsafe { libc::poll(&raw mut poll, 1, millis) }) {
                Ok(ready) => return Ok(ready > 0 && poll.revents & libc::POLLIN != 0),
                Err(Error::Os(libc::EINTR)) => {}
                Err(error) => return Err(error),
            }
        }
    }

    /// Whether the fence has signaled. Never blocks.
    pub fn is_signaled(&self) -> Result<bool, Error> {
        self.wait(Duration::ZERO)
    }
}
