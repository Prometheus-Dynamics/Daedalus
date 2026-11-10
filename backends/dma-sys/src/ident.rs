//! Which buffer an fd is.

use std::os::fd::{AsRawFd, BorrowedFd};

use crate::{Error, check};

/// The device and inode behind an fd. Two fds of one dma-buf have the same identity, so a cache
/// keyed by it finds a buffer again however it arrives.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Identity {
    pub dev: u64,
    pub ino: u64,
}

impl Identity {
    pub fn of(fd: BorrowedFd<'_>) -> Result<Identity, Error> {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: `fstat` writes one `stat` into the space given, and the fd is open for the
        // length of the borrow.
        check(unsafe { libc::fstat(fd.as_raw_fd(), stat.as_mut_ptr()) })?;
        // SAFETY: `fstat` succeeded, so the struct is filled in.
        let stat = unsafe { stat.assume_init() };
        // The field widths differ between architectures.
        #[allow(clippy::unnecessary_cast)]
        Ok(Identity {
            dev: stat.st_dev as u64,
            ino: stat.st_ino as u64,
        })
    }
}
