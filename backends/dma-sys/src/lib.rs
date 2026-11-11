//! The dma-buf, dma-heap and sync_file kernel calls. See the README.

#![cfg(target_os = "linux")]

mod buf;
mod heap;
mod ident;
mod sync;
mod uapi;

use std::fmt;

pub use buf::{Access, DmaBuf, DmaBufRef, Lent, Mapping};
pub use heap::DmaHeap;
pub use ident::Identity;
pub use sync::{SyncFence, is_sync_file};

/// Why a call failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// The kernel refused, with this `errno`.
    Os(i32),
    /// The fd isn't a sync_file, so it can't be used as a fence.
    NotSyncFile,
    /// An offset or length outside the mapping, or a size that can't be used.
    Range,
    /// The mapping's bytes are lent out, so this process can't write them.
    Lent,
}

impl Error {
    pub(crate) fn last() -> Error {
        Error::Os(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
    }

    /// Whether the kernel doesn't have the call at all, as an older one doesn't have fence export.
    pub fn is_unsupported(&self) -> bool {
        matches!(self, Error::Os(libc::ENOTTY | libc::ENOSYS | libc::EINVAL))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Os(errno) => write!(f, "{}", std::io::Error::from_raw_os_error(*errno)),
            Error::NotSyncFile => f.write_str("the fd isn't a sync_file"),
            Error::Range => f.write_str("outside the mapping"),
            Error::Lent => f.write_str("the mapping's bytes are lent out"),
        }
    }
}

impl std::error::Error for Error {}

/// Turns a `-1` from libc into the `errno`.
pub(crate) fn check(result: libc::c_int) -> Result<libc::c_int, Error> {
    if result < 0 {
        Err(Error::last())
    } else {
        Ok(result)
    }
}
