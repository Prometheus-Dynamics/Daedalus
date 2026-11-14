//! Shared memory between a host and its process workers (Linux).
//!
//! A worker's stdin and stdout are one unix socket. The host lends a worker two buffers in one
//! sealed memfd: one it writes packets into for the worker, one the worker writes its answers
//! into. A packet of at least the grant's threshold ([`THRESHOLD`] by default) is written into its
//! buffer and only a short note goes on the socket; anything smaller goes on the socket as before. The buffers are handed
//! over once, as descriptors on the socket, and named by number afterwards, so a steady run sends
//! no descriptors and allocates nothing.
//!
//! The host owns the memory. A worker only maps it, so a worker that exits, crashes or is killed
//! takes nothing with it: once the host has reaped it, the buffers are the host's alone again,
//! and a worker started in its place is lent them anew.
//!
//! Off Linux nothing here is built, and workers use the plain path.

#![forbid(unsafe_code)]

/// The smallest packet that goes in a buffer, in bytes: the default capacity of a pipe, below
/// which one write on the socket is one wake-up of the reader.
pub const THRESHOLD: usize = 64 * 1024;

/// The smallest packet or value that goes in a buffer for a worker that agreed values by reference
/// of any type, in bytes. On a CM5 a list placed and read where it is beats the socket from about
/// 1 KiB; at this size it takes half the time (testing/board-runs/worker-any-type.md).
pub const ANY_TYPE_THRESHOLD: usize = 4 * 1024;

/// The size of each buffer a worker is first lent, in bytes. Untouched pages of a memfd cost no
/// memory, so this is address space until a packet that large is sent.
pub const START_SIZE: usize = 1 << 20;

#[cfg(target_os = "linux")]
mod fds;
#[cfg(target_os = "linux")]
mod held;
#[cfg(target_os = "linux")]
mod linux;
mod worker;

#[cfg(target_os = "linux")]
pub use fds::SLOTS;
#[cfg(target_os = "linux")]
pub use linux::{Borrower, Lender, ShmError, is_socket, recv_packet};
pub use worker::{Inbox, Incoming, Lent, PLAIN_LINE, Reads, View, Writes};

/// What a worker did to the buffers it was sent by descriptor, for a test to read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Brackets {
    /// Descriptors received.
    pub attached: u64,
    /// Reads begun, and ended, with the kernel's sync call.
    pub begun: u64,
    pub ended: u64,
    /// Reads of a buffer that takes no sync call: a memfd, with no device behind it.
    pub unbracketed: u64,
}
