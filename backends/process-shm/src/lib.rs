//! Shared memory between a host and its process workers (Linux).
//!
//! A worker's stdin and stdout are one unix socket. The host lends a worker two buffers in one
//! sealed memfd: one it writes packets into for the worker, one the worker writes its answers
//! into. A packet of at least [`THRESHOLD`] bytes is written into its buffer and only a short
//! note goes on the socket; anything smaller goes on the socket as before. The buffers are handed
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

/// The size of each buffer a worker is first lent, in bytes. Untouched pages of a memfd cost no
/// memory, so this is address space until a packet that large is sent.
pub const START_SIZE: usize = 1 << 20;

#[cfg(target_os = "linux")]
mod linux;
mod worker;

#[cfg(target_os = "linux")]
pub use linux::{Borrower, Lender, ShmError, is_socket, recv_packet};
pub use worker::{Inbox, Incoming, Lent, PLAIN_LINE};
