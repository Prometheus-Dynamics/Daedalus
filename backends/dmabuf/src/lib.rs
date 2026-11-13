//! The dma-buf memory space: a place a value can be.
//!
//! A value here is bytes in a dma-buf. This crate knows where they are and how many there are,
//! and nothing else: not what they mean, how they are laid out or who made them. Whoever makes a
//! value answers for its contents being readable by whatever is given it.
//!
//! The space is only a location. A type's owner says its values are dma-bufs by implementing
//! [`DmaBufBacked`], and registers the type with [`DmaBufMemory::with`], which lists the moves
//! between this space and the host for that type. Neither move copies or touches the bytes. A
//! backend that can take a dma-buf in place lists its own move out of this space: the wgpu
//! backend's `with_dmabuf::<T>()` imports one into a device buffer.

#![forbid(unsafe_code)]

/// The id of the dma-buf memory space.
pub const SPACE: &str = "dmabuf";

/// The id of the host's memory space, where a CPU node's values are.
pub const HOST_SPACE: &str = "cpu.host";

#[cfg(target_os = "linux")]
mod space;

#[cfg(target_os = "linux")]
pub use space::{DmaBufBacked, DmaBufMemory, from_host_id, to_host_id};
