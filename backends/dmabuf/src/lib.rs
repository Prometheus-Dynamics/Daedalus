//! Shareable blocks of memory as a memory space. See the README.

#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

mod backend;
mod dma_heap;
mod memfd;
mod pool;

pub use backend::{COPY, DmabufBackend, HOST_SPACE, MAP, SPACE};
pub use daedalus_dma_sys::{Access, Error, Identity};
pub use pool::{Block, Guard, KEPT, Pool, PoolStats, Source};
