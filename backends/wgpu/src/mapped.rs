//! A buffer's bytes read where they are.
//!
//! On a board whose GPU shares memory with the CPU, a shader's output that the host reads is
//! written straight into memory the host maps (the planner places it there). Copying it out into
//! a `Vec` would then be the only copy left, and the slowest step: [`MappedBytes`] lends the
//! host the buffer's bytes in place instead. It holds the buffer, so the backend doesn't hand it
//! to the device again until the bytes are let go, and the buffer goes back to the backend when
//! they are.

use std::fmt;
use std::sync::Arc;

use crate::buffer::{Storage, StorageError};

/// A lend of a buffer's mapping, from the crate that maps it.
pub(crate) enum Lend {
    #[cfg(target_os = "linux")]
    Device(daedalus_vk_sys::memory::Lent),
    /// A dma-buf's. `synced` when the lend opened a read sync that its end must close.
    #[cfg(target_os = "linux")]
    Heap {
        lent: daedalus_dma_sys::Lent,
        synced: bool,
    },
}

/// The bytes of a buffer the host maps, borrowed in place. Dereferences to the buffer's bytes:
/// [`Storage::size`] of them, so a whole number of 4-byte words.
pub struct MappedBytes {
    lend: Lend,
    storage: Arc<Storage>,
}

impl MappedBytes {
    /// The bytes of `storage` in place, if the host maps it: a buffer in host-mapped or
    /// dma-heap memory. Any other is handed back, to be read with [`Storage::read_into`].
    ///
    /// The device work that wrote the buffer must have finished, as for a read. A dma-buf is
    /// synced for the host's reads here when the device may have written it, and the sync is
    /// closed when this drops.
    pub fn new(storage: Arc<Storage>) -> Result<MappedBytes, (Arc<Storage>, StorageError)> {
        #[cfg(target_os = "linux")]
        {
            match storage.lend() {
                Ok(lend) => Ok(MappedBytes { lend, storage }),
                Err(error) => Err((storage, error)),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err((
                storage,
                StorageError::Unsupported("buffers are read in place on Linux only"),
            ))
        }
    }

    /// The buffer the bytes are in.
    pub fn storage(&self) -> &Arc<Storage> {
        &self.storage
    }
}

impl std::ops::Deref for MappedBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        #[cfg(target_os = "linux")]
        {
            let size = self.storage.size() as usize;
            let all: &[u8] = match &self.lend {
                Lend::Device(lent) => lent,
                Lend::Heap { lent, .. } => lent,
            };
            // A mapping is never smaller than its buffer's size; a dma-buf may be larger.
            all.get(..size).unwrap_or(all)
        }
        #[cfg(not(target_os = "linux"))]
        match self.lend {}
    }
}

impl AsRef<[u8]> for MappedBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl Drop for MappedBytes {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        if let Lend::Heap { synced: true, .. } = &self.lend {
            self.storage.end_lend(true);
        }
    }
}

impl fmt::Debug for MappedBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MappedBytes")
            .field("len", &self.storage.size())
            .field("space", &self.storage.space())
            .finish()
    }
}
