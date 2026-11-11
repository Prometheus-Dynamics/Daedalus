//! Values a plugin produced. A value holds the plugin's bytes without a copy, when the plugin hands
//! them over with a release: the bytes stay where the plugin put them, and the plugin frees them
//! when the last value holding them is dropped. The value holds its generation too, so the library
//! stays open while the bytes are there.
//!
//! Bytes the host itself copies are the other kind: they are held as an `Arc` and freed by the host.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus_abi::{BytesC, BytesReleaseFn};

use crate::generation::Generation;

/// Bytes the host has copied, in total, since the process started. A hand-over copies none.
static COPIED: AtomicUsize = AtomicUsize::new(0);
/// Hand-over bytes the plugin has released, in total.
static RELEASED: AtomicUsize = AtomicUsize::new(0);

/// How many bytes the host has copied from plugins, in total.
pub fn bytes_copied() -> usize {
    COPIED.load(Ordering::Relaxed)
}

/// How many plugin-owned byte buffers have been released, in total.
pub fn bytes_released() -> usize {
    RELEASED.load(Ordering::Relaxed)
}

/// Where a value's bytes are.
enum Storage {
    /// The host's own copy.
    Copied(Arc<[u8]>),
    /// The plugin's bytes, freed by `release` with `data` when this is dropped.
    Handed {
        ptr: *const u8,
        len: usize,
        data: *mut (),
        release: Option<BytesReleaseFn>,
    },
}

// SAFETY: a handed-over buffer is only read, and the plugin's contract says it stays unchanged until
// `release` runs, which happens once, on the last drop. Any thread may read it or release it.
unsafe impl Send for Storage {}
// SAFETY: as above: shared reads of immutable bytes are safe from any thread.
unsafe impl Sync for Storage {}

impl Drop for Storage {
    fn drop(&mut self) {
        if let Storage::Handed {
            len, data, release, ..
        } = self
            && let Some(release) = release.take()
        {
            // SAFETY: the plugin handed `data` out with this release, and this is the one call.
            // The generation is still held: it drops after this, in `SharedBytes`.
            unsafe { release(*data) };
            RELEASED.fetch_add(*len, Ordering::Relaxed);
        }
    }
}

/// Immutable bytes from a plugin. Cloning shares them, and each clone holds the generation.
#[derive(Clone)]
pub struct SharedBytes {
    storage: Arc<Storage>,
    generation: Arc<Generation>,
}

impl SharedBytes {
    /// Copies `bytes` once, for a value of `generation`.
    pub fn from_plugin(generation: &Arc<Generation>, bytes: &[u8]) -> SharedBytes {
        COPIED.fetch_add(bytes.len(), Ordering::Relaxed);
        SharedBytes {
            storage: Arc::new(Storage::Copied(Arc::from(bytes))),
            generation: generation.clone(),
        }
    }

    /// Takes the plugin's bytes as they are, with no copy.
    ///
    /// # Safety
    /// `bytes` must come from the plugin of `generation`, and its bytes must stay valid and unchanged
    /// until its `release` is called. The value calls `release` once, on its last drop.
    pub unsafe fn adopt(generation: &Arc<Generation>, bytes: BytesC) -> SharedBytes {
        SharedBytes {
            storage: Arc::new(Storage::Handed {
                ptr: bytes.ptr,
                len: bytes.len,
                data: bytes.data,
                release: bytes.release,
            }),
            generation: generation.clone(),
        }
    }

    pub fn bytes(&self) -> &[u8] {
        match &*self.storage {
            Storage::Copied(bytes) => bytes,
            Storage::Handed { ptr, len, .. } => {
                if *len == 0 {
                    return &[];
                }
                // SAFETY: `adopt` promised `len` valid bytes at `ptr` until release, and this value
                // holds that release back until it is dropped.
                unsafe { std::slice::from_raw_parts(*ptr, *len) }
            }
        }
    }

    /// The generation these bytes came from.
    pub fn generation(&self) -> &Generation {
        &self.generation
    }

    /// How many handles share these bytes, this one included.
    pub fn handles(&self) -> usize {
        Arc::strong_count(&self.storage)
    }
}

impl std::fmt::Debug for SharedBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedBytes")
            .field("len", &self.bytes().len())
            .field("generation", &self.generation.number())
            .finish()
    }
}
