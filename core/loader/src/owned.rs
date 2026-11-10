//! Values a plugin produced. The host holds an immutable copy of the bytes and one count on the
//! generation they came from, so the library stays open while any such value lives. Passing a
//! value around clones a handle: the bytes are not copied again.

use std::sync::Arc;

use crate::generation::Generation;

/// Immutable bytes from a plugin. Cloning shares them, and each clone holds the generation.
#[derive(Clone)]
pub struct SharedBytes {
    bytes: Arc<[u8]>,
    generation: Arc<Generation>,
}

impl SharedBytes {
    /// Copies `bytes` once, for a value of `generation`.
    pub fn from_plugin(generation: &Arc<Generation>, bytes: &[u8]) -> SharedBytes {
        SharedBytes {
            bytes: Arc::from(bytes),
            generation: generation.clone(),
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The generation these bytes came from.
    pub fn generation(&self) -> &Generation {
        &self.generation
    }

    /// How many handles share these bytes, this one included.
    pub fn handles(&self) -> usize {
        Arc::strong_count(&self.bytes)
    }
}

impl std::fmt::Debug for SharedBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedBytes")
            .field("len", &self.bytes.len())
            .field("generation", &self.generation.number())
            .finish()
    }
}
