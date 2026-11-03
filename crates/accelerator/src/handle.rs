use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::AcceleratorError;

/// One accelerator instance; handles carry it so another instance can refuse them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AcceleratorId(u64);

impl AcceleratorId {
    /// A process-unique id, for an accelerator implementation to take once when it is created.
    pub fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// Called once with the object's id when the last clone of its handle drops: the accelerator
/// frees the object (after work that uses it completes).
pub type Release = Box<dyn FnOnce(u64) + Send + Sync>;

/// Marker of [`ProgramHandle`].
#[derive(Debug)]
pub enum Program {}
/// Marker of [`BufferHandle`].
#[derive(Debug)]
pub enum Buffer {}

/// A loaded program.
pub type ProgramHandle = Handle<Program>;
/// Device memory: allocated, uploaded or an imported dmabuf.
pub type BufferHandle = Handle<Buffer>;

/// An accelerator object: its owner, the owner's id for it, and its length in bytes (a buffer's
/// size; a program's image size). Clones share it; the owner's [`Release`] runs when the last
/// clone drops.
pub struct Handle<K> {
    inner: Arc<Inner>,
    kind: PhantomData<fn() -> K>,
}

struct Inner {
    owner: AcceleratorId,
    id: u64,
    len: usize,
    release: Option<Release>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release(self.id);
        }
    }
}

impl<K> Handle<K> {
    /// For accelerator implementations: a handle to object `id`, released through `release`.
    pub fn new(owner: AcceleratorId, id: u64, len: usize, release: Option<Release>) -> Self {
        Self {
            inner: Arc::new(Inner {
                owner,
                id,
                len,
                release,
            }),
            kind: PhantomData,
        }
    }

    pub fn owner(&self) -> AcceleratorId {
        self.inner.owner
    }

    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn len(&self) -> usize {
        self.inner.len
    }

    pub fn is_empty(&self) -> bool {
        self.inner.len == 0
    }

    /// `Ok` if `owner` made this handle, else [`AcceleratorError::ForeignHandle`].
    pub fn check_owner(&self, owner: AcceleratorId) -> Result<u64, AcceleratorError> {
        if self.inner.owner == owner {
            Ok(self.inner.id)
        } else {
            Err(AcceleratorError::ForeignHandle)
        }
    }
}

impl<K> Clone for Handle<K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            kind: PhantomData,
        }
    }
}

impl<K> fmt::Debug for Handle<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle")
            .field("owner", &self.inner.owner)
            .field("id", &self.inner.id)
            .field("len", &self.inner.len)
            .finish()
    }
}
