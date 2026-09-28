//! Type-erased payload storage backends.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::{ReleaseMode, TypeKey};

/// Type-erased payload storage.
pub trait PayloadStorage: Send + Sync + fmt::Debug {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync>;
    fn type_key(&self) -> &TypeKey;
    fn value_any(&self) -> Option<&dyn Any> {
        None
    }
    /// Borrow the stored value as a thread-safe `Any`, when the storage exposes one.
    ///
    /// Unlike [`PayloadStorage::value_any`], this keeps the `Send + Sync` bounds so the value can
    /// be handed to serializer maps keyed by `TypeId` that expect `&(dyn Any + Send + Sync)`.
    fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
        None
    }
    fn rust_type_name(&self) -> Option<&'static str> {
        None
    }
    fn bytes_estimate(&self) -> Option<u64> {
        None
    }
    fn release_mode(&self) -> ReleaseMode {
        ReleaseMode::ImmediateNonBlocking
    }
}

pub(super) struct TypedStorage<T: Send + Sync + 'static> {
    pub(super) type_key: TypeKey,
    pub(super) value: Arc<T>,
    pub(super) bytes_estimate: Option<u64>,
}

impl<T: Send + Sync + 'static> fmt::Debug for TypedStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypedStorage")
            .field("type_key", &self.type_key)
            .field("rust_type_name", &std::any::type_name::<T>())
            .field("bytes_estimate", &self.bytes_estimate)
            .finish_non_exhaustive()
    }
}

impl<T: Send + Sync + 'static> PayloadStorage for TypedStorage<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync> {
        self
    }

    fn type_key(&self) -> &TypeKey {
        &self.type_key
    }

    fn value_any(&self) -> Option<&dyn Any> {
        Some(self.value.as_ref())
    }

    fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
        Some(self.value.as_ref())
    }

    fn rust_type_name(&self) -> Option<&'static str> {
        Some(std::any::type_name::<T>())
    }

    fn bytes_estimate(&self) -> Option<u64> {
        self.bytes_estimate
    }
}

#[derive(Debug)]
pub(super) struct BytesStorage {
    pub(super) type_key: TypeKey,
    pub(super) bytes: Arc<[u8]>,
}

impl PayloadStorage for BytesStorage {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync> {
        self
    }

    fn type_key(&self) -> &TypeKey {
        &self.type_key
    }

    fn value_any(&self) -> Option<&dyn Any> {
        Some(&self.bytes)
    }

    fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
        Some(&self.bytes)
    }

    fn rust_type_name(&self) -> Option<&'static str> {
        Some(std::any::type_name::<Arc<[u8]>>())
    }

    fn bytes_estimate(&self) -> Option<u64> {
        Some(self.bytes.len() as u64)
    }
}
