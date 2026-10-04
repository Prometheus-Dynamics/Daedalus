//! Payloads carrying a [`ForeignHandle`].
//!
//! Consumers read the handle through [`PayloadStorage::foreign_handle`], a trait method, rather
//! than a `TypeId` downcast, so a separately built plugin finds it even when its copy of this
//! crate has other `TypeId`s.

use std::any::Any;
use std::sync::Arc;

use super::{Payload, PayloadStorage, ResidencyCache};
use crate::{ForeignHandle, PayloadLineage, Residency, TypeKey};

#[derive(Debug)]
struct ForeignStorage {
    type_key: TypeKey,
    handle: ForeignHandle,
}

impl PayloadStorage for ForeignStorage {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn type_key(&self) -> &TypeKey {
        &self.type_key
    }

    fn value_any(&self) -> Option<&dyn Any> {
        Some(&self.handle)
    }

    fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
        Some(&self.handle)
    }

    fn rust_type_name(&self) -> Option<&'static str> {
        Some(std::any::type_name::<ForeignHandle>())
    }

    fn foreign_handle(&self) -> Option<&ForeignHandle> {
        Some(&self.handle)
    }
}

impl Payload {
    /// A payload carrying `handle` under `type_key` (normally the interface key).
    pub fn foreign(
        type_key: impl Into<TypeKey>,
        handle: ForeignHandle,
        residency: Residency,
    ) -> Self {
        let type_key = type_key.into();
        Self {
            type_key: type_key.clone(),
            storage: Arc::new(ForeignStorage { type_key, handle }),
            residency,
            layout: None,
            residency_cache: ResidencyCache::default(),
            lineage: PayloadLineage::new(),
        }
    }

    /// The foreign handle this payload carries, if any.
    pub fn foreign_handle(&self) -> Option<&ForeignHandle> {
        self.storage.foreign_handle()
    }
}
