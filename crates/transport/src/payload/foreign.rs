//! Foreign-readable payloads: ones carrying a [`ForeignHandle`], and owner payloads retyped by
//! their provider ([`Payload::provide_foreign`]).
//!
//! Consumers read them through [`PayloadStorage::foreign_borrow`], a trait method, rather than a
//! `TypeId` downcast, so a separately built plugin finds the handle even when its copy of this
//! crate has other `TypeId`s.

use crate::portable::{Arc, arc_dyn};
use alloc::boxed::Box;
use core::any::{Any, TypeId};
use core::fmt;
use core::marker::PhantomData;

use super::{Payload, PayloadStorage, ResidencyCache, TypedStorage};
use crate::{
    ForeignBorrow, ForeignHandle, ForeignInterface, PayloadLineage, ProvideForeign, Residency,
    TypeKey,
};

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
        Some(core::any::type_name::<ForeignHandle>())
    }

    fn foreign_handle(&self) -> Option<&ForeignHandle> {
        Some(&self.handle)
    }
}

/// `TypedStorage<O>` that also lends its value through `O`'s provider for `I`. Same layout
/// (`repr(transparent)`), so [`Payload::provide_foreign`] turns typed storage into it in place:
/// only the payload's storage vtable changes. Downcasts still see the `TypedStorage<O>`.
#[repr(transparent)]
struct ProvidedStorage<O: Send + Sync + 'static, I> {
    typed: TypedStorage<O>,
    interface: PhantomData<fn() -> I>,
}

impl<O: Send + Sync + 'static, I> fmt::Debug for ProvidedStorage<O, I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.typed.fmt(f)
    }
}

impl<O: ProvideForeign<I>, I: ForeignInterface> PayloadStorage for ProvidedStorage<O, I> {
    fn as_any(&self) -> &dyn Any {
        &self.typed
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        &mut self.typed
    }

    fn type_key(&self) -> &TypeKey {
        self.typed.type_key()
    }

    fn value_any(&self) -> Option<&dyn Any> {
        self.typed.value_any()
    }

    fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
        self.typed.value_any_sync()
    }

    fn rust_type_name(&self) -> Option<&'static str> {
        self.typed.rust_type_name()
    }

    fn rust_type_id(&self) -> Option<TypeId> {
        self.typed.rust_type_id()
    }

    fn bytes_estimate(&self) -> Option<u64> {
        self.typed.bytes_estimate()
    }

    fn foreign_borrow(&self) -> Option<ForeignBorrow<'_>> {
        Some(ForeignBorrow::of::<O, I>(&self.typed.value))
    }

    fn to_foreign_handle(&self) -> Option<ForeignHandle> {
        Some(ForeignHandle::from_arc::<O, I>(self.typed.value.clone()))
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
            storage: arc_dyn!(ForeignStorage { type_key, handle }),
            residency,
            layout: None,
            residency_cache: ResidencyCache::default(),
            lineage: PayloadLineage::new(),
        }
    }

    /// The foreign handle this payload carries, if any (see [`Self::foreign_borrow`] for any
    /// foreign-readable payload).
    pub fn foreign_handle(&self) -> Option<&ForeignHandle> {
        self.storage.foreign_handle()
    }

    /// Retype a payload holding the owner type `O` as interface `I` (key `I::KEY`), readable
    /// through `O`'s provider; `Err` (the boxed payload) when it holds no `O`.
    ///
    /// A shared `O` (typed storage, as `Payload::shared_with` builds) is lent in place:
    /// [`Self::foreign_borrow`] borrows the owner value itself, and nothing is allocated, copied
    /// or reference counted. The payload keeps its storage, residency and lineage, so
    /// `get_ref::<O>()` still works downstream. Other storage (boundary storage) is wrapped in a
    /// [`ForeignHandle`] payload instead (one allocation).
    pub fn provide_foreign<O, I>(mut self) -> Result<Self, Box<Self>>
    where
        I: ForeignInterface,
        O: ProvideForeign<I>,
    {
        let typed = self.storage.as_any().downcast_ref::<TypedStorage<O>>();
        if !typed.is_some_and(|typed| core::ptr::addr_eq(typed, Arc::as_ptr(&self.storage))) {
            let Some(handle) = ForeignHandle::from_payload::<O, I>(&self) else {
                return Err(Box::new(self));
            };
            return Ok(Payload::foreign(I::KEY, handle, self.residency).with_lineage(self.lineage));
        }
        let raw: *const ProvidedStorage<O, I> = Arc::into_raw(self.storage).cast();
        // Safety: the allocation holds a `TypedStorage<O>` at this address (checked above), and
        // `ProvidedStorage<O, I>` is a `repr(transparent)` wrapper of it: same size, alignment
        // and drop, so the `Arc` takes back its reference under the new vtable.
        self.storage = unsafe { Arc::from_raw(raw as *const dyn PayloadStorage) };
        self.type_key = TypeKey::from_static(I::KEY);
        Ok(self)
    }

    /// Lend the payload's value through a foreign interface: the [`ForeignHandle`] it carries,
    /// or the owner value of a [`Self::provide_foreign`] payload. `None` for other payloads.
    pub fn foreign_borrow(&self) -> Option<ForeignBorrow<'_>> {
        self.storage.foreign_borrow()
    }

    /// An owning [`ForeignHandle`] of [`Self::foreign_borrow`]'s value (a retain of the carried
    /// handle, or the owner's `Arc`), for consumers the borrow cannot cover, such as the stable
    /// plugin boundary.
    pub fn to_foreign_handle(&self) -> Option<ForeignHandle> {
        self.storage.to_foreign_handle()
    }
}
