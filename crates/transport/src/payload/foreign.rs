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

#[cfg(target_has_atomic = "ptr")]
use super::ArcValue;
use super::{Payload, PayloadStorage, ResidencyCache, TypedStorage};
use crate::{
    ForeignBorrow, ForeignHandle, ForeignInterface, PayloadLineage, ProvideForeign, Residency,
    TypeKey,
};

#[derive(Debug)]
struct ForeignStorage {
    handle: ForeignHandle,
}

impl PayloadStorage for ForeignStorage {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
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

/// Storage holding an owner value `O` in an `Arc`: typed storage, or the value's own
/// allocation ([`ArcValue`]).
trait OwnerStorage<O>: PayloadStorage {
    fn owner(&self) -> &O;
    fn owner_arc(&self) -> Arc<O>;
}

impl<O: Send + Sync + 'static> OwnerStorage<O> for TypedStorage<O> {
    fn owner(&self) -> &O {
        &self.value
    }

    fn owner_arc(&self) -> Arc<O> {
        self.value.clone()
    }
}

#[cfg(target_has_atomic = "ptr")]
impl<O: Send + Sync + 'static> OwnerStorage<O> for ArcValue<O> {
    fn owner(&self) -> &O {
        &self.0
    }

    fn owner_arc(&self) -> Arc<O> {
        let value = (self as *const Self).cast::<O>();
        // Safety: an `ArcValue<O>` only lives in an `Arc` allocation laid out as `ArcInner<O>`
        // (`arc_storage`), alive while `self` is borrowed; the new reference is the returned one.
        unsafe {
            Arc::increment_strong_count(value);
            Arc::from_raw(value)
        }
    }
}

/// Owner storage `S` that also lends its `O` through `O`'s provider for `I`. Same layout
/// (`repr(transparent)`), so [`Payload::provide_foreign`] retypes the storage in place: only the
/// payload's storage vtable changes. Downcasts still see the `S`.
#[repr(transparent)]
struct ProvidedStorage<S, O, I> {
    inner: S,
    interface: PhantomData<fn() -> (O, I)>,
}

impl<S: fmt::Debug, O, I> fmt::Debug for ProvidedStorage<S, O, I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<S, O, I> PayloadStorage for ProvidedStorage<S, O, I>
where
    S: OwnerStorage<O>,
    O: ProvideForeign<I>,
    I: ForeignInterface,
{
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self.inner.as_any_mut()
    }

    fn value_any(&self) -> Option<&dyn Any> {
        self.inner.value_any()
    }

    fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
        self.inner.value_any_sync()
    }

    fn rust_type_name(&self) -> Option<&'static str> {
        self.inner.rust_type_name()
    }

    fn rust_type_id(&self) -> Option<TypeId> {
        self.inner.rust_type_id()
    }

    fn bytes_estimate(&self) -> Option<u64> {
        self.inner.bytes_estimate()
    }

    fn foreign_borrow(&self) -> Option<ForeignBorrow<'_>> {
        Some(ForeignBorrow::of::<O, I>(self.inner.owner()))
    }

    fn to_foreign_handle(&self) -> Option<ForeignHandle> {
        Some(ForeignHandle::from_arc::<O, I>(self.inner.owner_arc()))
    }
}

impl Payload {
    /// A payload carrying `handle` under `type_key` (normally the interface key).
    pub fn foreign(
        type_key: impl Into<TypeKey>,
        handle: ForeignHandle,
        residency: Residency,
    ) -> Self {
        Self {
            type_key: type_key.into(),
            storage: arc_dyn!(ForeignStorage { handle }),
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
    pub fn provide_foreign<O, I>(self) -> Result<Self, Box<Self>>
    where
        I: ForeignInterface,
        O: ProvideForeign<I>,
    {
        let any = self.storage.as_any();
        let at = |value: &dyn Any| core::ptr::addr_eq(value, Arc::as_ptr(&self.storage));
        if any.downcast_ref::<TypedStorage<O>>().is_some_and(|v| at(v)) {
            return Ok(self.provided::<TypedStorage<O>, O, I>());
        }
        #[cfg(target_has_atomic = "ptr")]
        if any.downcast_ref::<ArcValue<O>>().is_some_and(|v| at(v)) {
            return Ok(self.provided::<ArcValue<O>, O, I>());
        }
        let Some(handle) = ForeignHandle::from_payload::<O, I>(&self) else {
            return Err(Box::new(self));
        };
        Ok(Payload::foreign(I::KEY, handle, self.residency).with_lineage(self.lineage))
    }

    /// Retype storage that is an `S` at the allocation's address as `ProvidedStorage<S, O, I>`.
    fn provided<S, O, I>(mut self) -> Self
    where
        S: OwnerStorage<O> + 'static,
        O: ProvideForeign<I>,
        I: ForeignInterface,
    {
        let raw: *const ProvidedStorage<S, O, I> = Arc::into_raw(self.storage).cast();
        // Safety: the allocation holds an `S` at this address (checked by the caller), and
        // `ProvidedStorage<S, O, I>` is a `repr(transparent)` wrapper of it: same size, alignment
        // and drop, so the `Arc` takes back its reference under the new vtable.
        self.storage = unsafe { Arc::from_raw(raw as *const dyn PayloadStorage) };
        self.type_key = TypeKey::from_static(I::KEY);
        self
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
