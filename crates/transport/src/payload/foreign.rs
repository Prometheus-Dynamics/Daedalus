//! Foreign-readable payloads: ones carrying a [`ForeignHandle`], and owner payloads retyped by
//! their provider ([`Payload::provide_foreign`]).
//!
//! Consumers read them through [`PayloadStorage::foreign_borrow`], a trait method, rather than a
//! `TypeId` downcast, so a separately built plugin finds the handle even when its copy of this
//! crate has other `TypeId`s.

use crate::portable::Arc;
use alloc::boxed::Box;
use core::any::{Any, TypeId};
use core::fmt;
use core::marker::PhantomData;

#[cfg(target_has_atomic = "ptr")]
use super::ArcValue;
use super::inline::Body;
#[cfg(target_has_atomic = "ptr")]
use super::storage::arc_value;
use super::{Payload, PayloadStorage, ResidencyCache, TypedStorage};
use crate::{
    ForeignBorrow, ForeignHandle, ForeignInterface, PayloadLineage, ProvideForeign, Residency,
    TypeKey,
};

/// A foreign handle as a payload holds it, in place ([`Body::Foreign`] and
/// [`Body::LocalForeign`]): a clone retains it. `LOCAL` is whether this build created the handle
/// (see [`Payload::foreign_local`]); it is in the type, not a field, so the payload stays small.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub(super) struct ForeignStorage<const LOCAL: bool>(pub(super) ForeignHandle);

impl<const LOCAL: bool> ForeignStorage<LOCAL> {
    /// Whether both hold one foreign value: the same data under the same owner.
    pub(super) fn same_value(&self, other: &Self) -> bool {
        core::ptr::eq(self.0.data(), other.0.data())
            && core::ptr::eq(self.0.owner().ptr, other.0.owner().ptr)
    }
}

impl<const LOCAL: bool> PayloadStorage for ForeignStorage<LOCAL> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn value_any(&self) -> Option<&dyn Any> {
        Some(&self.0)
    }

    fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
        Some(&self.0)
    }

    fn rust_type_name(&self) -> Option<&'static str> {
        Some(core::any::type_name::<ForeignHandle>())
    }

    fn foreign_handle(&self) -> Option<&ForeignHandle> {
        Some(&self.0)
    }

    fn foreign_handle_is_local(&self) -> bool {
        LOCAL
    }
}

/// Storage holding an owner value `O` in an `Arc`: typed storage, or the value's own
/// allocation ([`ArcValue`]).
trait OwnerStorage<O>: PayloadStorage {
    fn owner(&self) -> &O;
    /// The owner's `Arc`, given the `Arc` holding this storage: a reference to the storage
    /// reaches the value alone, not the reference counts beside it.
    fn owner_arc(&self, storage: &Arc<dyn PayloadStorage>) -> Option<Arc<O>>;
}

impl<O: Send + Sync + 'static> OwnerStorage<O> for TypedStorage<O> {
    fn owner(&self) -> &O {
        &self.value
    }

    fn owner_arc(&self, _storage: &Arc<dyn PayloadStorage>) -> Option<Arc<O>> {
        Some(self.value.clone())
    }
}

#[cfg(target_has_atomic = "ptr")]
impl<O: Send + Sync + 'static> OwnerStorage<O> for ArcValue<O> {
    fn owner(&self) -> &O {
        &self.0
    }

    fn owner_arc(&self, storage: &Arc<dyn PayloadStorage>) -> Option<Arc<O>> {
        arc_value(storage)
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

    fn to_foreign_handle_in(&self, storage: &Arc<dyn PayloadStorage>) -> Option<ForeignHandle> {
        let owner = self.inner.owner_arc(storage)?;
        Some(ForeignHandle::from_arc::<O, I>(owner))
    }
}

impl Payload {
    /// A payload carrying `handle` under `type_key` (normally the interface key): one received
    /// from another build (see [`Self::foreign_local`]).
    pub fn foreign(
        type_key: impl Into<TypeKey>,
        handle: ForeignHandle,
        residency: Residency,
    ) -> Self {
        Self::foreign_with(type_key.into(), handle, residency, false)
    }

    /// [`Self::foreign`] for a handle this build created (its vtable and owner functions are
    /// this build's code). The stable plugin boundary passes that on
    /// ([`Self::foreign_handle_is_local`]), so a host keeps the creating plugin loaded.
    pub fn foreign_local(
        type_key: impl Into<TypeKey>,
        handle: ForeignHandle,
        residency: Residency,
    ) -> Self {
        Self::foreign_with(type_key.into(), handle, residency, true)
    }

    /// The payload holding `handle` in place: no allocation (see [`Body::Foreign`]).
    pub(super) fn foreign_with(
        type_key: TypeKey,
        handle: ForeignHandle,
        residency: Residency,
        local: bool,
    ) -> Self {
        Self {
            type_key,
            storage: if local {
                Body::LocalForeign(ForeignStorage(handle), residency)
            } else {
                Body::Foreign(ForeignStorage(handle), residency)
            },
            layout: None,
            residency_cache: ResidencyCache::default(),
            lineage: PayloadLineage::new(),
        }
    }

    /// The foreign handle this payload carries, if any (see [`Self::foreign_borrow`] for any
    /// foreign-readable payload).
    pub fn foreign_handle(&self) -> Option<&ForeignHandle> {
        self.storage.get().foreign_handle()
    }

    /// Whether [`Self::foreign_handle`] was created by this build ([`Self::foreign_local`]).
    pub fn foreign_handle_is_local(&self) -> bool {
        self.storage.get().foreign_handle_is_local()
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
        let any = self.storage.get().as_any();
        let at = |value: &dyn Any| core::ptr::addr_eq(value, self.storage.addr());
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
        Ok(Payload::foreign_local(I::KEY, handle, self.residency()).with_lineage(self.lineage))
    }

    /// A payload of key `key` sharing `owner`, readable through interface `I`: what a node or
    /// host returns for an owner type with a provider (a camera's frame type providing
    /// `daedalus:frame`, say). A consumer of another build, such as a stable plugin's node or its
    /// host, gets a [`ForeignHandle`] of the owner (no copy); a consumer of this build still reads
    /// `get_ref::<O>()`. The payload keeps `key` (the owner's key, so graph ports typed by it
    /// still match), unlike [`Self::provide_foreign`], which retypes to `I::KEY`: the stable path
    /// lends a handle by the payload's storage, not its key.
    ///
    /// Each call allocates its `Arc`. A stream of frames that must allocate nothing once warm uses
    /// [`Self::providing_recycled`] with a [`RecyclePool`](crate::RecyclePool) instead: same key
    /// and views, with the owner's node reused rather than a fresh `Arc`.
    pub fn providing<O, I>(key: impl Into<TypeKey>, owner: Arc<O>, residency: Residency) -> Self
    where
        I: ForeignInterface,
        O: ProvideForeign<I>,
    {
        let key = key.into();
        let mut payload = Self::shared_with(key.clone(), owner, residency, None, None)
            .provide_foreign::<O, I>()
            // Shared storage holds an `O`, so this never fails; keep the payload if it did.
            .unwrap_or_else(|payload| *payload);
        payload.type_key = key;
        payload
    }

    /// Retype storage that is an `S` at the allocation's address as `ProvidedStorage<S, O, I>`.
    fn provided<S, O, I>(mut self) -> Self
    where
        S: OwnerStorage<O> + 'static,
        O: ProvideForeign<I>,
        I: ForeignInterface,
    {
        let residency = self.storage.residency();
        let raw: *const ProvidedStorage<S, O, I> = Arc::into_raw(self.storage.into_arc()).cast();
        // Safety: the allocation holds an `S` at this address (checked by the caller), and
        // `ProvidedStorage<S, O, I>` is a `repr(transparent)` wrapper of it: same size, alignment
        // and drop, so the `Arc` takes back its reference under the new vtable.
        let storage = unsafe { Arc::from_raw(raw as *const dyn PayloadStorage) };
        self.storage = Body::Shared(storage, residency);
        self.type_key = TypeKey::from_static(I::KEY);
        self
    }

    /// Lend the payload's value through a foreign interface: the [`ForeignHandle`] it carries,
    /// or the owner value of a [`Self::provide_foreign`] payload. `None` for other payloads.
    pub fn foreign_borrow(&self) -> Option<ForeignBorrow<'_>> {
        self.storage.get().foreign_borrow()
    }

    /// An owning [`ForeignHandle`] of [`Self::foreign_borrow`]'s value (a retain of the carried
    /// handle, or the owner's `Arc`), for consumers the borrow cannot cover, such as the stable
    /// plugin boundary.
    pub fn to_foreign_handle(&self) -> Option<ForeignHandle> {
        match &self.storage {
            Body::Shared(storage, _) => storage.to_foreign_handle_in(storage),
            Body::Inline(value, _) => value.to_foreign_handle(),
            Body::Foreign(value, _) => value.to_foreign_handle(),
            Body::LocalForeign(value, _) => value.to_foreign_handle(),
            Body::Recycled(value, _) => value.to_foreign_handle(),
        }
    }
}
