use crate::portable::{Arc, arc_dyn};
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::any::Any;
use core::fmt;

use crate::{
    BoundaryCapabilities, BoundaryStorage, CorrelationId, Layout, PayloadLineage, ReleaseMode,
    Residency, TypeKey,
};

mod boundary;
mod foreign;
mod inline;
mod recycle;
mod residency;
mod slot;
mod storage;

pub use boundary::BoundaryPayloadError;
use inline::{Body, InlineValue};
pub use recycle::RecyclePool;
pub(crate) use recycle::{NodeWeak, recycled_incarnation, recycled_ref};
use residency::ResidencyCache;
pub use residency::ResidencyCacheKey;
pub use slot::PayloadSlot;
pub use storage::PayloadStorage;
use storage::{ArcValue, BytesStorage, TypedStorage, arc_storage, typed_arc, typed_ref};

/// Opaque host-owned payload handle for Rust plugin fast paths.
#[derive(Clone, Debug)]
pub struct OpaquePayloadHandle {
    payload: Arc<Payload>,
}

impl OpaquePayloadHandle {
    pub fn new(payload: Payload) -> Self {
        Self {
            payload: Arc::new(payload),
        }
    }

    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    pub fn into_payload(self) -> Result<Payload, Self> {
        Arc::try_unwrap(self.payload).map_err(|payload| Self { payload })
    }
}

/// Generic runtime payload envelope.
#[derive(Clone)]
pub struct Payload {
    type_key: TypeKey,
    /// Single allocation: a shared Rust value's own `Arc` (`ArcValue`), so wrapping an `Arc<T>`
    /// allocates nothing; unique payloads recover the owned value in place
    /// ([`Payload::try_into_owned`]). A primitive ([`Payload::owned`]) is held inline instead.
    /// Holds the payload's [`Residency`] too, in its tag word's spare bytes.
    storage: Body,
    layout: Option<Layout>,
    /// Empty (and allocation-free) until a resident is cached.
    residency_cache: ResidencyCache,
    lineage: PayloadLineage,
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Payload")
            .field("type_key", &self.type_key)
            .field("residency", &self.storage.residency())
            .field("layout", &self.layout)
            .field(
                "cached_residencies",
                &self.cached_residencies().collect::<Vec<_>>(),
            )
            .field("rust_type_name", &self.storage.get().rust_type_name())
            .field("bytes_estimate", &self.storage.get().bytes_estimate())
            .field("lineage", &self.lineage)
            .finish()
    }
}

impl Payload {
    pub fn shared<T>(type_key: impl Into<TypeKey>, value: Arc<T>) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self::shared_with(type_key, value, Residency::Cpu, None, None)
    }

    /// A payload owning `value`. A primitive (the integers, floats, `bool`,
    /// `char` and `()`) is held in the payload itself, with no allocation; any other value takes
    /// one allocation, the value's `Arc`. Boundary contracts are checked when a graph is compiled
    /// (against its registry's contracts); use [`Self::boundary_owned`] for a value whose access
    /// a contract must restrict at runtime.
    pub fn owned<T>(type_key: impl Into<TypeKey>, value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        let type_key = type_key.into();
        match InlineValue::of(&value) {
            Some(inline) => Self::with_body(type_key, Body::Inline(inline, Residency::Cpu)),
            None => Self::shared(type_key, Arc::new(value)),
        }
    }

    /// A payload owning a `Copy` `value`: held in the payload itself when it fits (a primitive,
    /// or at most 32 bytes with alignment at most 8), so it allocates nothing and a clone copies
    /// the value. A larger value takes [`Self::owned`]'s path, one allocation.
    pub fn plain<T>(type_key: impl Into<TypeKey>, value: T) -> Self
    where
        T: Copy + Send + Sync + 'static,
    {
        let type_key = type_key.into();
        match InlineValue::copied(&value) {
            Some(inline) => Self::with_body(type_key, Body::Inline(inline, Residency::Cpu)),
            None => Self::shared(type_key, Arc::new(value)),
        }
    }

    fn with_body(type_key: TypeKey, storage: Body) -> Self {
        Self {
            type_key,
            storage,
            layout: None,
            residency_cache: ResidencyCache::default(),
            lineage: PayloadLineage::new(),
        }
    }

    pub fn boundary_owned<T>(
        type_key: impl Into<TypeKey>,
        value: T,
        capabilities: BoundaryCapabilities,
    ) -> Self
    where
        T: Send + Sync + 'static,
    {
        let storage = BoundaryStorage::owned(type_key, value, capabilities);
        let type_key = storage.type_key.clone();
        Self::with_body(type_key, Body::Shared(arc_dyn!(storage), Residency::Cpu))
    }

    #[cfg(test)]
    pub(crate) fn boundary_shared<T>(
        type_key: impl Into<TypeKey>,
        value: T,
        capabilities: BoundaryCapabilities,
    ) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self::boundary_owned(type_key, value, capabilities)
    }

    /// A payload sharing `value`: its own allocation is the payload's storage unless a
    /// `bytes_estimate` needs a wrapper to carry it.
    pub fn shared_with<T>(
        type_key: impl Into<TypeKey>,
        value: Arc<T>,
        residency: Residency,
        layout: Option<Layout>,
        bytes_estimate: Option<u64>,
    ) -> Self
    where
        T: Send + Sync + 'static,
    {
        let type_key = type_key.into();
        let storage = match bytes_estimate {
            None => arc_storage(&type_key, value),
            Some(_) => arc_dyn!(TypedStorage {
                type_key: type_key.clone(),
                value,
                bytes_estimate,
            }),
        };
        Self {
            type_key,
            storage: Body::Shared(storage, residency),
            layout,
            residency_cache: ResidencyCache::default(),
            lineage: PayloadLineage::new(),
        }
    }

    pub fn bytes(bytes: Arc<[u8]>) -> Self {
        Self::bytes_with_type_key("bytes", bytes)
    }

    pub fn bytes_with_type_key(type_key: impl Into<TypeKey>, bytes: Arc<[u8]>) -> Self {
        Self::with_body(
            type_key.into(),
            Body::Shared(arc_dyn!(BytesStorage { bytes }), Residency::Cpu),
        )
    }

    pub fn with_lineage(mut self, lineage: PayloadLineage) -> Self {
        self.lineage = lineage;
        self
    }

    pub fn lineage(&self) -> &PayloadLineage {
        &self.lineage
    }

    /// Stamp lineage `created_at` with a reading of `clock`, the clock of the engine or bridge
    /// the payload is made for. Constructors read the platform clock, so this is a no-op (no
    /// second reading) for the platform clock (`Clock::default`).
    #[inline]
    pub fn stamp(mut self, clock: &daedalus_core::platform::Clock) -> Self {
        if !clock.is_platform() {
            self.lineage.created_at = clock.now();
        }
        self
    }

    pub fn correlation_id(&self) -> CorrelationId {
        self.lineage.correlation_id
    }

    pub fn release_mode(&self) -> ReleaseMode {
        self.storage.get().release_mode()
    }

    pub fn type_key(&self) -> &TypeKey {
        &self.type_key
    }

    pub fn residency(&self) -> Residency {
        self.storage.residency()
    }

    pub fn layout(&self) -> Option<&Layout> {
        self.layout.as_ref()
    }

    pub fn bytes_estimate(&self) -> Option<u64> {
        self.storage.get().bytes_estimate()
    }

    pub fn storage_rust_type_name(&self) -> Option<&'static str> {
        self.storage.get().rust_type_name()
    }

    /// `TypeId` of the stored Rust value (`None` for bytes payloads).
    pub fn storage_rust_type_id(&self) -> Option<core::any::TypeId> {
        self.storage.get().rust_type_id()
    }

    pub fn value_any(&self) -> Option<&dyn Any> {
        self.storage.get().value_any()
    }

    /// Borrow the payload value as `&(dyn Any + Send + Sync)`.
    ///
    /// Returns `None` for storage that does not expose a borrowable value (for example boundary
    /// storage without `borrow_ref` capability, or a boundary value that was already taken).
    pub fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
        self.storage.get().value_any_sync()
    }

    pub fn get_ref<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        if let Some(value) = typed_ref::<T>(self.storage.get()) {
            return Some(value);
        }
        // A recycled node holds its owner out of `typed_ref`'s reach (see `recycle::recycled_ref`).
        if let Body::Recycled(storage, _) = &self.storage
            && let Some(value) = recycle::recycled_ref::<T>(&storage.0)
        {
            return Some(value);
        }
        if let Some(storage) = self
            .storage
            .get()
            .as_any()
            .downcast_ref::<BoundaryStorage>()
        {
            if storage.holds::<T>() || !storage.may_hold::<T>() {
                return storage.borrow_ref_as::<T>(&self.type_key);
            }
            return storage.try_borrow_ref_as::<T>(&self.type_key).ok();
        }
        // Storage built by another copy of this crate (a separately built dynamic plugin) has
        // other wrapper `TypeId`s; its value can still be `T` (e.g. a std type).
        self.storage.get().value_any_sync()?.downcast_ref::<T>()
    }

    pub fn get_arc<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        typed_arc::<T>(&self.storage)
    }

    pub fn get_mut<T>(&mut self) -> Option<&mut T>
    where
        T: Send + Sync + 'static,
    {
        let type_key = &self.type_key;
        let storage = match &mut self.storage {
            Body::Inline(value, _) => return value.value_mut::<T>(),
            // A handle is lent, never mutated through the payload.
            Body::Foreign(..) | Body::LocalForeign(..) | Body::Recycled(..) => return None,
            Body::Shared(storage, _) => Arc::get_mut(storage)?,
        };
        if storage.as_any().is::<BoundaryStorage>() {
            let storage = storage.as_any_mut().downcast_mut::<BoundaryStorage>()?;
            if storage.holds::<T>() || !storage.may_hold::<T>() {
                return storage.borrow_mut_as::<T>(type_key);
            }
            return storage.try_borrow_mut_as::<T>(type_key).ok();
        }
        let storage = storage.as_any_mut();
        if storage.is::<ArcValue<T>>() {
            return storage
                .downcast_mut::<ArcValue<T>>()
                .map(|value| &mut value.0);
        }
        let storage = storage.downcast_mut::<TypedStorage<T>>()?;
        Arc::get_mut(&mut storage.value)
    }

    pub fn try_into_owned<T>(mut self) -> Result<T, Box<Self>>
    where
        T: Send + Sync + 'static,
    {
        if let Body::Inline(value, _) = self.storage {
            // Inline values are unique: the copy out of the payload always succeeds.
            return value.take::<T>().ok_or_else(|| Box::new(self));
        }
        if let Body::Recycled(storage, residency) = self.storage {
            // A unique node gives up its owner and goes back to its pool.
            return match storage.try_take::<T>() {
                Ok(value) => Ok(value),
                Err(storage) => {
                    self.storage = Body::Recycled(storage, residency);
                    Err(Box::new(self))
                }
            };
        }
        if let Some(storage) = self
            .storage
            .get()
            .as_any()
            .downcast_ref::<BoundaryStorage>()
        {
            let (holds, may_hold) = (storage.holds::<T>(), storage.may_hold::<T>());
            if !may_hold || self.storage.strong_count() != 1 {
                return Err(Box::new(self));
            }
            if holds {
                let taken = self
                    .storage
                    .get_mut()
                    .and_then(|storage| storage.as_any_mut().downcast_mut::<BoundaryStorage>())
                    .and_then(|storage| storage.take_owned_as::<T>(&self.type_key));
                return taken.ok_or_else(|| Box::new(self));
            }
            // A same-named type of another build: only the layout check can tell.
            return self
                .try_take_boundary_owned_as::<T>()
                .map_err(|payload| payload.0);
        }
        if self.storage.get().as_any().is::<ArcValue<T>>() {
            if self.storage.strong_count() != 1 {
                return Err(Box::new(self));
            }
            // A weak reference elsewhere may upgrade meanwhile: then the payload is handed back.
            let residency = self.storage.residency();
            let storage = match storage::into_arc_value::<T>(self.storage.into_arc()) {
                Ok(value) => match Arc::try_unwrap(value) {
                    Ok(value) => return Ok(value),
                    Err(value) => arc_storage(&self.type_key, value),
                },
                Err(storage) => storage,
            };
            self.storage = Body::Shared(storage, residency);
            return Err(Box::new(self));
        }
        let Some(storage) = self
            .storage
            .get()
            .as_any()
            .downcast_ref::<TypedStorage<T>>()
        else {
            return Err(Box::new(self));
        };
        if self.storage.strong_count() != 1
            || Arc::strong_count(&storage.value) != 1
            || !core::ptr::addr_eq(storage, self.storage.addr())
        {
            return Err(Box::new(self));
        }
        // Both handles are unique and owned by `self`, so nothing can clone them concurrently.
        // Safety: the allocation's value is the `TypedStorage<T>` checked above (same address),
        // so the pointer keeps its size and alignment.
        let storage = unsafe {
            Arc::from_raw(Arc::into_raw(self.storage.into_arc()).cast::<TypedStorage<T>>())
        };
        let storage =
            Arc::into_inner(storage).expect("payload storage uniqueness was checked before move");
        Ok(Arc::into_inner(storage.value)
            .expect("payload value uniqueness was checked before move"))
    }

    /// Whether `self` and `other` are clones of one payload (they share its storage), e.g. the
    /// copies of a graph constant handed out on every tick. Inline values share when they hold
    /// the same value of the same type: they are indistinguishable.
    pub fn shares_storage(&self, other: &Payload) -> bool {
        self.storage.shares_with(&other.storage)
    }

    /// Whether no other payload or `Arc` shares this payload's storage (for a shared value, its
    /// own allocation: other holders of the value count). An inline value is always unique.
    pub fn is_storage_unique(&self) -> bool {
        self.storage.strong_count() == 1
    }

    /// Strong references to the `T` the payload holds (payload clones and other holders). An
    /// inline value is held by its payload alone: one.
    pub fn typed_strong_count<T>(&self) -> Option<usize>
    where
        T: Send + Sync + 'static,
    {
        if let Body::Inline(value, _) = &self.storage {
            return value.value_ref::<T>().map(|_| 1);
        }
        let any = self.storage.get().as_any();
        if any.is::<ArcValue<T>>() {
            return Some(self.storage.strong_count());
        }
        any.downcast_ref::<TypedStorage<T>>()
            .map(|storage| Arc::strong_count(&storage.value))
    }

    pub fn is_typed_unique<T>(&self) -> bool
    where
        T: Send + Sync + 'static,
    {
        self.is_storage_unique() && self.typed_strong_count::<T>() == Some(1)
    }

    pub fn get_bytes(&self) -> Option<Arc<[u8]>> {
        self.storage
            .get()
            .as_any()
            .downcast_ref::<BytesStorage>()
            .map(|storage| storage.bytes.clone())
    }
}

/// Output wrapping for the code the node and adapter macros generate. Not for other use.
///
/// The generated code writes `OutputProbe::new(value).output_payload(key)` for each output, and
/// method resolution picks the payload's path from the output's type: [`CopyOutput`] for a `Copy`
/// type (held in the payload, as [`Payload::plain`]), [`OwnedOutput`] for any other (as
/// [`Payload::owned`]). The macro never names the type's traits, so the compiler makes the choice
/// at the call site.
#[doc(hidden)]
pub struct OutputProbe<T>(Option<T>);

impl<T> OutputProbe<T> {
    /// The probe of `value`.
    pub fn new(value: T) -> Self {
        Self(Some(value))
    }
}

/// The `Copy` path of [`OutputProbe`]: it applies to a probe of a `Copy` type, by value.
#[doc(hidden)]
pub trait CopyOutput {
    /// The payload of the probed value.
    fn output_payload(self, type_key: TypeKey) -> Payload;
}

impl<T: Copy + Send + Sync + 'static> CopyOutput for OutputProbe<T> {
    fn output_payload(self, type_key: TypeKey) -> Payload {
        let value = self
            .0
            .expect("an output probe holds its value until it is taken");
        Payload::plain(type_key, value)
    }
}

/// The owned path of [`OutputProbe`]: for a type that is not `Copy`, the probe is taken by
/// reference, so the value is moved out of it.
#[doc(hidden)]
pub trait OwnedOutput {
    /// The payload of the probed value, which the probe no longer holds.
    fn output_payload(&mut self, type_key: TypeKey) -> Payload;
}

impl<T: Send + Sync + 'static> OwnedOutput for OutputProbe<T> {
    fn output_payload(&mut self, type_key: TypeKey) -> Payload {
        let value = self
            .0
            .take()
            .expect("an output probe holds its value until it is taken");
        Payload::owned(type_key, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundary_shared_does_not_advertise_unimplemented_clone() {
        let payload = Payload::boundary_shared(
            "test:u32",
            1u32,
            BoundaryCapabilities {
                shared_clone: false,
                ..BoundaryCapabilities::rust_value()
            },
        );

        assert_eq!(
            payload
                .boundary_contract()
                .map(|contract| contract.capabilities.shared_clone),
            Some(false)
        );
    }
}
