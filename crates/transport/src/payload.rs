use crate::portable::{Arc, arc_dyn};
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::any::Any;
use core::fmt;

use crate::{
    BoundaryCapabilities, BoundaryStorage, BoundaryTypeContract, CorrelationId, Layout,
    PayloadLineage, ReleaseMode, Residency, TypeKey,
};

mod boundary;
mod foreign;
mod residency;
mod storage;

pub use boundary::BoundaryPayloadError;
use residency::ResidencyCache;
pub use residency::ResidencyCacheKey;
pub use storage::PayloadStorage;
use storage::{BytesStorage, TypedStorage};

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
    /// Single allocation; unique payloads recover owned storage by downcasting it in place
    /// ([`Payload::try_into_owned`]).
    storage: Arc<dyn PayloadStorage>,
    residency: Residency,
    layout: Option<Layout>,
    /// Empty (and allocation-free) until a resident is cached.
    residency_cache: ResidencyCache,
    lineage: PayloadLineage,
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Payload")
            .field("type_key", &self.type_key)
            .field("residency", &self.residency)
            .field("layout", &self.layout)
            .field(
                "cached_residencies",
                &self.cached_residencies().collect::<Vec<_>>(),
            )
            .field("rust_type_name", &self.storage.rust_type_name())
            .field("bytes_estimate", &self.storage.bytes_estimate())
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

    /// A payload owning `value` in typed storage. Boundary contracts are checked when a graph
    /// is compiled (against its registry's contracts); use [`Self::boundary_owned`] for a value
    /// whose access a contract must restrict at runtime.
    pub fn owned<T>(type_key: impl Into<TypeKey>, value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self::shared(type_key, Arc::new(value))
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
        Self {
            type_key,
            storage: arc_dyn!(storage),
            residency: Residency::Cpu,
            layout: None,
            residency_cache: ResidencyCache::default(),
            lineage: PayloadLineage::new(),
        }
    }

    pub fn boundary_shared<T>(
        type_key: impl Into<TypeKey>,
        value: T,
        capabilities: BoundaryCapabilities,
    ) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self::boundary_owned(type_key, value, capabilities)
    }

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
        Self {
            type_key: type_key.clone(),
            storage: arc_dyn!(TypedStorage {
                type_key,
                value,
                bytes_estimate,
            }),
            residency,
            layout,
            residency_cache: ResidencyCache::default(),
            lineage: PayloadLineage::new(),
        }
    }

    pub fn bytes(bytes: Arc<[u8]>) -> Self {
        Self::bytes_with_type_key("bytes", bytes)
    }

    pub fn bytes_with_type_key(type_key: impl Into<TypeKey>, bytes: Arc<[u8]>) -> Self {
        let type_key = type_key.into();
        Self {
            type_key: type_key.clone(),
            storage: arc_dyn!(BytesStorage { type_key, bytes }),
            residency: Residency::Cpu,
            layout: None,
            residency_cache: ResidencyCache::default(),
            lineage: PayloadLineage::new(),
        }
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
        self.storage.release_mode()
    }

    pub fn type_key(&self) -> &TypeKey {
        &self.type_key
    }

    pub fn residency(&self) -> Residency {
        self.residency
    }

    pub fn layout(&self) -> Option<&Layout> {
        self.layout.as_ref()
    }

    pub fn bytes_estimate(&self) -> Option<u64> {
        self.storage.bytes_estimate()
    }

    pub fn storage_rust_type_name(&self) -> Option<&'static str> {
        self.storage.rust_type_name()
    }

    /// `TypeId` of the stored Rust value (`None` for bytes payloads).
    pub fn storage_rust_type_id(&self) -> Option<core::any::TypeId> {
        self.storage.rust_type_id()
    }

    pub fn value_any(&self) -> Option<&dyn Any> {
        self.storage.value_any()
    }

    /// Borrow the payload value as `&(dyn Any + Send + Sync)`.
    ///
    /// Returns `None` for storage that does not expose a borrowable value (for example boundary
    /// storage without `borrow_ref` capability, or a boundary value that was already taken).
    pub fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
        self.storage.value_any_sync()
    }

    pub fn get_ref<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        if let Some(value) = self
            .storage
            .as_any()
            .downcast_ref::<TypedStorage<T>>()
            .map(|storage| storage.value.as_ref())
        {
            return Some(value);
        }
        if let Some(storage) = self.storage.as_any().downcast_ref::<BoundaryStorage>() {
            if storage.holds::<T>() || !storage.may_hold::<T>() {
                return storage.borrow_ref_as::<T>(&self.type_key);
            }
            let required = BoundaryTypeContract::for_type::<T>(
                self.type_key.clone(),
                BoundaryCapabilities {
                    borrow_ref: true,
                    ..BoundaryCapabilities::default()
                },
            );
            return storage.try_borrow_ref::<T>(&required).ok();
        }
        // Storage built by another copy of this crate (a separately built dynamic plugin) has
        // other wrapper `TypeId`s; its value can still be `T` (e.g. a std type).
        self.storage.value_any_sync()?.downcast_ref::<T>()
    }

    pub fn get_arc<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.storage
            .as_any()
            .downcast_ref::<TypedStorage<T>>()
            .map(|storage| storage.value.clone())
    }

    pub fn get_mut<T>(&mut self) -> Option<&mut T>
    where
        T: Send + Sync + 'static,
    {
        if self.storage.as_any().is::<BoundaryStorage>() {
            let storage = Arc::get_mut(&mut self.storage)?;
            let storage = storage.as_any_mut().downcast_mut::<BoundaryStorage>()?;
            if storage.holds::<T>() || !storage.may_hold::<T>() {
                return storage.borrow_mut_as::<T>(&self.type_key);
            }
            let required = BoundaryTypeContract::for_type::<T>(
                self.type_key.clone(),
                BoundaryCapabilities {
                    borrow_mut: true,
                    ..BoundaryCapabilities::default()
                },
            );
            return storage.try_borrow_mut::<T>(&required).ok();
        }
        let storage = Arc::get_mut(&mut self.storage)?;
        let storage = storage.as_any_mut().downcast_mut::<TypedStorage<T>>()?;
        Arc::get_mut(&mut storage.value)
    }

    pub fn try_into_owned<T>(mut self) -> Result<T, Box<Self>>
    where
        T: Send + Sync + 'static,
    {
        if let Some(storage) = self.storage.as_any().downcast_ref::<BoundaryStorage>() {
            let (holds, may_hold) = (storage.holds::<T>(), storage.may_hold::<T>());
            if !may_hold || Arc::strong_count(&self.storage) != 1 {
                return Err(Box::new(self));
            }
            if holds {
                let taken = Arc::get_mut(&mut self.storage)
                    .and_then(|storage| storage.as_any_mut().downcast_mut::<BoundaryStorage>())
                    .and_then(|storage| storage.take_owned_as::<T>(&self.type_key));
                return taken.ok_or_else(|| Box::new(self));
            }
            // A same-named type of another build: only the full contract can tell.
            let type_key = self.type_key.clone();
            return self
                .try_take_boundary_owned::<T>(&BoundaryTypeContract::for_type::<T>(
                    type_key,
                    BoundaryCapabilities::owned(),
                ))
                .map_err(|payload| payload.0);
        }
        let Some(storage) = self.storage.as_any().downcast_ref::<TypedStorage<T>>() else {
            return Err(Box::new(self));
        };
        if Arc::strong_count(&self.storage) != 1
            || Arc::strong_count(&storage.value) != 1
            || !core::ptr::addr_eq(storage, Arc::as_ptr(&self.storage))
        {
            return Err(Box::new(self));
        }
        // Both handles are unique and owned by `self`, so nothing can clone them concurrently.
        // Safety: the allocation's value is the `TypedStorage<T>` checked above (same address),
        // so the pointer keeps its size and alignment.
        let storage =
            unsafe { Arc::from_raw(Arc::into_raw(self.storage).cast::<TypedStorage<T>>()) };
        let storage =
            Arc::into_inner(storage).expect("payload storage uniqueness was checked before move");
        Ok(Arc::into_inner(storage.value)
            .expect("payload value uniqueness was checked before move"))
    }

    /// Whether `self` and `other` are clones of one payload (they share its storage), e.g. the
    /// copies of a graph constant handed out on every tick.
    pub fn shares_storage(&self, other: &Payload) -> bool {
        core::ptr::addr_eq(Arc::as_ptr(&self.storage), Arc::as_ptr(&other.storage))
    }

    pub fn is_storage_unique(&self) -> bool {
        Arc::strong_count(&self.storage) == 1
    }

    pub fn typed_strong_count<T>(&self) -> Option<usize>
    where
        T: Send + Sync + 'static,
    {
        self.storage
            .as_any()
            .downcast_ref::<TypedStorage<T>>()
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
            .as_any()
            .downcast_ref::<BytesStorage>()
            .map(|storage| storage.bytes.clone())
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
