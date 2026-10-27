use crate::portable::{Arc, arc_dyn};
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::any::Any;
use core::fmt;

use crate::{
    BoundaryCapabilities, BoundaryStorage, CorrelationId, Layout, PayloadLineage, ReleaseMode,
    Residency, SourceId, TypeKey,
};

mod boundary;
mod created;
mod foreign;
mod residency;
mod storage;

pub use boundary::BoundaryPayloadError;
use residency::ResidencyCache;
pub use residency::ResidencyCacheKey;
pub use storage::PayloadStorage;
use storage::{
    ArcValue, BytesStorage, TypedStorage, arc_storage, into_arc_value, typed_arc, typed_ref,
};

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
///
/// Its hot fields sit inline (about 128 bytes, two cache lines): the lineage a frame carries on
/// every tick (correlation id, creation time, sequence, source timestamp) and its layout. The
/// rare parts (a parent, a source id, cached residents) live in a block shared by clones and
/// allocated only when one is set.
#[derive(Clone)]
pub struct Payload {
    type_key: TypeKey,
    /// Single allocation: a shared Rust value's own `Arc` (`ArcValue`), so wrapping an `Arc<T>`
    /// allocates nothing; unique payloads recover the owned value in place
    /// ([`Payload::try_into_owned`]).
    storage: Arc<dyn PayloadStorage>,
    layout: Option<Layout>,
    correlation_id: CorrelationId,
    /// `created_at`, packed (see `created`).
    created_ns: i64,
    sequence: Option<u64>,
    source_timestamp: Option<u64>,
    residency: Residency,
    spill: Option<Arc<PayloadSpill>>,
}

/// The rarely set parts of a payload's lineage and its cached residents, shared by clones.
#[derive(Clone, Default)]
struct PayloadSpill {
    parent: Option<CorrelationId>,
    source_id: Option<SourceId>,
    residency_cache: ResidencyCache,
}

static NO_RESIDENTS: ResidencyCache = ResidencyCache::EMPTY;

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
            .field("lineage", &self.lineage())
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

    /// A payload owning `value` (one allocation, the value's `Arc`). Boundary contracts are
    /// checked when a graph is compiled (against its registry's contracts); use
    /// [`Self::boundary_owned`] for a value whose access a contract must restrict at runtime.
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
        Self::assemble(type_key, arc_dyn!(storage), Residency::Cpu, None)
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
        Self::assemble(type_key, storage, residency, layout)
    }

    pub fn bytes(bytes: Arc<[u8]>) -> Self {
        Self::bytes_with_type_key("bytes", bytes)
    }

    pub fn bytes_with_type_key(type_key: impl Into<TypeKey>, bytes: Arc<[u8]>) -> Self {
        Self::assemble(
            type_key.into(),
            arc_dyn!(BytesStorage { bytes }),
            Residency::Cpu,
            None,
        )
    }

    /// A payload with the given storage and a fresh lineage (see [`PayloadLineage::new`]).
    fn assemble(
        type_key: TypeKey,
        storage: Arc<dyn PayloadStorage>,
        residency: Residency,
        layout: Option<Layout>,
    ) -> Self {
        let lineage = PayloadLineage::new();
        Self {
            type_key,
            storage,
            layout,
            correlation_id: lineage.correlation_id,
            created_ns: created::encode(lineage.created_at),
            sequence: lineage.sequence,
            source_timestamp: lineage.source_timestamp,
            residency,
            spill: None,
        }
    }

    /// Replaces this payload's lineage with `lineage`, keeping its storage, layout and residents.
    pub fn with_lineage(mut self, lineage: PayloadLineage) -> Self {
        self.correlation_id = lineage.correlation_id;
        self.created_ns = created::encode(lineage.created_at);
        self.sequence = lineage.sequence;
        self.source_timestamp = lineage.source_timestamp;
        if lineage.parent.is_some() || lineage.source_id.is_some() || self.spill.is_some() {
            let spill = Arc::make_mut(self.spill.get_or_insert_with(Default::default));
            spill.parent = lineage.parent;
            spill.source_id = lineage.source_id;
        }
        self
    }

    /// A payload from a cached resident's parts, with `cache` as its residents.
    fn from_resident(
        type_key: TypeKey,
        storage: Arc<dyn PayloadStorage>,
        residency: Residency,
        layout: Option<Layout>,
        lineage: PayloadLineage,
        cache: ResidencyCache,
    ) -> Self {
        let spill = (!cache.is_empty() || lineage.parent.is_some() || lineage.source_id.is_some())
            .then(|| {
                Arc::new(PayloadSpill {
                    parent: lineage.parent,
                    source_id: lineage.source_id,
                    residency_cache: cache,
                })
            });
        Self {
            type_key,
            storage,
            layout,
            correlation_id: lineage.correlation_id,
            created_ns: created::encode(lineage.created_at),
            sequence: lineage.sequence,
            source_timestamp: lineage.source_timestamp,
            residency,
            spill,
        }
    }

    /// The payload's lineage, built from its fields (a value: the fields are not stored as one).
    pub fn lineage(&self) -> PayloadLineage {
        let spill = self.spill.as_deref();
        PayloadLineage {
            correlation_id: self.correlation_id,
            parent: spill.and_then(|spill| spill.parent),
            source_id: spill.and_then(|spill| spill.source_id.clone()),
            created_at: created::decode(self.created_ns),
            source_timestamp: self.source_timestamp,
            sequence: self.sequence,
        }
    }

    /// The residents cached on this payload (empty and allocation-free until one is cached).
    fn residency_cache(&self) -> &ResidencyCache {
        self.spill
            .as_deref()
            .map_or(&NO_RESIDENTS, |spill| &spill.residency_cache)
    }

    /// Mutable access to the residents cache; allocates the spill block on first use.
    fn residency_cache_mut(&mut self) -> &mut ResidencyCache {
        &mut Arc::make_mut(self.spill.get_or_insert_with(Default::default)).residency_cache
    }

    /// Stamp lineage `created_at` with a reading of `clock`, the clock of the engine or bridge
    /// the payload is made for. Constructors read the platform clock, so this is a no-op (no
    /// second reading) for the platform clock (`Clock::default`).
    #[inline]
    pub fn stamp(mut self, clock: &daedalus_core::platform::Clock) -> Self {
        if !clock.is_platform() {
            self.created_ns = created::encode(clock.now());
        }
        self
    }

    pub fn correlation_id(&self) -> CorrelationId {
        self.correlation_id
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
        if let Some(value) = typed_ref::<T>(self.storage.as_ref()) {
            return Some(value);
        }
        if let Some(storage) = self.storage.as_any().downcast_ref::<BoundaryStorage>() {
            if storage.holds::<T>() || !storage.may_hold::<T>() {
                return storage.borrow_ref_as::<T>(&self.type_key);
            }
            return storage.try_borrow_ref_as::<T>(&self.type_key).ok();
        }
        // Storage built by another copy of this crate (a separately built dynamic plugin) has
        // other wrapper `TypeId`s; its value can still be `T` (e.g. a std type).
        self.storage.value_any_sync()?.downcast_ref::<T>()
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
        if self.storage.as_any().is::<BoundaryStorage>() {
            let storage = Arc::get_mut(&mut self.storage)?;
            let storage = storage.as_any_mut().downcast_mut::<BoundaryStorage>()?;
            if storage.holds::<T>() || !storage.may_hold::<T>() {
                return storage.borrow_mut_as::<T>(&self.type_key);
            }
            return storage.try_borrow_mut_as::<T>(&self.type_key).ok();
        }
        let storage = Arc::get_mut(&mut self.storage)?.as_any_mut();
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
            // A same-named type of another build: only the layout check can tell.
            return self
                .try_take_boundary_owned_as::<T>()
                .map_err(|payload| payload.0);
        }
        if self.storage.as_any().is::<ArcValue<T>>() {
            if Arc::strong_count(&self.storage) != 1 {
                return Err(Box::new(self));
            }
            // A weak reference elsewhere may upgrade meanwhile: then the payload is handed back.
            let storage = match into_arc_value::<T>(self.storage) {
                Ok(value) => match Arc::try_unwrap(value) {
                    Ok(value) => return Ok(value),
                    Err(value) => arc_storage(&self.type_key, value),
                },
                Err(storage) => storage,
            };
            self.storage = storage;
            return Err(Box::new(self));
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

    /// Whether no other payload or `Arc` shares this payload's storage (for a shared value, its
    /// own allocation: other holders of the value count).
    pub fn is_storage_unique(&self) -> bool {
        Arc::strong_count(&self.storage) == 1
    }

    /// Strong references to the `T` the payload holds (payload clones and other holders).
    pub fn typed_strong_count<T>(&self) -> Option<usize>
    where
        T: Send + Sync + 'static,
    {
        let any = self.storage.as_any();
        if any.is::<ArcValue<T>>() {
            return Some(Arc::strong_count(&self.storage));
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
            .as_any()
            .downcast_ref::<BytesStorage>()
            .map(|storage| storage.bytes.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_hot_fields_fit_two_cache_lines() {
        assert!(core::mem::size_of::<Payload>() <= 128);
    }

    #[test]
    fn frame_lineage_with_layout_sequence_and_timestamp_allocates_no_spill() {
        let layout = Layout::from("frame:nv12");
        let lineage = PayloadLineage::new()
            .with_sequence(41)
            .with_source_timestamp(9_000);
        let payload = Payload::shared_with(
            "frame:test",
            Arc::new(7u32),
            Residency::Cpu,
            Some(layout.clone()),
            None,
        )
        .with_lineage(lineage);
        assert!(payload.spill.is_none());
        assert_eq!(payload.layout(), Some(&layout));
        let read = payload.lineage();
        assert_eq!(read.sequence, Some(41));
        assert_eq!(read.source_timestamp, Some(9_000));
        assert_eq!(read.correlation_id, payload.correlation_id());
    }

    #[test]
    fn created_at_round_trips_through_the_packed_reading() {
        let payload = Payload::owned("test:u32", 1u32);
        let created = payload.lineage().created_at;
        assert_eq!(payload.clone().lineage().created_at, created);
    }

    #[test]
    fn a_parent_allocates_the_spill_block_and_clones_share_it() {
        let parent = PayloadLineage::new();
        let child = Payload::owned("test:u32", 1u32)
            .with_lineage(PayloadLineage::new().with_parent(parent.correlation_id));
        assert!(child.spill.is_some());
        let copy = child.clone();
        assert!(Arc::ptr_eq(
            child.spill.as_ref().unwrap(),
            copy.spill.as_ref().unwrap()
        ));
        assert_eq!(copy.lineage().parent, Some(parent.correlation_id));
    }

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
