use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::{
    BoundaryCapabilities, BoundaryStorage, BoundaryTypeContract, CorrelationId, Layout,
    PayloadLineage, ReleaseMode, Residency, TypeKey, boundary_contract_for_type,
};

mod boundary;
mod residency;
mod storage;

pub use boundary::BoundaryPayloadError;
pub use residency::ResidencyCacheKey;
use residency::ResidentPayload;
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
    // Keep `Arc<Box<dyn PayloadStorage>>` rather than `Arc<dyn PayloadStorage>`
    // so unique payloads can recover owned storage without cloning.
    storage: Arc<Box<dyn PayloadStorage>>,
    residency: Residency,
    layout: Option<Layout>,
    residency_cache: Arc<BTreeMap<ResidencyCacheKey, ResidentPayload>>,
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
                &self.residency_cache.keys().collect::<Vec<_>>(),
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

    pub fn owned<T>(type_key: impl Into<TypeKey>, value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        let type_key = type_key.into();
        if let Some(contract) = boundary_contract_for_type::<T>()
            && contract.type_key == type_key
        {
            return Self::boundary_owned(type_key, value, contract.capabilities);
        }
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
            storage: Arc::new(Box::new(storage) as Box<dyn PayloadStorage>),
            residency: Residency::Cpu,
            layout: None,
            residency_cache: Arc::new(BTreeMap::new()),
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
            storage: Arc::new(Box::new(TypedStorage {
                type_key,
                value,
                bytes_estimate,
            }) as Box<dyn PayloadStorage>),
            residency,
            layout,
            residency_cache: Arc::new(BTreeMap::new()),
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
            storage: Arc::new(Box::new(BytesStorage { type_key, bytes }) as Box<dyn PayloadStorage>),
            residency: Residency::Cpu,
            layout: None,
            residency_cache: Arc::new(BTreeMap::new()),
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
        let required = BoundaryTypeContract::for_type::<T>(
            self.type_key.clone(),
            BoundaryCapabilities {
                borrow_ref: true,
                ..BoundaryCapabilities::default()
            },
        );
        self.storage
            .as_any()
            .downcast_ref::<BoundaryStorage>()
            .and_then(|storage| storage.try_borrow_ref::<T>(&required).ok())
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
        let required = BoundaryTypeContract::for_type::<T>(
            self.type_key.clone(),
            BoundaryCapabilities {
                borrow_mut: true,
                ..BoundaryCapabilities::default()
            },
        );
        if self.storage.as_any().is::<BoundaryStorage>() {
            let storage = Arc::get_mut(&mut self.storage)?;
            let storage = storage.as_any_mut().downcast_mut::<BoundaryStorage>()?;
            return storage.try_borrow_mut::<T>(&required).ok();
        }
        let storage = Arc::get_mut(&mut self.storage)?;
        let storage = storage.as_any_mut().downcast_mut::<TypedStorage<T>>()?;
        Arc::get_mut(&mut storage.value)
    }

    pub fn try_into_owned<T>(self) -> Result<T, Box<Self>>
    where
        T: Send + Sync + 'static,
    {
        if self.storage.as_any().is::<BoundaryStorage>() {
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
        if Arc::strong_count(&self.storage) != 1 || Arc::strong_count(&storage.value) != 1 {
            return Err(Box::new(self));
        }

        let Self {
            type_key,
            storage,
            residency,
            layout,
            residency_cache,
            lineage,
        } = self;
        let storage = match Arc::try_unwrap(storage) {
            Ok(storage) => storage,
            Err(storage) => {
                return Err(Box::new(Self {
                    type_key,
                    storage,
                    residency,
                    layout,
                    residency_cache,
                    lineage,
                }));
            }
        };
        let storage = match storage.into_any().downcast::<TypedStorage<T>>() {
            Ok(storage) => storage,
            Err(_) => unreachable!("payload storage type was checked before move"),
        };
        match Arc::try_unwrap(storage.value) {
            Ok(value) => Ok(value),
            Err(_) => unreachable!("payload value uniqueness was checked before move"),
        }
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
