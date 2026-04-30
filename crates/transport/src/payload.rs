use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::{
    BoundaryCapabilities, BoundaryStorage, BoundaryTakeError, BoundaryTypeContract, CorrelationId,
    Layout, PayloadLineage, ReleaseMode, Residency, TypeKey, boundary_contract_for_type,
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

/// Type-erased payload storage.
pub trait PayloadStorage: Send + Sync + fmt::Debug {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync>;
    fn type_key(&self) -> &TypeKey;
    fn value_any(&self) -> Option<&dyn Any> {
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

struct TypedStorage<T: Send + Sync + 'static> {
    type_key: TypeKey,
    value: Arc<T>,
    bytes_estimate: Option<u64>,
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

    fn rust_type_name(&self) -> Option<&'static str> {
        Some(std::any::type_name::<T>())
    }

    fn bytes_estimate(&self) -> Option<u64> {
        self.bytes_estimate
    }
}

#[derive(Debug)]
struct BytesStorage {
    type_key: TypeKey,
    bytes: Arc<[u8]>,
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

    fn rust_type_name(&self) -> Option<&'static str> {
        Some(std::any::type_name::<Arc<[u8]>>())
    }

    fn bytes_estimate(&self) -> Option<u64> {
        Some(self.bytes.len() as u64)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResidencyCacheKey {
    pub type_key: TypeKey,
    pub residency: Residency,
    pub layout: Option<Layout>,
}

impl ResidencyCacheKey {
    pub fn new(type_key: impl Into<TypeKey>, residency: Residency, layout: Option<Layout>) -> Self {
        Self {
            type_key: type_key.into(),
            residency,
            layout,
        }
    }
}

#[derive(Clone)]
struct ResidentPayload {
    type_key: TypeKey,
    // The boxed trait object is intentional: owned extraction paths unwrap the
    // Arc, then consume the box through `PayloadStorage::into_any`.
    storage: Arc<Box<dyn PayloadStorage>>,
    residency: Residency,
    layout: Option<Layout>,
    lineage: PayloadLineage,
}

impl fmt::Debug for ResidentPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResidentPayload")
            .field("type_key", &self.type_key)
            .field("residency", &self.residency)
            .field("layout", &self.layout)
            .field("rust_type_name", &self.storage.rust_type_name())
            .field("bytes_estimate", &self.storage.bytes_estimate())
            .finish()
    }
}

impl ResidentPayload {
    fn key(&self) -> ResidencyCacheKey {
        ResidencyCacheKey::new(self.type_key.clone(), self.residency, self.layout.clone())
    }

    fn from_payload(payload: &Payload) -> Self {
        Self {
            type_key: payload.type_key.clone(),
            storage: payload.storage.clone(),
            residency: payload.residency,
            layout: payload.layout.clone(),
            lineage: payload.lineage.clone(),
        }
    }

    fn into_payload(self, cache: Arc<BTreeMap<ResidencyCacheKey, ResidentPayload>>) -> Payload {
        Payload {
            type_key: self.type_key,
            storage: self.storage,
            residency: self.residency,
            layout: self.layout,
            residency_cache: cache,
            lineage: self.lineage,
        }
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

    pub fn key(&self) -> ResidencyCacheKey {
        ResidencyCacheKey::new(self.type_key.clone(), self.residency, self.layout.clone())
    }

    pub fn with_cached_resident(mut self, resident: Payload) -> Self {
        self.insert_cached_resident(resident);
        self
    }

    pub fn insert_cached_resident(&mut self, resident: Payload) {
        let mut cache = (*self.residency_cache).clone();
        cache.extend(
            resident
                .residency_cache
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        let resident = ResidentPayload::from_payload(&resident);
        cache.insert(resident.key(), resident);
        self.residency_cache = Arc::new(cache);
    }

    pub fn cache_current(mut self) -> Self {
        let mut cache = (*self.residency_cache).clone();
        let resident = ResidentPayload::from_payload(&self);
        cache.insert(resident.key(), resident);
        self.residency_cache = Arc::new(cache);
        self
    }

    pub fn residency_cache_len(&self) -> usize {
        self.residency_cache.len()
    }

    pub fn cached_residencies(&self) -> impl Iterator<Item = &ResidencyCacheKey> {
        self.residency_cache.keys()
    }

    pub fn has_resident(
        &self,
        type_key: &TypeKey,
        residency: Residency,
        layout: Option<&Layout>,
    ) -> bool {
        let key = ResidencyCacheKey::new(type_key.clone(), residency, layout.cloned());
        self.key() == key || self.residency_cache.contains_key(&key)
    }

    pub fn resident(
        &self,
        type_key: &TypeKey,
        residency: Residency,
        layout: Option<&Layout>,
    ) -> Option<Payload> {
        let key = ResidencyCacheKey::new(type_key.clone(), residency, layout.cloned());
        if self.key() == key {
            return Some(self.clone());
        }
        self.residency_cache
            .get(&key)
            .cloned()
            .map(|resident| resident.into_payload(self.residency_cache.clone()))
    }

    pub fn resident_by_type(&self, type_key: &TypeKey, layout: Option<&Layout>) -> Option<Payload> {
        if &self.type_key == type_key
            && layout.is_none_or(|layout| self.layout.as_ref() == Some(layout))
        {
            return Some(self.clone());
        }

        const PREFERRED_RESIDENCY: [Residency; 4] = [
            Residency::Cpu,
            Residency::Gpu,
            Residency::CpuAndGpu,
            Residency::External,
        ];
        for residency in PREFERRED_RESIDENCY {
            if let Some(payload) = self.resident(type_key, residency, layout) {
                return Some(payload);
            }
        }
        self.residency_cache
            .values()
            .find(|resident| {
                &resident.type_key == type_key
                    && layout.is_none_or(|layout| resident.layout.as_ref() == Some(layout))
            })
            .cloned()
            .map(|resident| resident.into_payload(self.residency_cache.clone()))
    }

    pub fn resident_ref<T>(
        &self,
        type_key: &TypeKey,
        residency: Residency,
        layout: Option<&Layout>,
    ) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        let key = ResidencyCacheKey::new(type_key.clone(), residency, layout.cloned());
        if self.key() == key {
            return self.get_ref::<T>();
        }
        self.residency_cache
            .get(&key)
            .and_then(|resident| resident.storage.as_any().downcast_ref::<TypedStorage<T>>())
            .map(|storage| storage.value.as_ref())
    }

    pub fn resident_arc<T>(
        &self,
        type_key: &TypeKey,
        residency: Residency,
        layout: Option<&Layout>,
    ) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let key = ResidencyCacheKey::new(type_key.clone(), residency, layout.cloned());
        if self.key() == key {
            return self.get_arc::<T>();
        }
        self.residency_cache
            .get(&key)
            .and_then(|resident| resident.storage.as_any().downcast_ref::<TypedStorage<T>>())
            .map(|storage| storage.value.clone())
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

    pub fn boundary_contract(&self) -> Option<&BoundaryTypeContract> {
        self.storage
            .as_any()
            .downcast_ref::<BoundaryStorage>()
            .map(BoundaryStorage::contract)
    }

    pub fn try_borrow_boundary_ref<T>(
        &self,
        required: &BoundaryTypeContract,
    ) -> Result<&T, BoundaryTakeError>
    where
        T: Send + Sync + 'static,
    {
        self.storage
            .as_any()
            .downcast_ref::<BoundaryStorage>()
            .ok_or(BoundaryTakeError::NotBoundary)?
            .try_borrow_ref(required)
    }

    pub fn try_borrow_boundary_mut<T>(
        &mut self,
        required: &BoundaryTypeContract,
    ) -> Result<&mut T, BoundaryTakeError>
    where
        T: Send + Sync + 'static,
    {
        let storage = Arc::get_mut(&mut self.storage).ok_or(BoundaryTakeError::Shared)?;
        storage
            .as_any_mut()
            .downcast_mut::<BoundaryStorage>()
            .ok_or(BoundaryTakeError::NotBoundary)?
            .try_borrow_mut(required)
    }

    pub fn try_take_boundary_owned<T>(
        self,
        required: &BoundaryTypeContract,
    ) -> Result<T, BoundaryPayloadError>
    where
        T: Send + Sync + 'static,
    {
        if !self.storage.as_any().is::<BoundaryStorage>() {
            return Err(BoundaryPayloadError(
                Box::new(self),
                BoundaryTakeError::NotBoundary,
            ));
        }
        if Arc::strong_count(&self.storage) != 1 {
            return Err(BoundaryPayloadError(
                Box::new(self),
                BoundaryTakeError::Shared,
            ));
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
                return Err(BoundaryPayloadError(
                    Box::new(Self {
                        type_key,
                        storage,
                        residency,
                        layout,
                        residency_cache,
                        lineage,
                    }),
                    BoundaryTakeError::Shared,
                ));
            }
        };
        let mut storage = match storage.into_any().downcast::<BoundaryStorage>() {
            Ok(storage) => storage,
            Err(_) => unreachable!("boundary storage type was checked before move"),
        };
        storage.try_take_owned::<T>(required).map_err(|err| {
            BoundaryPayloadError(
                Box::new(Self {
                    type_key,
                    storage: Arc::new(storage as Box<dyn PayloadStorage>),
                    residency,
                    layout,
                    residency_cache,
                    lineage,
                }),
                err,
            )
        })
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

#[derive(Debug)]
pub struct BoundaryPayloadError(pub Box<Payload>, pub BoundaryTakeError);

impl fmt::Display for BoundaryPayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.1.fmt(f)
    }
}

impl std::error::Error for BoundaryPayloadError {}

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
