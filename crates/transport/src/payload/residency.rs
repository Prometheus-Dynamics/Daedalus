//! Residency cache: per-payload cached residents keyed by type, residency and layout.

use crate::portable::Arc;
use alloc::collections::BTreeMap;
use core::fmt;

use crate::{Layout, PayloadLineage, Residency, TypeKey};

use super::{Payload, PayloadStorage, TypedStorage};

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

/// Cached residents of one payload. `None` means empty, so plain payloads never allocate a map.
#[derive(Clone, Default)]
pub(super) struct ResidencyCache(Option<Arc<BTreeMap<ResidencyCacheKey, ResidentPayload>>>);

impl ResidencyCache {
    fn map(&self) -> Option<&BTreeMap<ResidencyCacheKey, ResidentPayload>> {
        self.0.as_deref()
    }

    fn get(&self, key: &ResidencyCacheKey) -> Option<&ResidentPayload> {
        self.map()?.get(key)
    }

    fn iter(&self) -> impl Iterator<Item = (&ResidencyCacheKey, &ResidentPayload)> {
        self.map().into_iter().flatten()
    }

    /// Copy-on-write access for inserting residents.
    fn make_mut(&mut self) -> &mut BTreeMap<ResidencyCacheKey, ResidentPayload> {
        Arc::make_mut(self.0.get_or_insert_with(Default::default))
    }
}

#[derive(Clone)]
pub(super) struct ResidentPayload {
    pub(super) type_key: TypeKey,
    pub(super) storage: Arc<dyn PayloadStorage>,
    pub(super) residency: Residency,
    pub(super) layout: Option<Layout>,
    pub(super) lineage: PayloadLineage,
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
    pub(super) fn key(&self) -> ResidencyCacheKey {
        ResidencyCacheKey::new(self.type_key.clone(), self.residency, self.layout.clone())
    }

    pub(super) fn from_payload(payload: &Payload) -> Self {
        Self {
            type_key: payload.type_key.clone(),
            storage: payload.storage.clone(),
            residency: payload.residency,
            layout: payload.layout.clone(),
            lineage: payload.lineage.clone(),
        }
    }

    pub(super) fn into_payload(self, cache: ResidencyCache) -> Payload {
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

impl Payload {
    pub fn key(&self) -> ResidencyCacheKey {
        ResidencyCacheKey::new(self.type_key.clone(), self.residency, self.layout.clone())
    }

    pub fn with_cached_resident(mut self, resident: Payload) -> Self {
        self.insert_cached_resident(resident);
        self
    }

    pub fn insert_cached_resident(&mut self, resident: Payload) {
        let cache = self.residency_cache.make_mut();
        cache.extend(
            resident
                .residency_cache
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        let resident = ResidentPayload::from_payload(&resident);
        cache.insert(resident.key(), resident);
    }

    pub fn cache_current(mut self) -> Self {
        let resident = ResidentPayload::from_payload(&self);
        self.residency_cache
            .make_mut()
            .insert(resident.key(), resident);
        self
    }

    pub fn residency_cache_len(&self) -> usize {
        self.residency_cache.map().map_or(0, BTreeMap::len)
    }

    pub fn cached_residencies(&self) -> impl Iterator<Item = &ResidencyCacheKey> {
        self.residency_cache.iter().map(|(key, _)| key)
    }

    pub fn has_resident(
        &self,
        type_key: &TypeKey,
        residency: Residency,
        layout: Option<&Layout>,
    ) -> bool {
        let key = ResidencyCacheKey::new(type_key.clone(), residency, layout.cloned());
        self.key() == key || self.residency_cache.get(&key).is_some()
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
            .iter()
            .map(|(_, resident)| resident)
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
}
