//! Boundary-contract borrow/take paths on [`Payload`] and their error type.

use std::fmt;
use std::sync::Arc;

use crate::{BoundaryStorage, BoundaryTakeError, BoundaryTypeContract};

use super::{Payload, PayloadStorage};

impl Payload {
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
}

#[derive(Debug)]
pub struct BoundaryPayloadError(pub Box<Payload>, pub BoundaryTakeError);

impl fmt::Display for BoundaryPayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.1.fmt(f)
    }
}

impl std::error::Error for BoundaryPayloadError {}
