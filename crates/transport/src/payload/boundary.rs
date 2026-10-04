//! Boundary-contract borrow/take paths on [`Payload`] and their error type.

use crate::portable::Arc;
use alloc::boxed::Box;
use core::fmt;

use crate::{BoundaryStorage, BoundaryTakeError, BoundaryTypeContract};

use super::Payload;

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
        mut self,
        required: &BoundaryTypeContract,
    ) -> Result<T, BoundaryPayloadError>
    where
        T: Send + Sync + 'static,
    {
        let taken = if self.storage.as_any().is::<BoundaryStorage>() {
            Arc::get_mut(&mut self.storage)
                .ok_or(BoundaryTakeError::Shared)
                .and_then(|storage| {
                    storage
                        .as_any_mut()
                        .downcast_mut::<BoundaryStorage>()
                        .ok_or(BoundaryTakeError::NotBoundary)?
                        .try_take_owned::<T>(required)
                })
        } else {
            Err(BoundaryTakeError::NotBoundary)
        };
        taken.map_err(|err| BoundaryPayloadError(Box::new(self), err))
    }
}

#[derive(Debug)]
pub struct BoundaryPayloadError(pub Box<Payload>, pub BoundaryTakeError);

impl fmt::Display for BoundaryPayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.1.fmt(f)
    }
}

impl core::error::Error for BoundaryPayloadError {}
