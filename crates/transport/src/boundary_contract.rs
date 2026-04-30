use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{LayoutHash, TypeKey};

/// Operations a payload type exposes across a Rust dynamic plugin boundary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundaryCapabilities {
    pub owned_move: bool,
    pub shared_clone: bool,
    pub borrow_ref: bool,
    pub borrow_mut: bool,
    pub metadata_read: bool,
    pub metadata_write: bool,
    pub backing_read: bool,
    pub backing_write: bool,
}

impl BoundaryCapabilities {
    pub const fn owned() -> Self {
        Self {
            owned_move: true,
            shared_clone: false,
            borrow_ref: false,
            borrow_mut: false,
            metadata_read: false,
            metadata_write: false,
            backing_read: false,
            backing_write: false,
        }
    }

    pub const fn rust_value() -> Self {
        Self {
            owned_move: true,
            shared_clone: true,
            borrow_ref: true,
            borrow_mut: true,
            metadata_read: false,
            metadata_write: false,
            backing_read: false,
            backing_write: false,
        }
    }

    pub const fn frame_like() -> Self {
        Self {
            owned_move: true,
            shared_clone: true,
            borrow_ref: true,
            borrow_mut: true,
            metadata_read: true,
            metadata_write: true,
            backing_read: true,
            backing_write: true,
        }
    }

    pub fn satisfies(self, required: Self) -> bool {
        (!required.owned_move || self.owned_move)
            && (!required.shared_clone || self.shared_clone)
            && (!required.borrow_ref || self.borrow_ref)
            && (!required.borrow_mut || self.borrow_mut)
            && (!required.metadata_read || self.metadata_read)
            && (!required.metadata_write || self.metadata_write)
            && (!required.backing_read || self.backing_read)
            && (!required.backing_write || self.backing_write)
    }
}

/// Dynamic plugin boundary contract for a concrete Rust payload type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundaryTypeContract {
    pub type_key: TypeKey,
    pub rust_type_name: Option<String>,
    pub abi_version: u32,
    pub layout_hash: LayoutHash,
    pub capabilities: BoundaryCapabilities,
}

impl BoundaryTypeContract {
    pub const ABI_VERSION: u32 = 1;

    pub fn new(
        type_key: impl Into<TypeKey>,
        layout_hash: impl Into<LayoutHash>,
        capabilities: BoundaryCapabilities,
    ) -> Self {
        Self {
            type_key: type_key.into(),
            rust_type_name: None,
            abi_version: Self::ABI_VERSION,
            layout_hash: layout_hash.into(),
            capabilities,
        }
    }

    pub fn for_type<T: 'static>(
        type_key: impl Into<TypeKey>,
        capabilities: BoundaryCapabilities,
    ) -> Self {
        let mut contract = Self::new(type_key, LayoutHash::for_type::<T>(), capabilities);
        contract.rust_type_name = Some(std::any::type_name::<T>().to_string());
        contract
    }

    pub fn compatible_with(&self, required: &Self) -> Result<(), BoundaryContractError> {
        if self.type_key != required.type_key {
            return Err(BoundaryContractError::TypeKey {
                expected: required.type_key.clone(),
                found: self.type_key.clone(),
            });
        }
        if self.abi_version != required.abi_version {
            return Err(BoundaryContractError::Abi {
                expected: required.abi_version,
                found: self.abi_version,
            });
        }
        if self.layout_hash != required.layout_hash {
            return Err(BoundaryContractError::Layout {
                expected: required.layout_hash.clone(),
                found: self.layout_hash.clone(),
            });
        }
        if !self.capabilities.satisfies(required.capabilities) {
            return Err(BoundaryContractError::Capabilities {
                expected: required.capabilities,
                found: self.capabilities,
            });
        }
        Ok(())
    }
}

static BOUNDARY_CONTRACTS_BY_RUST_TYPE: OnceLock<Mutex<BTreeMap<String, BoundaryTypeContract>>> =
    OnceLock::new();

pub fn register_boundary_contract(contract: BoundaryTypeContract) {
    if let Some(rust_type_name) = contract.rust_type_name.clone()
        && let Ok(mut contracts) = BOUNDARY_CONTRACTS_BY_RUST_TYPE
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
    {
        contracts.insert(rust_type_name, contract);
    }
}

pub fn boundary_contract_for_type<T: 'static>() -> Option<BoundaryTypeContract> {
    BOUNDARY_CONTRACTS_BY_RUST_TYPE
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()
        .and_then(|contracts| contracts.get(std::any::type_name::<T>()).cloned())
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum BoundaryContractError {
    #[error("boundary type key mismatch: expected {expected}, found {found}")]
    TypeKey { expected: TypeKey, found: TypeKey },
    #[error("boundary ABI mismatch: expected {expected}, found {found}")]
    Abi { expected: u32, found: u32 },
    #[error("boundary layout mismatch: expected {expected}, found {found}")]
    Layout {
        expected: LayoutHash,
        found: LayoutHash,
    },
    #[error("boundary capabilities mismatch: expected {expected:?}, found {found:?}")]
    Capabilities {
        expected: BoundaryCapabilities,
        found: BoundaryCapabilities,
    },
}
