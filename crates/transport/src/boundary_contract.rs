use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

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

    pub fn for_schema<T: 'static>(
        type_key: impl Into<TypeKey>,
        schema: impl std::fmt::Display,
        capabilities: BoundaryCapabilities,
    ) -> Self {
        let mut contract = Self::new(type_key, LayoutHash::for_schema::<T>(schema), capabilities);
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

/// Explicit registry for boundary contracts keyed by Rust type name.
///
/// Use an owned registry when tests or plugin/runtime state need isolation. The global
/// registration helpers remain as compatibility wrappers for process-wide type registration.
#[derive(Clone, Debug, Default)]
pub struct BoundaryContractRegistry {
    contracts_by_rust_type: Arc<Mutex<BTreeMap<String, BoundaryTypeContract>>>,
}

impl BoundaryContractRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, contract: BoundaryTypeContract) {
        if let Some(rust_type_name) = contract.rust_type_name.clone()
            && let Ok(mut contracts) = self.contracts_by_rust_type.lock()
        {
            contracts.insert(rust_type_name, contract);
        }
    }

    pub fn contract_for_type<T: 'static>(&self) -> Option<BoundaryTypeContract> {
        self.contracts_by_rust_type
            .lock()
            .ok()
            .and_then(|contracts| contracts.get(std::any::type_name::<T>()).cloned())
    }
}

static GLOBAL_BOUNDARY_CONTRACTS: OnceLock<BoundaryContractRegistry> = OnceLock::new();

pub fn global_boundary_contract_registry() -> BoundaryContractRegistry {
    GLOBAL_BOUNDARY_CONTRACTS
        .get_or_init(BoundaryContractRegistry::new)
        .clone()
}

pub fn register_boundary_contract_in(
    registry: &BoundaryContractRegistry,
    contract: BoundaryTypeContract,
) {
    registry.register(contract);
}

pub fn boundary_contract_for_type_in<T: 'static>(
    registry: &BoundaryContractRegistry,
) -> Option<BoundaryTypeContract> {
    registry.contract_for_type::<T>()
}

pub fn register_boundary_contract(contract: BoundaryTypeContract) {
    global_boundary_contract_registry().register(contract);
}

pub fn boundary_contract_for_type<T: 'static>() -> Option<BoundaryTypeContract> {
    global_boundary_contract_registry().contract_for_type::<T>()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C)]
    struct Left {
        a: u32,
        b: u32,
    }

    #[repr(C)]
    struct Right {
        a: u32,
        b: u32,
    }

    #[test]
    fn schema_layout_hash_rejects_same_size_alignment_shape_changes() {
        assert_eq!(std::mem::size_of::<Left>(), std::mem::size_of::<Right>());
        assert_eq!(std::mem::align_of::<Left>(), std::mem::align_of::<Right>());

        let producer = BoundaryTypeContract::for_schema::<Left>(
            "example:frame",
            "struct Frame { width: u32, height: u32 }",
            BoundaryCapabilities::rust_value(),
        );
        let consumer = BoundaryTypeContract::for_schema::<Right>(
            "example:frame",
            "struct Frame { height: u32, width: u32 }",
            BoundaryCapabilities::rust_value(),
        );

        assert!(matches!(
            producer.compatible_with(&consumer),
            Err(BoundaryContractError::Layout { .. })
        ));
    }

    #[test]
    fn schema_layout_hash_accepts_matching_schema() {
        let producer = BoundaryTypeContract::for_schema::<Left>(
            "example:frame",
            "struct Frame { width: u32, height: u32 }",
            BoundaryCapabilities::rust_value(),
        );
        let consumer = BoundaryTypeContract::for_schema::<Left>(
            "example:frame",
            "struct Frame { width: u32, height: u32 }",
            BoundaryCapabilities::owned(),
        );

        assert!(producer.compatible_with(&consumer).is_ok());
    }

    #[test]
    fn owned_boundary_contract_registries_are_isolated() {
        let left = BoundaryContractRegistry::new();
        let right = BoundaryContractRegistry::new();
        left.register(BoundaryTypeContract::for_type::<Left>(
            "example:left",
            BoundaryCapabilities::rust_value(),
        ));

        assert_eq!(
            left.contract_for_type::<Left>()
                .map(|contract| contract.type_key),
            Some(TypeKey::from("example:left"))
        );
        assert!(right.contract_for_type::<Left>().is_none());
    }
}
