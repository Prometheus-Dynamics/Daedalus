use std::any::TypeId;
use std::fmt;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::TypeKey;

/// Identity of the concrete Rust type a [`TypeKey`] carries in one build.
///
/// Two copies of Daedalus code linked separately (a host and a dynamic plugin) can agree on a
/// key and a type name yet disagree on the type itself, e.g. when a third-party crate was
/// compiled with different features. Comparing identities catches that before any payload is
/// downcast. `type_id_hash` ([`Self::hash_type_id`]) is only comparable between builds of the
/// same rustc (which the Rust-ABI plugin path already requires).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RustTypeIdentity {
    pub type_name: &'static str,
    pub type_id_hash: u64,
    pub size: usize,
    pub align: usize,
}

impl RustTypeIdentity {
    pub fn of<T: 'static>() -> Self {
        Self {
            type_name: std::any::type_name::<T>(),
            type_id_hash: Self::hash_type_id(TypeId::of::<T>()),
            size: std::mem::size_of::<T>(),
            align: std::mem::align_of::<T>(),
        }
    }

    /// The `type_id_hash` of a type: a few instructions, so payload checks on hot paths can
    /// afford it. `TypeId` already is a hash; this only folds what its `Hash` impl writes.
    pub fn hash_type_id(id: TypeId) -> u64 {
        let mut hasher = FoldHasher(0xcbf2_9ce4_8422_2325);
        id.hash(&mut hasher);
        hasher.0
    }

    /// Whether both identities describe the same Rust type (the name is informational).
    pub fn same_type(&self, other: &Self) -> bool {
        (self.type_id_hash, self.size, self.align) == (other.type_id_hash, other.size, other.align)
    }
}

impl fmt::Display for RustTypeIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` (type id {:016x}, size {}, align {})",
            self.type_name, self.type_id_hash, self.size, self.align
        )
    }
}

/// FNV-1a over whatever `TypeId::hash` writes (one `u64` on current toolchains).
struct FoldHasher(u64);

impl Hasher for FoldHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = (self.0 ^ value).wrapping_mul(0x0000_0100_0000_01b3);
    }
}

/// Why a Rust type or payload could not be matched to a transport key.
#[derive(Clone, Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeKeyError {
    /// No installed plugin, foreign-type mapping or port names a key for the Rust type.
    #[error(
        "no type key for Rust type `{rust_type}` in this registry; install the plugin that owns \
         the type (its `#[type_key]`/`DaedalusTypeExpr` registration), map it with \
         `#[plugin(foreign_types(Type = \"key\"))]` or `PluginRegistry::register_foreign_type`, \
         give a port that uses it a `type_key`, or pass the key explicitly (`push_as`)"
    )]
    Unkeyed { rust_type: String },
    /// Ports use the Rust type under several keys and none of them is the type's own.
    #[error(
        "Rust type `{rust_type}` is used under several type keys ({keys}) and owns none of them; \
         pass the key explicitly (`push_as`) or register the type's own key",
        keys = join_keys(.keys)
    )]
    Ambiguous {
        rust_type: String,
        keys: Vec<TypeKey>,
    },
    /// The payload's key is registered for another Rust type.
    #[error(
        "payload for `{type_key}` holds `{found}` but this graph expects `{expected}` (built \
         separately?); host and Rust-ABI plugins must share one cargo build, or the host must \
         install the plugin that owns `{type_key}`"
    )]
    RustTypeMismatch {
        type_key: TypeKey,
        found: String,
        expected: String,
    },
}

fn join_keys(keys: &[TypeKey]) -> String {
    keys.iter()
        .map(|key| format!("`{key}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_distinguish_types_with_equal_layout() {
        struct A(#[allow(dead_code)] u32);
        struct B(#[allow(dead_code)] u32);
        let a = RustTypeIdentity::of::<A>();
        assert!(a.same_type(&RustTypeIdentity::of::<A>()));
        assert!(!a.same_type(&RustTypeIdentity::of::<B>()));
        assert_eq!((a.size, a.align), (4, 4));
        assert_eq!(
            a.type_id_hash,
            RustTypeIdentity::hash_type_id(TypeId::of::<A>())
        );
        assert!(a.to_string().contains("::A`"), "{a}");
    }
}
