use std::any::TypeId;
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};

/// Identity of the concrete Rust type a [`TypeKey`](crate::TypeKey) carries in one build.
///
/// Two copies of Daedalus code linked separately (a host and a dynamic plugin) can agree on a
/// key and a type name yet disagree on the type itself, e.g. when a third-party crate was
/// compiled with different features. Comparing identities catches that before any payload is
/// downcast. `type_id_hash` hashes the `TypeId` with the standard library's fixed-key hasher,
/// so it is only comparable between builds of the same rustc (which the Rust-ABI plugin path
/// already requires).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RustTypeIdentity {
    pub type_name: &'static str,
    pub type_id_hash: u64,
    pub size: usize,
    pub align: usize,
}

impl RustTypeIdentity {
    pub fn of<T: 'static>() -> Self {
        let mut hasher = DefaultHasher::new();
        TypeId::of::<T>().hash(&mut hasher);
        Self {
            type_name: std::any::type_name::<T>(),
            type_id_hash: hasher.finish(),
            size: std::mem::size_of::<T>(),
            align: std::mem::align_of::<T>(),
        }
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
        assert!(a.to_string().contains("::A`"), "{a}");
    }
}
