use crate::model::TypeExpr;

/// Trait for types that want a stable, Daedalus-facing schema identity.
///
/// The key is expected to be used as `TypeExpr::Opaque(<key>)` in port schemas so it remains
/// stable across plugin boundaries. The associated `TypeExpr` provides a richer schema for UI
/// tooling and optional host export validation.
pub trait DaedalusTypeExpr: 'static {
    const TYPE_KEY: &'static str;
    fn type_expr() -> TypeExpr;

    /// Visit the `DaedalusTypeExpr` types this schema refers to (direct field types, including
    /// those nested in `Vec`/`Option`/`Box`/`Arc`/arrays/tuples). Registries use this to register
    /// nested schemas before `Self`. The `DaedalusTypeExpr` derive implements it; the default
    /// visits nothing.
    fn visit_dependencies<V: DaedalusTypeVisitor>(_visitor: &mut V) {}
}

/// Receives the nested types reported by [`DaedalusTypeExpr::visit_dependencies`].
pub trait DaedalusTypeVisitor {
    fn visit<T: DaedalusTypeExpr>(&mut self);
}

/// Support code for the Daedalus derive and attribute macros; not a public API.
#[doc(hidden)]
pub mod derive_support {
    use core::marker::PhantomData;

    use super::{DaedalusTypeExpr, DaedalusTypeVisitor};
    use crate::model::TypeExpr;

    /// Autoref-specialization probe. Called as `(&Probe::<T>(PhantomData)).method()`, the
    /// `VisitTyped`/`KeyedLeaf` impls are picked when `T` implements `DaedalusTypeExpr` and the
    /// fallback impls otherwise:
    ///
    /// - `visit_into(v)` visits `T` (or nothing);
    /// - `declared_key()` is the key `T` owns (or `None`);
    /// - `leaf_type_expr()` is `Opaque(T::TYPE_KEY)`, resolved at compile time so it does not
    ///   depend on registration order, or [`crate::typing::type_expr`] for types without one.
    pub struct Probe<T>(pub PhantomData<T>);

    pub trait KeyedLeaf {
        fn declared_key(&self) -> Option<&'static str>;
        fn leaf_type_expr(&self) -> TypeExpr;
    }

    impl<T: DaedalusTypeExpr> KeyedLeaf for Probe<T> {
        fn declared_key(&self) -> Option<&'static str> {
            Some(T::TYPE_KEY)
        }

        fn leaf_type_expr(&self) -> TypeExpr {
            TypeExpr::Opaque(T::TYPE_KEY.to_string())
        }
    }

    pub trait RegistryLeaf {
        fn declared_key(&self) -> Option<&'static str> {
            None
        }

        fn leaf_type_expr(&self) -> TypeExpr;
    }

    impl<T: 'static> RegistryLeaf for &Probe<T> {
        fn leaf_type_expr(&self) -> TypeExpr {
            crate::typing::type_expr::<T>()
        }
    }

    pub trait VisitTyped {
        fn visit_into<V: DaedalusTypeVisitor>(&self, visitor: &mut V);
    }

    impl<T: DaedalusTypeExpr> VisitTyped for Probe<T> {
        fn visit_into<V: DaedalusTypeVisitor>(&self, visitor: &mut V) {
            visitor.visit::<T>();
        }
    }

    pub trait VisitUntyped {
        fn visit_into<V: DaedalusTypeVisitor>(&self, _visitor: &mut V) {}
    }

    impl<T> VisitUntyped for &Probe<T> {}
}

#[cfg(test)]
mod tests {
    use core::marker::PhantomData;

    use super::derive_support::{
        KeyedLeaf as _, Probe, RegistryLeaf as _, VisitTyped as _, VisitUntyped as _,
    };
    use super::*;

    struct Keyed;
    impl DaedalusTypeExpr for Keyed {
        const TYPE_KEY: &'static str = "test:keyed";
        fn type_expr() -> TypeExpr {
            TypeExpr::opaque(Self::TYPE_KEY)
        }
    }

    #[derive(Default)]
    struct Keys(Vec<&'static str>);
    impl DaedalusTypeVisitor for Keys {
        fn visit<T: DaedalusTypeExpr>(&mut self) {
            self.0.push(T::TYPE_KEY);
        }
    }

    #[test]
    // The explicit borrow is the autoref-specialization call shape the derive emits.
    #[allow(clippy::needless_borrow)]
    fn probe_visits_only_daedalus_types() {
        let mut keys = Keys::default();
        (&Probe::<Keyed>(PhantomData)).visit_into(&mut keys);
        (&Probe::<u32>(PhantomData)).visit_into(&mut keys);
        assert_eq!(keys.0, ["test:keyed"]);
    }

    #[test]
    #[allow(clippy::needless_borrow)]
    fn probe_prefers_the_declared_key_over_the_registry() {
        struct Unkeyed;
        let keyed = &Probe::<Keyed>(PhantomData);
        assert_eq!(keyed.declared_key(), Some("test:keyed"));
        assert_eq!(keyed.leaf_type_expr(), TypeExpr::opaque("test:keyed"));
        let unkeyed = &Probe::<Unkeyed>(PhantomData);
        assert_eq!(unkeyed.declared_key(), None);
        assert_eq!(
            unkeyed.leaf_type_expr(),
            crate::typing::type_expr::<Unkeyed>()
        );
    }
}
