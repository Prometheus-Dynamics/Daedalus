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

/// Support code for `#[derive(DaedalusTypeExpr)]`; not a public API.
#[doc(hidden)]
pub mod derive_support {
    use core::marker::PhantomData;

    use super::{DaedalusTypeExpr, DaedalusTypeVisitor};

    /// Autoref-specialization probe: `(&Probe::<T>(PhantomData)).visit_into(v)` visits `T` when
    /// it implements `DaedalusTypeExpr` and does nothing otherwise.
    pub struct Probe<T>(pub PhantomData<T>);

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

    use super::derive_support::{Probe, VisitTyped as _, VisitUntyped as _};
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
}
