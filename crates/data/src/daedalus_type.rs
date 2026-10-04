use crate::model::{TypeExpr, Value};

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

    /// Build `Self` from a graph value (a constant or a `Value` payload). The derive implements
    /// it for enums whose variants are all unit variants: a variant name (`Value::String` or
    /// `Value::Enum`, case-insensitive) or its index (`Value::Int`). The default builds nothing.
    fn from_value(_value: &Value) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
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
    use crate::model::{TypeExpr, Value};
    use crate::typing::TypeRegistry;

    /// Index of the unit variant `value` names among `names`: a name (case-insensitive) as
    /// `Value::String` or a payload-free `Value::Enum`, or an index as `Value::Int`.
    pub fn unit_variant_index(value: &Value, names: &[&str]) -> Option<usize> {
        let name = match value {
            Value::Int(index) => return usize::try_from(*index).ok().filter(|i| *i < names.len()),
            Value::String(name) => name.as_ref(),
            Value::Enum(ev) if ev.value.is_none() => ev.name.as_str(),
            _ => return None,
        };
        let name = name.trim();
        names.iter().position(|n| n.eq_ignore_ascii_case(name))
    }

    /// Autoref-specialization probe. Called as `(&Probe::<T>(PhantomData)).method()`, the
    /// `VisitTyped`/`KeyedLeaf` impls are picked when `T` implements `DaedalusTypeExpr` and the
    /// fallback impls otherwise:
    ///
    /// - `visit_into(v)` visits `T` (or nothing);
    /// - `declared_key()` is the key `T` owns (or `None`);
    /// - `leaf_type_expr(types)` is `Opaque(T::TYPE_KEY)`, resolved at compile time so it does
    ///   not depend on registration order, or `types.type_expr::<T>()` (the installing
    ///   registry's mapping, a builtin, or the `rust:` fallback) for types without one.
    pub struct Probe<T>(pub PhantomData<T>);

    pub trait KeyedLeaf {
        fn declared_key(&self) -> Option<&'static str>;
        fn leaf_type_expr(&self, types: &TypeRegistry) -> TypeExpr;
    }

    impl<T: DaedalusTypeExpr> KeyedLeaf for Probe<T> {
        fn declared_key(&self) -> Option<&'static str> {
            Some(T::TYPE_KEY)
        }

        fn leaf_type_expr(&self, _types: &TypeRegistry) -> TypeExpr {
            TypeExpr::Opaque(T::TYPE_KEY.to_string())
        }
    }

    pub trait RegistryLeaf {
        fn declared_key(&self) -> Option<&'static str> {
            None
        }

        fn leaf_type_expr(&self, types: &TypeRegistry) -> TypeExpr;
    }

    impl<T: 'static> RegistryLeaf for &Probe<T> {
        fn leaf_type_expr(&self, types: &TypeRegistry) -> TypeExpr {
            types.type_expr::<T>()
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
        let mut types = crate::typing::TypeRegistry::new();
        types
            .register_type::<Keyed>(TypeExpr::opaque("test:elsewhere"))
            .unwrap();
        types
            .register_type::<Unkeyed>(TypeExpr::opaque("test:mapped"))
            .unwrap();
        let keyed = &Probe::<Keyed>(PhantomData);
        assert_eq!(keyed.declared_key(), Some("test:keyed"));
        assert_eq!(keyed.leaf_type_expr(&types), TypeExpr::opaque("test:keyed"));
        let unkeyed = &Probe::<Unkeyed>(PhantomData);
        assert_eq!(unkeyed.declared_key(), None);
        assert_eq!(
            unkeyed.leaf_type_expr(&types),
            TypeExpr::opaque("test:mapped")
        );
        assert!(matches!(
            unkeyed.leaf_type_expr(crate::typing::TypeRegistry::empty()),
            TypeExpr::Opaque(key) if key.starts_with("rust:")
        ));
    }

    #[test]
    fn unit_variant_index_accepts_names_indices_and_enum_values() {
        use super::derive_support::unit_variant_index;
        use crate::model::EnumValue;
        let names = ["reflect", "wrap"];
        assert_eq!(
            unit_variant_index(&Value::String(" Wrap ".into()), &names),
            Some(1)
        );
        assert_eq!(unit_variant_index(&Value::Int(0), &names), Some(0));
        assert_eq!(unit_variant_index(&Value::Int(2), &names), None);
        assert_eq!(unit_variant_index(&Value::Int(-1), &names), None);
        let named = |value: Option<Value>| {
            Value::Enum(EnumValue {
                name: "wrap".into(),
                value: value.map(Box::new),
            })
        };
        assert_eq!(unit_variant_index(&named(None), &names), Some(1));
        assert_eq!(unit_variant_index(&named(Some(Value::Unit)), &names), None);
        assert_eq!(unit_variant_index(&Value::Bool(true), &names), None);
    }
}
