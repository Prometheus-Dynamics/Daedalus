use crate::model::TypeExpr;

/// Trait for types that want a stable, Daedalus-facing schema identity.
///
/// The key is expected to be used as `TypeExpr::Opaque(<key>)` in port schemas so it remains
/// stable across plugin boundaries. The associated `TypeExpr` provides a richer schema for UI
/// tooling and optional host export validation.
pub trait DaedalusTypeExpr: 'static {
    const TYPE_KEY: &'static str;
    fn type_expr() -> TypeExpr;
}
