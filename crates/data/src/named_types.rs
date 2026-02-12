use crate::model::TypeExpr;
use std::collections::BTreeMap;
use std::sync::{OnceLock, RwLock};

/// Policy for whether a port typed as a given schema should be considered exportable via
/// "serialized" host boundaries (JSON/bytes), e.g. `HostBridgeHandle::{try_pop_serialized,recv_serialized}`.
///
/// This is intentionally small and conservative:
/// - `Value`: can be represented as `daedalus_data::model::Value` (JSON-friendly)
/// - `Bytes`: should be exported as raw bytes
/// - `None`: should not be auto-serialized (callers must explicitly encode/convert in-graph)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum HostExportPolicy {
    Value,
    Bytes,
    #[default]
    None,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedType {
    pub key: String,
    pub expr: TypeExpr,
    pub export: HostExportPolicy,
}

fn registry() -> &'static RwLock<BTreeMap<String, NamedType>> {
    static REG: OnceLock<RwLock<BTreeMap<String, NamedType>>> = OnceLock::new();
    REG.get_or_init(|| RwLock::new(BTreeMap::new()))
}

/// Register a named type schema keyed by a stable string.
///
/// The `key` is expected to match `TypeExpr::Opaque(key)` when used in port schemas.
/// Registering the same key multiple times is allowed if the normalized `TypeExpr` matches.
pub fn register_named_type(
    key: impl Into<String>,
    expr: TypeExpr,
    export: HostExportPolicy,
) -> Result<(), String> {
    let key = key.into();
    let expr = expr.normalize();
    let mut guard = registry()
        .write()
        .map_err(|_| "daedalus_data::named_types registry lock poisoned".to_string())?;

    if let Some(prev) = guard.get(&key) {
        if prev.expr != expr || prev.export != export {
            return Err(format!(
                "named type conflict for key '{key}': existing expr/export differ"
            ));
        }
        return Ok(());
    }

    guard.insert(key.clone(), NamedType { key, expr, export });
    Ok(())
}

pub fn lookup_named_type(key: &str) -> Option<NamedType> {
    let guard = registry().read().ok()?;
    guard.get(key).cloned()
}

/// Resolve an opaque type key into its registered (normalized) schema, if present.
pub fn resolve_opaque(expr: &TypeExpr) -> Option<TypeExpr> {
    if let TypeExpr::Opaque(key) = expr {
        return lookup_named_type(key).map(|t| t.expr);
    }
    None
}

/// Determine the export policy for a given port schema.
///
/// Note: this only describes whether *serialized* host boundaries should attempt automatic
/// JSON/bytes encoding. It does not affect typed host polling (`try_pop::<T>()`) which can
/// still move runtime payloads like `DynamicImage` without serialization.
pub fn export_policy_for(expr: &TypeExpr) -> HostExportPolicy {
    use crate::model::{TypeExpr as TE, ValueType};

    match expr {
        TE::Scalar(ValueType::Bytes) => HostExportPolicy::Bytes,
        TE::Opaque(key) => lookup_named_type(key)
            .map(|t| t.export)
            .unwrap_or(HostExportPolicy::None),
        // Structural types are representable as `Value` (if the runtime payload is `Value`-like).
        _ => HostExportPolicy::Value,
    }
}

/// Snapshot the current named-type registry for UI/tooling.
pub fn snapshot() -> Vec<NamedType> {
    let guard = match registry().read() {
        Ok(guard) => guard,
        Err(_) => return Vec::new(),
    };
    guard.values().cloned().collect()
}
