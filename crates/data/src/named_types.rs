use crate::model::TypeExpr;
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock, RwLock};

/// Policy for whether a port typed as a given schema should be considered exportable through
/// callers that explicitly encode host boundary payloads as JSON or bytes.
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

#[derive(Clone, Debug, Default)]
pub struct NamedTypeRegistry {
    types: Arc<RwLock<BTreeMap<String, NamedType>>>,
}

impl NamedTypeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn global() -> Self {
        static REG: OnceLock<NamedTypeRegistry> = OnceLock::new();
        REG.get_or_init(Self::new).clone()
    }

    pub fn register(
        &self,
        key: impl Into<String>,
        expr: TypeExpr,
        export: HostExportPolicy,
    ) -> Result<(), String> {
        let key = key.into();
        let expr = expr.normalize();
        let mut guard = self
            .types
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

    pub fn lookup(&self, key: &str) -> Option<NamedType> {
        let guard = self.types.read().ok()?;
        guard.get(key).cloned()
    }

    pub fn resolve_opaque(&self, expr: &TypeExpr) -> Option<TypeExpr> {
        if let TypeExpr::Opaque(key) = expr {
            return self.lookup(key).map(|t| t.expr);
        }
        None
    }

    pub fn export_policy_for(&self, expr: &TypeExpr) -> HostExportPolicy {
        use crate::model::{TypeExpr as TE, ValueType};

        match expr {
            TE::Scalar(ValueType::Bytes) => HostExportPolicy::Bytes,
            TE::Opaque(key) => self
                .lookup(key)
                .map(|t| t.export)
                .unwrap_or(HostExportPolicy::None),
            _ => HostExportPolicy::Value,
        }
    }

    pub fn snapshot(&self) -> Vec<NamedType> {
        let guard = match self.types.read() {
            Ok(guard) => guard,
            Err(_) => return Vec::new(),
        };
        guard.values().cloned().collect()
    }
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
    NamedTypeRegistry::global().register(key, expr, export)
}

pub fn lookup_named_type(key: &str) -> Option<NamedType> {
    NamedTypeRegistry::global().lookup(key)
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
    NamedTypeRegistry::global().snapshot()
}

#[cfg(test)]
mod tests {
    use super::{HostExportPolicy, NamedTypeRegistry};
    use crate::model::{TypeExpr, ValueType};

    #[test]
    fn owned_named_type_registries_do_not_leak_entries() {
        let left = NamedTypeRegistry::new();
        let right = NamedTypeRegistry::new();

        left.register(
            "test:named:type",
            TypeExpr::Scalar(ValueType::Bool),
            HostExportPolicy::Value,
        )
        .expect("register named type");

        assert!(left.lookup("test:named:type").is_some());
        assert!(right.lookup("test:named:type").is_none());
        assert_eq!(
            left.export_policy_for(&TypeExpr::Opaque("test:named:type".into())),
            HostExportPolicy::Value
        );
        assert_eq!(
            right.export_policy_for(&TypeExpr::Opaque("test:named:type".into())),
            HostExportPolicy::None
        );
    }
}
