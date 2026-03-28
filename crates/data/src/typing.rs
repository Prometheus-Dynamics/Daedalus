use crate::model::{EnumVariant, TypeExpr, ValueType};
use std::any::{TypeId, type_name};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::OnceLock;
use std::sync::RwLock;

/// Registered mapping between a Rust type name and a `TypeExpr`.
///
/// ```
/// use daedalus_data::model::{TypeExpr, ValueType};
/// use daedalus_data::typing::register_type;
///
/// register_type::<u32>(TypeExpr::Scalar(ValueType::U32));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RegisteredType {
    pub rust: String,
    pub expr: TypeExpr,
}

struct TypeRegistry {
    by_type_id: HashMap<TypeId, RegisteredType>,
    by_rust_name: HashMap<String, TypeExpr>,
}

fn registry() -> &'static RwLock<TypeRegistry> {
    static REGISTRY: OnceLock<RwLock<TypeRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        RwLock::new(TypeRegistry {
            by_type_id: HashMap::new(),
            by_rust_name: HashMap::new(),
        })
    })
}

#[derive(Clone, Debug)]
struct CompatRegistry {
    edges: BTreeMap<TypeExpr, BTreeMap<TypeExpr, CompatibilityRule>>,
    resolved: BTreeMap<(TypeExpr, TypeExpr), Option<TypeCompatibilityPath>>,
}

fn compat_registry() -> &'static RwLock<CompatRegistry> {
    static REGISTRY: OnceLock<RwLock<CompatRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        RwLock::new(CompatRegistry {
            edges: BTreeMap::new(),
            resolved: BTreeMap::new(),
        })
    })
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityKind {
    #[default]
    Convert,
    View,
    Materialize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompatibilityRule {
    #[serde(default)]
    pub kind: CompatibilityKind,
    #[serde(default)]
    pub cost: u32,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub capabilities: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TypeCompatibilityEdge {
    pub from: TypeExpr,
    pub to: TypeExpr,
    pub rule: CompatibilityRule,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TypeCompatibilityPath {
    pub steps: Vec<TypeCompatibilityEdge>,
    #[serde(default)]
    pub total_cost: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RegisteredTypeCapabilities {
    pub ty: TypeExpr,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub capabilities: BTreeSet<String>,
}

#[derive(Clone, Debug, Default)]
struct CapabilityRegistry {
    by_type: BTreeMap<TypeExpr, BTreeSet<String>>,
}

fn capability_registry() -> &'static RwLock<CapabilityRegistry> {
    static REGISTRY: OnceLock<RwLock<CapabilityRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        RwLock::new(CapabilityRegistry {
            by_type: BTreeMap::new(),
        })
    })
}

fn normalize_rust_type_name(raw: &str) -> String {
    raw.chars().filter(|c| !c.is_whitespace()).collect()
}

fn rust_type_key<T: 'static>() -> String {
    normalize_rust_type_name(type_name::<T>())
}

/// Register a concrete `TypeExpr` for a Rust type `T`.
///
/// This is how applications/plugins can attach richer type information to
/// structured payloads without requiring proc-macro hard-coding.
///
/// ```
/// use daedalus_data::model::{TypeExpr, ValueType};
/// use daedalus_data::typing::{register_type, lookup_type};
///
/// register_type::<u16>(TypeExpr::Scalar(ValueType::U32));
/// assert!(lookup_type::<u16>().is_some());
/// ```
pub fn register_type<T: 'static>(expr: TypeExpr) {
    let rust = rust_type_key::<T>();
    let expr = expr.normalize();

    let mut guard = registry()
        .write()
        .expect("daedalus_data::typing registry lock poisoned");
    guard.by_rust_name.insert(rust.clone(), expr.clone());
    guard
        .by_type_id
        .insert(TypeId::of::<T>(), RegisteredType { rust, expr });
}

/// Register a type-compatibility edge from `from` to `to`.
///
/// Compatibility is directional; call twice for bidirectional compatibility.
///
/// ```
/// use daedalus_data::model::{TypeExpr, ValueType};
/// use daedalus_data::typing::{register_compatibility, can_convert_typeexpr};
///
/// register_compatibility(
///     TypeExpr::Scalar(ValueType::I32),
///     TypeExpr::Scalar(ValueType::Int),
/// );
/// assert!(can_convert_typeexpr(
///     &TypeExpr::Scalar(ValueType::I32),
///     &TypeExpr::Scalar(ValueType::Int),
/// ));
/// ```
pub fn register_compatibility(from: TypeExpr, to: TypeExpr) {
    register_compatibility_with_rule(from, to, CompatibilityRule::default());
}

pub fn register_compatibility_with_rule(from: TypeExpr, to: TypeExpr, rule: CompatibilityRule) {
    let from = from.normalize();
    let to = to.normalize();
    let mut guard = compat_registry()
        .write()
        .expect("daedalus_data::typing compatibility registry lock poisoned");
    guard.edges.entry(from).or_default().insert(to, rule);
    guard.resolved.clear();
}

pub fn compatibility_rule(from: &TypeExpr, to: &TypeExpr) -> Option<CompatibilityRule> {
    let from = from.clone().normalize();
    let to = to.clone().normalize();
    let guard = compat_registry()
        .read()
        .expect("daedalus_data::typing compatibility registry lock poisoned");
    guard
        .edges
        .get(&from)
        .and_then(|rules| rules.get(&to))
        .cloned()
}

pub fn register_type_capability(ty: TypeExpr, capability: impl Into<String>) {
    register_type_capabilities(ty, [capability]);
}

pub fn register_type_capabilities(
    ty: TypeExpr,
    capabilities: impl IntoIterator<Item = impl Into<String>>,
) {
    let ty = ty.normalize();
    let mut guard = capability_registry()
        .write()
        .expect("daedalus_data::typing capability registry lock poisoned");
    let caps = guard.by_type.entry(ty).or_default();
    for capability in capabilities {
        let capability = capability.into();
        if !capability.trim().is_empty() {
            caps.insert(capability);
        }
    }
}

pub fn type_capabilities(ty: &TypeExpr) -> BTreeSet<String> {
    let ty = ty.clone().normalize();
    let guard = capability_registry()
        .read()
        .expect("daedalus_data::typing capability registry lock poisoned");
    guard.by_type.get(&ty).cloned().unwrap_or_default()
}

pub fn has_type_capability(ty: &TypeExpr, capability: &str) -> bool {
    if capability.trim().is_empty() {
        return false;
    }
    type_capabilities(ty).contains(capability)
}

pub fn snapshot_type_capabilities() -> Vec<RegisteredTypeCapabilities> {
    let guard = capability_registry()
        .read()
        .expect("daedalus_data::typing capability registry lock poisoned");
    guard
        .by_type
        .iter()
        .map(|(ty, capabilities)| RegisteredTypeCapabilities {
            ty: ty.clone(),
            capabilities: capabilities.clone(),
        })
        .collect()
}

pub fn snapshot_compatibility_edges() -> Vec<TypeCompatibilityEdge> {
    let guard = compat_registry()
        .read()
        .expect("daedalus_data::typing compatibility registry lock poisoned");
    let mut out = Vec::new();
    for (from, nexts) in &guard.edges {
        for (to, rule) in nexts {
            out.push(TypeCompatibilityEdge {
                from: from.clone(),
                to: to.clone(),
                rule: rule.clone(),
            });
        }
    }
    out
}

/// Return true if `from` can be coerced into `to` based on registered compatibility.
///
/// ```
/// use daedalus_data::model::{TypeExpr, ValueType};
/// use daedalus_data::typing::can_convert_typeexpr;
///
/// assert!(can_convert_typeexpr(
///     &TypeExpr::Scalar(ValueType::Int),
///     &TypeExpr::Scalar(ValueType::Int),
/// ));
/// ```
pub fn can_convert_typeexpr(from: &TypeExpr, to: &TypeExpr) -> bool {
    explain_typeexpr_conversion(from, to).is_some()
}

pub fn explain_typeexpr_conversion(
    from: &TypeExpr,
    to: &TypeExpr,
) -> Option<TypeCompatibilityPath> {
    let from = from.clone().normalize();
    let to = to.clone().normalize();
    if from == to {
        return Some(TypeCompatibilityPath::default());
    }
    let mut guard = compat_registry()
        .write()
        .expect("daedalus_data::typing compatibility registry lock poisoned");
    if let Some(cached) = guard.resolved.get(&(from.clone(), to.clone())) {
        return cached.clone();
    }
    let resolved = resolve_compatibility_path(&guard.edges, &from, &to);
    guard.resolved.insert((from, to), resolved.clone());
    resolved
}

/// Register an enum (variants only) for Rust type `T`.
///
/// ```
/// use daedalus_data::typing::{register_enum, lookup_type};
/// register_enum::<bool>(["Yes", "No"]);
/// assert!(lookup_type::<bool>().is_some());
/// ```
pub fn register_enum<T: 'static>(variants: impl IntoIterator<Item = impl Into<String>>) {
    let variants = variants
        .into_iter()
        .map(|name| EnumVariant {
            name: name.into(),
            ty: None,
        })
        .collect();
    register_type::<T>(TypeExpr::Enum(variants));
}

/// Look up a previously registered `TypeExpr` for a Rust type `T`.
///
/// ```
/// use daedalus_data::model::{TypeExpr, ValueType};
/// use daedalus_data::typing::{register_type, lookup_type};
/// register_type::<u8>(TypeExpr::Scalar(ValueType::U32));
/// let found = lookup_type::<u8>().unwrap();
/// assert!(matches!(found, TypeExpr::Scalar(_)));
/// ```
pub fn lookup_type<T: 'static>() -> Option<TypeExpr> {
    let guard = registry()
        .read()
        .expect("daedalus_data::typing registry lock poisoned");
    guard
        .by_type_id
        .get(&TypeId::of::<T>())
        .map(|v| v.expr.clone())
}

fn builtin_type_expr<T: 'static>() -> Option<TypeExpr> {
    let tid = TypeId::of::<T>();
    let scalar = |v| TypeExpr::Scalar(v);

    if tid == TypeId::of::<()>() {
        return Some(scalar(ValueType::Unit));
    }
    if tid == TypeId::of::<bool>() {
        return Some(scalar(ValueType::Bool));
    }

    if tid == TypeId::of::<i8>()
        || tid == TypeId::of::<i16>()
        || tid == TypeId::of::<i32>()
        || tid == TypeId::of::<i64>()
        || tid == TypeId::of::<i128>()
        || tid == TypeId::of::<isize>()
        || tid == TypeId::of::<u8>()
        || tid == TypeId::of::<u16>()
        || tid == TypeId::of::<u32>()
        || tid == TypeId::of::<u64>()
        || tid == TypeId::of::<u128>()
        || tid == TypeId::of::<usize>()
    {
        return Some(scalar(ValueType::Int));
    }

    if tid == TypeId::of::<f32>() || tid == TypeId::of::<f64>() {
        return Some(scalar(ValueType::Float));
    }

    if tid == TypeId::of::<String>() {
        return Some(scalar(ValueType::String));
    }

    if tid == TypeId::of::<Vec<u8>>() {
        return Some(scalar(ValueType::Bytes));
    }

    None
}

/// Return an explicit type expression if `T` has either been registered or is
/// covered by built-in mappings (without falling back to `Opaque`).
///
/// ```
/// use daedalus_data::typing::override_type_expr;
/// use daedalus_data::model::TypeExpr;
/// let ty = override_type_expr::<u32>().unwrap();
/// assert!(matches!(ty, TypeExpr::Scalar(_)));
/// ```
pub fn override_type_expr<T: 'static>() -> Option<TypeExpr> {
    lookup_type::<T>().or_else(builtin_type_expr::<T>)
}

/// Returns the best-effort `TypeExpr` for a Rust type `T`.
///
/// Resolution order:
/// 1) `register_type::<T>(...)`
/// 2) Built-in primitives and common shims (e.g. `Vec<u8>` as `Bytes`)
/// 3) `Opaque("rust:<type_name>")` fallback
///
/// ```
/// use daedalus_data::typing::type_expr;
/// let ty = type_expr::<String>();
/// assert!(format!("{ty:?}").contains("String"));
/// ```
pub fn type_expr<T: 'static>() -> TypeExpr {
    if let Some(expr) = override_type_expr::<T>() {
        return expr;
    }
    TypeExpr::Opaque(format!("rust:{}", rust_type_key::<T>()))
}

/// Look up a previously registered `TypeExpr` by Rust type name (whitespace is ignored).
///
/// This is stable across dylib/plugin boundaries where `TypeId` differs but `type_name::<T>()`
/// is identical (compiled from the same sources).
pub fn lookup_type_by_rust_name(raw: &str) -> Option<TypeExpr> {
    let key = normalize_rust_type_name(raw);
    let guard = registry()
        .read()
        .expect("daedalus_data::typing registry lock poisoned");
    guard.by_rust_name.get(&key).cloned()
}

/// Snapshot the current registry as a list keyed by Rust type name.
///
/// Intended for UIs and tooling (e.g. exposing enum/struct definitions registered by plugins).
pub fn snapshot_by_rust_name() -> Vec<RegisteredType> {
    let guard = registry()
        .read()
        .expect("daedalus_data::typing registry lock poisoned");
    let mut out: Vec<RegisteredType> = guard
        .by_rust_name
        .iter()
        .map(|(rust, expr)| RegisteredType {
            rust: rust.clone(),
            expr: expr.clone(),
        })
        .collect();
    out.sort_by(|a, b| a.rust.cmp(&b.rust));
    out
}

fn resolve_compatibility_path(
    edges: &BTreeMap<TypeExpr, BTreeMap<TypeExpr, CompatibilityRule>>,
    from: &TypeExpr,
    to: &TypeExpr,
) -> Option<TypeCompatibilityPath> {
    let mut frontier: Vec<(TypeExpr, u32)> = vec![(from.clone(), 0)];
    let mut best_costs: BTreeMap<TypeExpr, u32> = BTreeMap::new();
    let mut previous: BTreeMap<TypeExpr, (TypeExpr, CompatibilityRule)> = BTreeMap::new();
    best_costs.insert(from.clone(), 0);

    while let Some((idx, (current, current_cost))) = frontier
        .iter()
        .enumerate()
        .min_by_key(|(_, (_, cost))| *cost)
        .map(|(idx, (ty, cost))| (idx, (ty.clone(), *cost)))
    {
        frontier.swap_remove(idx);
        if current == *to {
            break;
        }
        if let Some(nexts) = edges.get(&current) {
            for (next, rule) in nexts {
                let next_cost = current_cost.saturating_add(rule.cost);
                let is_better = best_costs
                    .get(next)
                    .map(|existing| next_cost < *existing)
                    .unwrap_or(true);
                if is_better {
                    best_costs.insert(next.clone(), next_cost);
                    previous.insert(next.clone(), (current.clone(), rule.clone()));
                    frontier.push((next.clone(), next_cost));
                }
            }
        }
    }

    let total_cost = best_costs.get(to).copied()?;
    let mut steps = Vec::new();
    let mut current = to.clone();
    while let Some((prev, rule)) = previous.get(&current).cloned() {
        steps.push(TypeCompatibilityEdge {
            from: prev.clone(),
            to: current.clone(),
            rule,
        });
        current = prev;
    }
    steps.reverse();
    Some(TypeCompatibilityPath { steps, total_cost })
}

#[cfg(test)]
mod tests {
    use super::{
        CompatibilityKind, CompatibilityRule, can_convert_typeexpr, compatibility_rule,
        explain_typeexpr_conversion, has_type_capability, register_compatibility_with_rule,
        register_type_capabilities, snapshot_compatibility_edges, snapshot_type_capabilities,
        type_capabilities,
    };
    use crate::model::TypeExpr;
    use std::collections::BTreeSet;

    #[test]
    fn capabilities_are_registered_per_typeexpr() {
        let ty = TypeExpr::Opaque("test:semantic:image".to_string());
        register_type_capabilities(
            ty.clone(),
            ["croppable", "luma-readable", "cpu-materializable"],
        );
        let caps = type_capabilities(&ty);
        assert!(caps.contains("croppable"));
        assert!(caps.contains("luma-readable"));
        assert!(has_type_capability(&ty, "cpu-materializable"));
        assert!(!has_type_capability(&ty, "gpu-materializable"));
    }

    #[test]
    fn compatibility_rules_preserve_metadata() {
        let from = TypeExpr::Opaque("test:semantic:dynamic".to_string());
        let to = TypeExpr::Opaque("test:semantic:gray".to_string());
        register_compatibility_with_rule(
            from.clone(),
            to.clone(),
            CompatibilityRule {
                kind: CompatibilityKind::View,
                cost: 1,
                capabilities: ["luma-readable".to_string(), "view-compatible".to_string()]
                    .into_iter()
                    .collect(),
            },
        );
        assert!(can_convert_typeexpr(&from, &to));
        let rule = compatibility_rule(&from, &to).expect("compatibility rule should exist");
        assert_eq!(rule.kind, CompatibilityKind::View);
        assert_eq!(rule.cost, 1);
        assert!(rule.capabilities.contains("view-compatible"));
        assert!(rule.capabilities.contains("luma-readable"));
    }

    #[test]
    fn explain_typeexpr_conversion_returns_lowest_cost_path() {
        let from = TypeExpr::Opaque("test:semantic:dynamic:from".to_string());
        let mid = TypeExpr::Opaque("test:semantic:dynamic:mid".to_string());
        let to = TypeExpr::Opaque("test:semantic:dynamic:to".to_string());

        register_compatibility_with_rule(
            from.clone(),
            to.clone(),
            CompatibilityRule {
                kind: CompatibilityKind::Materialize,
                cost: 10,
                capabilities: BTreeSet::new(),
            },
        );
        register_compatibility_with_rule(
            from.clone(),
            mid.clone(),
            CompatibilityRule {
                kind: CompatibilityKind::View,
                cost: 1,
                capabilities: ["view-compatible".to_string()].into_iter().collect(),
            },
        );
        register_compatibility_with_rule(
            mid.clone(),
            to.clone(),
            CompatibilityRule {
                kind: CompatibilityKind::Convert,
                cost: 2,
                capabilities: BTreeSet::new(),
            },
        );

        let path = explain_typeexpr_conversion(&from, &to).expect("path");
        assert_eq!(path.total_cost, 3);
        assert_eq!(path.steps.len(), 2);
        assert_eq!(path.steps[0].from, from);
        assert_eq!(path.steps[0].to, mid);
        assert_eq!(path.steps[1].to, to);
    }

    #[test]
    fn snapshot_helpers_include_registered_capabilities_and_edges() {
        let ty = TypeExpr::Opaque("test:snapshot:type".to_string());
        register_type_capabilities(ty.clone(), ["croppable"]);
        register_compatibility_with_rule(
            ty.clone(),
            TypeExpr::Opaque("test:snapshot:other".to_string()),
            CompatibilityRule::default(),
        );

        let capability_snapshot = snapshot_type_capabilities();
        assert!(
            capability_snapshot
                .iter()
                .any(|entry| { entry.ty == ty && entry.capabilities.contains("croppable") })
        );

        let edge_snapshot = snapshot_compatibility_edges();
        assert!(edge_snapshot.iter().any(|edge| edge.from == ty));
    }
}
