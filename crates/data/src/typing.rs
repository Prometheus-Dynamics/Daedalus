use crate::model::{EnumVariant, TypeExpr, ValueType};
use std::any::{TypeId, type_name};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
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
    edges: BTreeMap<TypeExpr, BTreeSet<TypeExpr>>,
    resolved: BTreeMap<(TypeExpr, TypeExpr), bool>,
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
    let from = from.normalize();
    let to = to.normalize();
    let mut guard = compat_registry()
        .write()
        .expect("daedalus_data::typing compatibility registry lock poisoned");
    guard.edges.entry(from).or_default().insert(to);
    guard.resolved.clear();
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
    let from = from.clone().normalize();
    let to = to.clone().normalize();
    if from == to {
        return true;
    }
    let mut guard = compat_registry()
        .write()
        .expect("daedalus_data::typing compatibility registry lock poisoned");
    if let Some(cached) = guard.resolved.get(&(from.clone(), to.clone())) {
        return *cached;
    }
    let mut queue: VecDeque<TypeExpr> = VecDeque::new();
    let mut seen: BTreeSet<TypeExpr> = BTreeSet::new();
    queue.push_back(from.clone());
    seen.insert(from.clone());
    let mut found = false;
    while let Some(cur) = queue.pop_front() {
        if cur == to {
            found = true;
            break;
        }
        if let Some(nexts) = guard.edges.get(&cur) {
            for next in nexts {
                if seen.insert(next.clone()) {
                    queue.push_back(next.clone());
                }
            }
        }
    }
    guard.resolved.insert((from, to), found);
    found
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
