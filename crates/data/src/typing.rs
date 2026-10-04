use crate::model::{EnumVariant, TypeExpr, Value, ValueType};
use parking_lot::RwLock;
use std::any::{Any, TypeId, type_name};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::convert::TryFrom;
use std::sync::OnceLock;

/// Registered mapping between a Rust type name and a `TypeExpr`.
///
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RegisteredType {
    pub rust: String,
    pub expr: TypeExpr,
}

/// A Rust type registered with two different type expressions.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("Rust type `{rust}` is already registered as {existing:?}; it cannot also be {new:?}")]
pub struct TypeConflict {
    pub rust: String,
    pub existing: TypeExpr,
    pub new: TypeExpr,
}

#[derive(Clone, Debug, Default)]
pub struct TypeRegistry {
    by_type_id: HashMap<TypeId, RegisteredType>,
    by_rust_name: HashMap<String, TypeExpr>,
    by_capability_type: BTreeMap<TypeExpr, BTreeSet<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RegisteredTypeCapabilities {
    pub ty: TypeExpr,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub capabilities: BTreeSet<String>,
}

fn registry() -> &'static RwLock<TypeRegistry> {
    static TYPE_REGISTRY: OnceLock<RwLock<TypeRegistry>> = OnceLock::new();
    TYPE_REGISTRY.get_or_init(|| RwLock::new(TypeRegistry::new()))
}

fn normalize_rust_type_name(raw: &str) -> String {
    raw.chars().filter(|c| !c.is_whitespace()).collect()
}

fn rust_type_key<T: 'static>() -> String {
    normalize_rust_type_name(type_name::<T>())
}

pub const BUILTIN_VALUE_TYPES: &[ValueType] = &[
    ValueType::Unit,
    ValueType::Bool,
    ValueType::I32,
    ValueType::U32,
    ValueType::Int,
    ValueType::F32,
    ValueType::Float,
    ValueType::String,
    ValueType::Bytes,
];

macro_rules! with_builtin_rust_scalar_types {
    ($macro:ident) => {
        $macro! {
            () => Unit,
            bool => Bool,
            i8 => Int,
            i16 => Int,
            i32 => Int,
            i64 => Int,
            i128 => Int,
            isize => Int,
            u8 => Int,
            u16 => Int,
            u32 => Int,
            u64 => Int,
            u128 => Int,
            usize => Int,
            f32 => Float,
            f64 => Float,
            String => String,
            Vec<u8> => Bytes,
        }
    };
}

impl TypeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `expr` as the type expression of `T`. Registering the same expression again is
    /// a no-op; a different one fails instead of replacing the first, so the result never
    /// depends on registration order.
    pub fn register_type<T: 'static>(&mut self, expr: TypeExpr) -> Result<(), TypeConflict> {
        let expr = expr.normalize();
        if let Some(existing) = self.by_type_id.get(&TypeId::of::<T>()) {
            return if existing.expr == expr {
                Ok(())
            } else {
                Err(TypeConflict {
                    rust: existing.rust.clone(),
                    existing: existing.expr.clone(),
                    new: expr,
                })
            };
        }
        let rust = rust_type_key::<T>();
        self.by_rust_name.insert(rust.clone(), expr.clone());
        self.by_type_id
            .insert(TypeId::of::<T>(), RegisteredType { rust, expr });
        Ok(())
    }

    pub fn register_enum<T: 'static>(
        &mut self,
        variants: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<(), TypeConflict> {
        let variants = variants
            .into_iter()
            .map(|name| EnumVariant {
                name: name.into(),
                ty: None,
            })
            .collect();
        self.register_type::<T>(TypeExpr::Enum(variants))
    }

    /// Every registered Rust type, by `TypeId`, with its type expression.
    pub fn registered_types(&self) -> impl Iterator<Item = (TypeId, &RegisteredType)> {
        self.by_type_id
            .iter()
            .map(|(id, registered)| (*id, registered))
    }

    pub fn lookup_type<T: 'static>(&self) -> Option<TypeExpr> {
        self.by_type_id
            .get(&TypeId::of::<T>())
            .map(|v| v.expr.clone())
    }

    pub fn lookup_type_by_rust_name(&self, raw: &str) -> Option<TypeExpr> {
        let key = normalize_rust_type_name(raw);
        self.by_rust_name.get(&key).cloned()
    }

    pub fn snapshot_by_rust_name(&self) -> Vec<RegisteredType> {
        let mut out: Vec<RegisteredType> = self
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

    pub fn register_type_capability(&mut self, ty: TypeExpr, capability: impl Into<String>) {
        self.register_type_capabilities(ty, [capability]);
    }

    pub fn register_type_capabilities(
        &mut self,
        ty: TypeExpr,
        capabilities: impl IntoIterator<Item = impl Into<String>>,
    ) {
        let ty = ty.normalize();
        let caps = self.by_capability_type.entry(ty).or_default();
        for capability in capabilities {
            let capability = capability.into();
            if !capability.trim().is_empty() {
                caps.insert(capability);
            }
        }
    }

    pub fn type_capabilities(&self, ty: &TypeExpr) -> BTreeSet<String> {
        let ty = ty.clone().normalize();
        self.by_capability_type
            .get(&ty)
            .cloned()
            .unwrap_or_default()
    }

    pub fn has_type_capability(&self, ty: &TypeExpr, capability: &str) -> bool {
        if capability.trim().is_empty() {
            return false;
        }
        self.type_capabilities(ty).contains(capability)
    }

    pub fn snapshot_type_capabilities(&self) -> Vec<RegisteredTypeCapabilities> {
        self.by_capability_type
            .iter()
            .map(|(ty, capabilities)| RegisteredTypeCapabilities {
                ty: ty.clone(),
                capabilities: capabilities.clone(),
            })
            .collect()
    }

    pub fn override_type_expr<T: 'static>(&self) -> Option<TypeExpr> {
        self.lookup_type::<T>().or_else(builtin_type_expr::<T>)
    }

    pub fn type_expr<T: 'static>(&self) -> TypeExpr {
        if let Some(expr) = self.override_type_expr::<T>() {
            return expr;
        }
        TypeExpr::Opaque(format!("rust:{}", rust_type_key::<T>()))
    }
}

trait BuiltinConstCoerce: Sized + Send + Sync + 'static {
    fn coerce_builtin(value: &Value) -> Option<Self>;
}

impl BuiltinConstCoerce for () {
    fn coerce_builtin(value: &Value) -> Option<Self> {
        matches!(value, Value::Unit).then_some(())
    }
}

impl BuiltinConstCoerce for bool {
    fn coerce_builtin(value: &Value) -> Option<Self> {
        match value {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }
}

macro_rules! impl_builtin_signed_const_coerce {
    ($($ty:ty),* $(,)?) => {
        $(
            impl BuiltinConstCoerce for $ty {
                fn coerce_builtin(value: &Value) -> Option<Self> {
                    match value {
                        Value::Int(v) => <$ty>::try_from(*v).ok(),
                        _ => None,
                    }
                }
            }
        )*
    };
}

macro_rules! impl_builtin_float_const_coerce {
    ($($ty:ty),* $(,)?) => {
        $(
            impl BuiltinConstCoerce for $ty {
                fn coerce_builtin(value: &Value) -> Option<Self> {
                    match value {
                        Value::Float(v) => Some(*v as $ty),
                        _ => None,
                    }
                }
            }
        )*
    };
}

impl_builtin_signed_const_coerce!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize
);
impl_builtin_float_const_coerce!(f32, f64);

impl BuiltinConstCoerce for String {
    fn coerce_builtin(value: &Value) -> Option<Self> {
        match value {
            Value::String(v) => Some(v.to_string()),
            _ => None,
        }
    }
}

impl BuiltinConstCoerce for Vec<u8> {
    fn coerce_builtin(value: &Value) -> Option<Self> {
        match value {
            Value::Bytes(v) => Some(v.to_vec()),
            _ => None,
        }
    }
}

fn coerce_builtin_as<T, U>(value: &Value) -> Option<T>
where
    T: Send + Sync + 'static,
    U: BuiltinConstCoerce + 'static,
{
    // `T` is `U` (the caller matched their `TypeId`s); the `Option` slot converts without boxing.
    let mut typed = Some(U::coerce_builtin(value)?);
    (&mut typed as &mut dyn Any)
        .downcast_mut::<Option<T>>()?
        .take()
}

/// Register a concrete `TypeExpr` for a Rust type `T`.
///
/// This global helper is convenience API for small applications and generic host
/// helpers. Plugin and engine-owned code should prefer an owned [`TypeRegistry`]
/// or `PluginRegistry::type_registry` so independent registries do not share type
/// state.
///
pub fn register_type<T: 'static>(expr: TypeExpr) -> Result<(), TypeConflict> {
    registry().write().register_type::<T>(expr)
}

pub fn register_type_capability(ty: TypeExpr, capability: impl Into<String>) {
    register_type_capabilities(ty, [capability]);
}

pub fn register_type_capabilities(
    ty: TypeExpr,
    capabilities: impl IntoIterator<Item = impl Into<String>>,
) {
    let mut guard = registry().write();
    guard.register_type_capabilities(ty, capabilities);
}

pub fn type_capabilities(ty: &TypeExpr) -> BTreeSet<String> {
    let guard = registry().read();
    guard.type_capabilities(ty)
}

pub fn has_type_capability(ty: &TypeExpr, capability: &str) -> bool {
    if capability.trim().is_empty() {
        return false;
    }
    type_capabilities(ty).contains(capability)
}

pub fn snapshot_type_capabilities() -> Vec<RegisteredTypeCapabilities> {
    let guard = registry().read();
    guard.snapshot_type_capabilities()
}

/// Snapshot the full process-global type registry.
///
/// This is intended for test isolation and embedders that need to temporarily install global
/// convenience registrations and restore the previous process state afterward. Code that already
/// owns an engine or plugin registry should prefer passing an owned [`TypeRegistry`] directly.
pub fn snapshot_global_registry() -> TypeRegistry {
    registry().read().clone()
}

/// Replace the process-global type registry with a previous snapshot.
pub fn restore_global_registry(snapshot: TypeRegistry) {
    *registry().write() = snapshot;
}

/// Reset the process-global type registry to an empty registry.
pub fn reset_global_registry() {
    restore_global_registry(TypeRegistry::new());
}

/// Register an enum (variants only) for Rust type `T`.
///
pub fn register_enum<T: 'static>(
    variants: impl IntoIterator<Item = impl Into<String>>,
) -> Result<(), TypeConflict> {
    registry().write().register_enum::<T>(variants)
}

/// Look up a previously registered `TypeExpr` for a Rust type `T`.
///
pub fn lookup_type<T: 'static>() -> Option<TypeExpr> {
    let guard = registry().read();
    guard.lookup_type::<T>()
}

/// The type expression of a builtin Rust type: the scalars (integers, floats, `bool`, `String`,
/// `Vec<u8>`, `()`) and `Option`/`Vec` of them, encoded as the node macros encode them. Depends
/// on no registry.
pub fn builtin_type_expr<T: 'static>() -> Option<TypeExpr> {
    builtin_type_exprs().get(&TypeId::of::<T>()).cloned()
}

/// The type expression of every builtin Rust type (see [`builtin_type_expr`]) by `TypeId`.
pub fn builtin_type_exprs() -> &'static HashMap<TypeId, TypeExpr> {
    static BUILTINS: OnceLock<HashMap<TypeId, TypeExpr>> = OnceLock::new();
    BUILTINS.get_or_init(|| {
        let mut builtins = HashMap::new();
        macro_rules! insert_builtins {
            ($($ty:ty => $value_type:ident),* $(,)?) => {
                $(
                    let scalar = TypeExpr::Scalar(ValueType::$value_type);
                    let optional = TypeExpr::Optional(Box::new(scalar.clone()));
                    let list = TypeExpr::List(Box::new(scalar.clone()));
                    builtins.entry(TypeId::of::<Option<$ty>>()).or_insert(optional);
                    builtins.entry(TypeId::of::<Vec<$ty>>()).or_insert(list);
                    // Scalars win over containers (`Vec<u8>` is `Bytes`, not a list of ints).
                    builtins.insert(TypeId::of::<$ty>(), scalar);
                )*
            };
        }
        with_builtin_rust_scalar_types!(insert_builtins);
        builtins
    })
}

pub fn coerce_builtin_const_value<T>(value: &Value) -> Option<T>
where
    T: Send + Sync + 'static,
{
    macro_rules! coerce_builtin {
        ($($ty:ty => $value_type:ident),* $(,)?) => {
            $(
                if TypeId::of::<T>() == TypeId::of::<$ty>() {
                    return coerce_builtin_as::<T, $ty>(value);
                }
            )*
        };
    }
    with_builtin_rust_scalar_types!(coerce_builtin);

    None
}

/// Return an explicit type expression if `T` has either been registered or is
/// covered by built-in mappings (without falling back to `Opaque`).
///
pub fn override_type_expr<T: 'static>() -> Option<TypeExpr> {
    let guard = registry().read();
    guard.override_type_expr::<T>()
}

/// Returns the best-effort `TypeExpr` for a Rust type `T`.
///
/// Resolution order:
/// 1) Global `register_type::<T>(...)` convenience registration
/// 2) Built-in primitives and common shims (e.g. `Vec<u8>` as `Bytes`)
/// 3) `Opaque("rust:<type_name>")` fallback
///
/// Prefer [`TypeRegistry::type_expr`] when code already owns a plugin or engine
/// registry.
///
pub fn type_expr<T: 'static>() -> TypeExpr {
    let guard = registry().read();
    guard.type_expr::<T>()
}

/// Look up a previously registered `TypeExpr` by Rust type name (whitespace is ignored).
///
/// This is stable across dylib/plugin boundaries where `TypeId` differs but `type_name::<T>()`
/// is identical (compiled from the same sources).
pub fn lookup_type_by_rust_name(raw: &str) -> Option<TypeExpr> {
    let guard = registry().read();
    guard.lookup_type_by_rust_name(raw)
}

/// Snapshot the current registry as a list keyed by Rust type name.
///
/// Intended for UIs and tooling (e.g. exposing enum/struct definitions registered by plugins).
pub fn snapshot_by_rust_name() -> Vec<RegisteredType> {
    let guard = registry().read();
    guard.snapshot_by_rust_name()
}

#[cfg(test)]
mod tests {
    use super::{
        TypeRegistry, coerce_builtin_const_value, has_type_capability, register_type_capabilities,
        snapshot_type_capabilities, type_capabilities,
    };
    use crate::model::{TypeExpr, Value, ValueType};

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
    fn snapshot_helpers_include_registered_capabilities() {
        let ty = TypeExpr::Opaque("test:snapshot:type".to_string());
        register_type_capabilities(ty.clone(), ["croppable"]);

        let capability_snapshot = snapshot_type_capabilities();
        assert!(
            capability_snapshot
                .iter()
                .any(|entry| { entry.ty == ty && entry.capabilities.contains("croppable") })
        );
    }

    #[test]
    fn owned_type_registries_do_not_leak_registered_types() {
        struct LocalType;

        let mut left = TypeRegistry::new();
        let right = TypeRegistry::new();
        left.register_type::<LocalType>(TypeExpr::Scalar(ValueType::Bool))
            .unwrap();

        assert_eq!(
            left.lookup_type::<LocalType>(),
            Some(TypeExpr::Scalar(ValueType::Bool))
        );
        assert_eq!(right.lookup_type::<LocalType>(), None);
    }

    #[test]
    fn owned_type_registries_do_not_leak_capabilities() {
        let ty = TypeExpr::Opaque("test:isolated:capability".to_string());
        let mut left = TypeRegistry::new();
        let right = TypeRegistry::new();

        left.register_type_capabilities(ty.clone(), ["left-only"]);

        assert!(left.has_type_capability(&ty, "left-only"));
        assert!(!right.has_type_capability(&ty, "left-only"));
        assert!(right.snapshot_type_capabilities().is_empty());
    }

    macro_rules! assert_builtin_int {
        ($($ty:ty),* $(,)?) => {
            $(
                assert_eq!(coerce_builtin_const_value::<$ty>(&Value::Int(7)), Some(7 as $ty));
            )*
        };
    }

    #[test]
    fn builtin_scalar_consts_coerce_through_shared_type_registry_path() {
        assert_eq!(coerce_builtin_const_value::<()>(&Value::Unit), Some(()));
        assert_eq!(
            coerce_builtin_const_value::<bool>(&Value::Bool(true)),
            Some(true)
        );
        assert_builtin_int!(
            i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize
        );
        assert_eq!(
            coerce_builtin_const_value::<f32>(&Value::Float(1.5)),
            Some(1.5)
        );
        assert_eq!(
            coerce_builtin_const_value::<f64>(&Value::Float(1.5)),
            Some(1.5)
        );
        assert_eq!(
            coerce_builtin_const_value::<String>(&Value::String("hello".into())),
            Some("hello".to_string())
        );
        assert_eq!(
            coerce_builtin_const_value::<Vec<u8>>(&Value::Bytes(vec![1, 2, 3].into())),
            Some(vec![1, 2, 3])
        );
        assert_eq!(coerce_builtin_const_value::<u8>(&Value::Int(-1)), None);
        assert_eq!(
            coerce_builtin_const_value::<i8>(&Value::Int(i64::from(i8::MAX) + 1)),
            None
        );
    }
}
