//! Rust type → transport key resolution for generic pushes and typed host ports, and the
//! per-key Rust type check for payloads fed into a graph.
//!
//! A [`TypeIndex`] is a frozen snapshot of one `PluginRegistry` (`PluginRegistry::type_index`),
//! captured by `PluginRegistry::graph_builder` and when a graph is compiled. Lookups therefore
//! never depend on what else the process registered or on plugin install order: a type resolves
//! if the registry the graph came from knows it, whenever its plugin was installed. Builtin
//! scalars and `Option`/`Vec` of them resolve in every index, including
//! [`TypeIndex::builtin`].

use crate::portable::Arc;
use crate::prelude::*;
use alloc::collections::{BTreeMap, BTreeSet};
use core::any::{TypeId, type_name};
use core::fmt;
use core::hash::{BuildHasherDefault, Hasher};

use crate::portable::OnceLock;

use daedalus_data::model::{TypeExpr, Value};
use daedalus_transport::{Payload, RustTypeIdentity, TypeKey, TypeKeyError};

use crate::io::ConstCoercerMap;
use crate::transport::{transport_key_typeexpr, typeexpr_transport_key};

/// A payload of the given key built from a `Value` (a payload codec's `from_value`; the same type
/// as `plugins::payload_codecs::CodecFromValueFn`).
pub(crate) type ValueFromFn = Arc<dyn Fn(TypeKey, &Value) -> Result<Payload, String> + Send + Sync>;

/// Frozen Rust type → [`TypeKey`] index of one registry (see the module docs). Cheap to clone.
#[derive(Clone)]
pub struct TypeIndex {
    inner: Arc<Inner>,
}

impl fmt::Debug for TypeIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypeIndex")
            .field("keys", &self.inner.keys.len())
            .field("rust_types", &self.inner.rust_types.len())
            .field("codecs", &self.inner.codecs.len())
            .finish()
    }
}

#[derive(Default)]
struct Inner {
    keys: HashMap<TypeId, Resolved, Fnv>,
    /// The Rust type registered per key; payloads under these keys are checked against it.
    rust_types: HashMap<TypeKey, RustTypeIdentity, Fnv>,
    /// The `Value` → payload conversion of a key with a payload codec ([`ValueRoute::Codec`]).
    codecs: HashMap<TypeKey, ValueFromFn, Fnv>,
    /// The registry's const coercers, shared with the nodes ([`ValueRoute::Coerce`]).
    const_coercers: Option<ConstCoercerMap>,
}

/// How a `Value` pushed under a key becomes a payload of that key (see
/// [`TypeIndex::value_route`]).
pub(crate) enum ValueRoute<'a> {
    /// The key's payload codec builds the payload.
    Codec(&'a ValueFromFn),
    /// The const coercer of the key's Rust type checks the value. The `Value` is the payload:
    /// node reads coerce it with the same coercer.
    Coerce {
        coercers: &'a ConstCoercerMap,
        rust_type: &'static str,
    },
    /// The key names no Rust type the registry knows (builtin or structural), or the `Value`
    /// itself, so the `Value` is the payload.
    Passthrough,
    /// The key is registered for a Rust type with neither a codec nor a const coercer.
    Unconvertible { rust_type: &'static str },
}

#[derive(Clone, Debug)]
enum Resolved {
    Key(TypeKey),
    Ambiguous(Vec<TypeKey>),
}

/// The keys one Rust type was registered or used under, for [`TypeIndex::new`].
#[derive(Clone, Debug, Default)]
pub struct TypeKeyUses {
    /// The key the type owns (`#[type_key]`, `DaedalusTypeExpr`, a foreign-type mapping).
    pub owned: Option<TypeKey>,
    /// Keys ports and adapters use the type under.
    pub used: BTreeSet<TypeKey>,
}

impl TypeIndex {
    /// Build an index: each type resolves to the key it owns, else to the single key it is used
    /// under (several are ambiguous). Builtins always resolve to their structural key.
    pub fn new(
        types: impl IntoIterator<Item = (TypeId, TypeKeyUses)>,
        rust_types: impl IntoIterator<Item = (TypeKey, RustTypeIdentity)>,
    ) -> Self {
        Self::new_with_values(types, rust_types, [], None)
    }

    /// [`Self::new`] plus the sources that convert a `Value` pushed under a key (see
    /// `HostBridgeHandle::push_value`): the payload codecs' `from_value` conversions and the
    /// registry's const coercers.
    pub fn new_with_values(
        types: impl IntoIterator<Item = (TypeId, TypeKeyUses)>,
        rust_types: impl IntoIterator<Item = (TypeKey, RustTypeIdentity)>,
        codecs: impl IntoIterator<Item = (TypeKey, ValueFromFn)>,
        const_coercers: Option<ConstCoercerMap>,
    ) -> Self {
        let mut keys: HashMap<TypeId, Resolved, Fnv> = types
            .into_iter()
            .filter_map(|(id, uses)| {
                let resolved = match uses.owned {
                    Some(key) => Resolved::Key(key),
                    None if uses.used.len() == 1 => Resolved::Key(uses.used.into_iter().next()?),
                    None => Resolved::Ambiguous(uses.used.into_iter().collect()),
                };
                Some((id, resolved))
            })
            .collect();
        keys.extend(builtin_keys().iter().map(|(id, key)| (*id, key.clone())));
        Self {
            inner: Arc::new(Inner {
                keys,
                rust_types: rust_types.into_iter().collect(),
                codecs: codecs.into_iter().collect(),
                const_coercers,
            }),
        }
    }

    /// How a `Value` pushed under `key` becomes a payload of `key`. A payload codec wins; then a
    /// key whose Rust type the registry does not know, or is `Value`, passes the `Value` through;
    /// then the registry's const coercer of the Rust type.
    pub(crate) fn value_route(&self, key: &TypeKey) -> ValueRoute<'_> {
        if let Some(from_value) = self.inner.codecs.get(key) {
            return ValueRoute::Codec(from_value);
        }
        let Some(rust) = self.inner.rust_types.get(key) else {
            return ValueRoute::Passthrough;
        };
        if rust.same_type(&RustTypeIdentity::of::<Value>()) {
            return ValueRoute::Passthrough;
        }
        match &self.inner.const_coercers {
            Some(coercers) if coercers.read().contains_key(rust.type_name) => ValueRoute::Coerce {
                coercers,
                rust_type: rust.type_name,
            },
            _ => ValueRoute::Unconvertible {
                rust_type: rust.type_name,
            },
        }
    }

    /// The index of an empty registry: builtins only. Shared, so it never allocates.
    pub fn builtin() -> &'static TypeIndex {
        static BUILTIN: OnceLock<TypeIndex> = OnceLock::new();
        BUILTIN.get_or_init(|| TypeIndex::new([], []))
    }

    /// The transport key of `T`.
    pub fn key_of<T: 'static>(&self) -> Result<TypeKey, TypeKeyError> {
        match self.inner.keys.get(&TypeId::of::<T>()) {
            Some(Resolved::Key(key)) => Ok(key.clone()),
            Some(Resolved::Ambiguous(keys)) => Err(TypeKeyError::Ambiguous {
                rust_type: type_name::<T>().to_string(),
                keys: keys.clone(),
            }),
            None => Err(TypeKeyError::Unkeyed {
                rust_type: type_name::<T>().to_string(),
            }),
        }
    }

    /// The type expression of `T` (what `GraphBuilder::input_typed` declares).
    pub fn type_expr_of<T: 'static>(&self) -> Result<TypeExpr, TypeKeyError> {
        self.key_of::<T>().map(|key| transport_key_typeexpr(&key))
    }

    /// The Rust type registered for `key`, if any.
    pub fn rust_type(&self, key: &TypeKey) -> Option<&RustTypeIdentity> {
        self.inner.rust_types.get(key)
    }

    /// Reject a payload whose key is registered for another Rust type than the one it holds.
    /// Payloads with unknown keys and payloads without a Rust value (bytes) pass.
    pub fn check_payload(&self, payload: &Payload) -> Result<(), TypeKeyError> {
        if self.inner.rust_types.is_empty() {
            return Ok(());
        }
        let Some(found) = payload.storage_rust_type_id() else {
            return Ok(());
        };
        match self.inner.rust_types.get(payload.type_key()) {
            Some(expected) if RustTypeIdentity::hash_type_id(found) != expected.type_id_hash => {
                Err(TypeKeyError::RustTypeMismatch {
                    type_key: payload.type_key().clone(),
                    found: payload.storage_rust_type_name().unwrap_or("?").to_string(),
                    expected: expected.type_name.to_string(),
                })
            }
            _ => Ok(()),
        }
    }
}

impl Default for TypeIndex {
    fn default() -> Self {
        Self::builtin().clone()
    }
}

/// The transport key of every builtin type, computed once.
fn builtin_keys() -> &'static BTreeMap<TypeId, Resolved> {
    static KEYS: OnceLock<BTreeMap<TypeId, Resolved>> = OnceLock::new();
    KEYS.get_or_init(|| {
        daedalus_data::typing::builtin_type_exprs()
            .iter()
            .map(|(id, expr)| (*id, Resolved::Key(typeexpr_transport_key(expr))))
            .collect()
    })
}

type Fnv = BuildHasherDefault<FnvHasher>;

/// FNV-1a: `TypeId`s are already hashes and keys are short, so SipHash would only add cost on
/// the feed path.
#[derive(Default)]
struct FnvHasher(Option<u64>);

impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.0.unwrap_or(0xcbf2_9ce4_8422_2325)
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = self.finish();
        for byte in bytes {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.0 = Some(hash);
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = Some((self.finish() ^ value).wrapping_mul(0x0000_0100_0000_01b3));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Frame;
    struct Other;

    fn uses(owned: Option<&str>, used: &[&str]) -> TypeKeyUses {
        TypeKeyUses {
            owned: owned.map(TypeKey::new),
            used: used.iter().copied().map(TypeKey::new).collect(),
        }
    }

    #[test]
    fn builtins_resolve_without_a_registry() {
        let index = TypeIndex::builtin();
        let int = typeexpr_transport_key(&TypeExpr::Scalar(daedalus_data::model::ValueType::Int));
        assert_eq!(index.key_of::<i64>().unwrap(), int);
        assert_eq!(
            index.type_expr_of::<Option<String>>().unwrap(),
            TypeExpr::Optional(Box::new(TypeExpr::Scalar(
                daedalus_data::model::ValueType::String
            )))
        );
        let err = index.key_of::<Frame>().unwrap_err();
        assert!(
            matches!(err, TypeKeyError::Unkeyed { ref rust_type } if rust_type.ends_with("Frame"))
        );
        assert!(err.to_string().contains("foreign_types"), "{err}");
    }

    #[test]
    fn owned_keys_win_and_several_used_keys_are_ambiguous() {
        let index = TypeIndex::new(
            [
                (
                    TypeId::of::<Frame>(),
                    uses(Some("t:frame"), &["t:a", "t:b"]),
                ),
                (TypeId::of::<Other>(), uses(None, &["t:a", "t:b"])),
                (TypeId::of::<i64>(), uses(None, &["t:custom"])),
            ],
            [],
        );
        assert_eq!(index.key_of::<Frame>().unwrap(), TypeKey::new("t:frame"));
        assert!(matches!(
            index.key_of::<Other>(),
            Err(TypeKeyError::Ambiguous { ref keys, .. }) if keys.len() == 2
        ));
        assert_eq!(
            index.key_of::<i64>().unwrap(),
            TypeIndex::builtin().key_of::<i64>().unwrap()
        );
    }

    #[test]
    fn payloads_are_checked_against_the_registered_rust_type() {
        let key = TypeKey::new("t:frame");
        let index = TypeIndex::new([], [(key.clone(), RustTypeIdentity::of::<u32>())]);
        assert!(
            index
                .check_payload(&Payload::owned(key.clone(), 1_u32))
                .is_ok()
        );
        assert!(
            index
                .check_payload(&Payload::owned("t:other", 1_u64))
                .is_ok()
        );
        assert!(
            index
                .check_payload(&Payload::bytes_with_type_key(
                    key.clone(),
                    Arc::from(&[1_u8][..])
                ))
                .is_ok()
        );
        let err = index
            .check_payload(&Payload::owned(key, 1_u64))
            .unwrap_err();
        assert_eq!(
            err.to_string().split(" (built").next(),
            Some("payload for `t:frame` holds `u64` but this graph expects `u32`")
        );
    }
}
