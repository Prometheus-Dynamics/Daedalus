//! Rust types behind transport keys: types that own a key (strict, one key per type and one
//! type per key), the fail-loud check for unkeyed foreign port types, the per-key
//! [`RustTypeIdentity`] table that dynamic plugin installation compares between host and plugin,
//! and the frozen [`TypeIndex`] graphs resolve generic pushes through.

use super::*;
use crate::type_index::{TypeIndex, TypeKeyUses};
use core::any::TypeId;
use core::fmt;
use daedalus_transport::RustTypeIdentity;

/// Crates whose types keep their `rust:` fallback key without an error.
const STD_CRATES: &[&str] = &["core", "alloc", "std"];

/// One type key registered for two different Rust types (two plugins, a plugin and the host,
/// or a dynamic plugin built separately from the host).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundaryTypeConflict {
    pub key: TypeKey,
    /// The Rust type the registry already maps `key` to.
    pub registered: RustTypeIdentity,
    /// The Rust type the rejected registration uses.
    pub new: RustTypeIdentity,
}

impl fmt::Display for BoundaryTypeConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}`: registered {}, new {}",
            self.key, self.registered, self.new
        )
    }
}

impl PluginRegistry {
    /// Record that payloads under `key` hold the Rust type `T`.
    ///
    /// Node macros, `#[adapt]`, [`Self::register_daedalus_type`] and
    /// [`Self::register_foreign_type`] call this; call it directly for a type the host wraps in
    /// payloads itself (`Payload::shared_with(key, Arc<T>, ..)`) without registering it otherwise,
    /// so dynamic plugins using the key are checked against it (see [`Self::boundary_types`]) and
    /// generic pushes of `T` resolve to `key` (see [`Self::type_index`]). Fails with
    /// [`PluginError::BoundaryTypeConflict`] when `key` is already recorded for another Rust
    /// type. Structural keys (`typeexpr:...`, e.g. `List(Int)`) name no single Rust type and
    /// only feed the type index.
    pub fn register_boundary_type<T: 'static>(
        &mut self,
        key: impl Into<TypeKey>,
    ) -> PluginResult<()> {
        self.ensure_open()?;
        let key = key.into();
        if !is_structural(&key) {
            self.record_boundary_identity(&key, RustTypeIdentity::of::<T>())?;
        }
        self.type_key_uses
            .entry(TypeId::of::<T>())
            .or_default()
            .insert(key);
        Ok(())
    }

    /// Record Rust types reported for keys by another build (a dynamic plugin's
    /// `boundary_types()`). Fails on the first key already recorded for another Rust type; use
    /// [`Self::boundary_type_conflicts`] first to report all of them.
    pub fn register_boundary_identities<'a>(
        &mut self,
        types: impl IntoIterator<Item = &'a (TypeKey, RustTypeIdentity)>,
    ) -> PluginResult<()> {
        self.ensure_open()?;
        types
            .into_iter()
            .try_for_each(|(key, identity)| self.record_boundary_identity(key, *identity))
    }

    /// Every entry of `types` whose key this registry maps to another Rust type.
    pub fn boundary_type_conflicts<'a>(
        &self,
        types: impl IntoIterator<Item = &'a (TypeKey, RustTypeIdentity)>,
    ) -> Vec<BoundaryTypeConflict> {
        types
            .into_iter()
            .filter_map(|(key, new)| {
                let registered = *self.boundary_types.get(key)?;
                (!registered.same_type(new)).then(|| BoundaryTypeConflict {
                    key: key.clone(),
                    registered,
                    new: *new,
                })
            })
            .collect()
    }

    fn record_boundary_identity(
        &mut self,
        key: &TypeKey,
        new: RustTypeIdentity,
    ) -> PluginResult<()> {
        if let Some(conflict) = self.boundary_type_conflicts(&[(key.clone(), new)]).pop() {
            return Err(PluginError::BoundaryTypeConflict(conflict));
        }
        self.boundary_types.entry(key.clone()).or_insert(new);
        Ok(())
    }

    /// The Rust type recorded for each transport key (see [`Self::register_boundary_type`]).
    pub fn boundary_types(&self) -> &BTreeMap<TypeKey, RustTypeIdentity> {
        &self.boundary_types
    }

    /// Freeze what this registry knows about Rust types into a [`TypeIndex`]: each type
    /// resolves to the key it owns (`#[type_key]`/`DaedalusTypeExpr`, a foreign-type mapping,
    /// [`Self::type_registry`]) or else to the single key ports and adapters use it under, and
    /// each key to the Rust type recorded for it. Graph builders and compiled graphs capture
    /// one, so lookups only depend on this registry, never on install order or on other
    /// registries in the process.
    pub fn type_index(&self) -> TypeIndex {
        let mut types: HashMap<TypeId, TypeKeyUses> = self
            .type_key_uses
            .iter()
            .map(|(id, used)| {
                let uses = TypeKeyUses {
                    owned: None,
                    used: used.clone(),
                };
                (*id, uses)
            })
            .collect();
        for (id, registered) in self.type_registry.registered_types() {
            types.entry(id).or_default().owned = Some(typeexpr_transport_key(&registered.expr));
        }
        TypeIndex::new(
            types,
            self.boundary_types
                .iter()
                .map(|(key, identity)| (key.clone(), *identity)),
        )
    }

    /// Give a Rust type defined in another crate, which declares no key itself, the transport
    /// key `key` (what `#[plugin(foreign_types(Type = "key"))]` generates).
    ///
    /// Prefer the owning crate's own Daedalus integration when it has one: never mint a second
    /// key for a type whose crate already owns one. Registers the type with this registry's
    /// typing registry (node macros installed afterwards resolve it there), a placeholder
    /// transport type declaration (the owner's declaration may replace it), and the boundary
    /// type. Fails when `T` already owns another key or `key` names another Rust type.
    pub fn register_foreign_type<T: 'static>(&mut self, key: &str) -> PluginResult<()> {
        self.register_owned_type::<T>(key, |registry| {
            registry.register_transport_type_decl(TypeKey::new(key), TypeExpr::opaque(key))
        })
    }

    /// Make `key` the key `T` owns, after `declare` registered the key's type declaration.
    ///
    /// Strict, so the result never depends on registration order: registering the same type
    /// under the same key again is a no-op, while another key for `T`
    /// ([`PluginError::TypeKeyedTwice`]), another Rust type for `key`
    /// ([`PluginError::BoundaryTypeConflict`]) or another declaration for `key`
    /// ([`PluginError::TypeDeclarationConflict`]) fails before anything is registered.
    pub(super) fn register_owned_type<T: 'static>(
        &mut self,
        key: &str,
        declare: impl FnOnce(&mut Self) -> PluginResult<()>,
    ) -> PluginResult<()> {
        self.ensure_open()?;
        let own = TypeExpr::opaque(key);
        let keyed_twice = |existing: TypeExpr| PluginError::TypeKeyedTwice {
            rust_type: core::any::type_name::<T>(),
            existing: typeexpr_transport_key(&existing),
            new: TypeKey::new(key),
        };
        if let Some(existing) = self.type_registry.lookup_type::<T>()
            && existing != own
        {
            return Err(keyed_twice(existing));
        }
        let identity = [(TypeKey::new(key), RustTypeIdentity::of::<T>())];
        if let Some(conflict) = self.boundary_type_conflicts(&identity).pop() {
            return Err(PluginError::BoundaryTypeConflict(conflict));
        }
        declare(self)?;
        self.type_registry
            .register_type::<T>(own)
            .map_err(|conflict| keyed_twice(conflict.existing))?;
        self.register_boundary_type::<T>(TypeKey::new(key))
    }

    /// Macro support: check and record the Rust type `T` used at a node port or adapter end.
    ///
    /// `key` is the transport key the macro resolved for `T`; `declared` says whether it came
    /// from the type itself (`DaedalusTypeExpr`) or an explicit `type_key`/`from`/`to`. An
    /// undeclared `rust:` fallback key for a type from another crate depends on whether some
    /// other code registered the type first, so it fails with
    /// [`PluginError::UnkeyedForeignType`].
    #[doc(hidden)]
    pub fn register_port_type<T: 'static>(
        &mut self,
        port: PortTypeUse<'_>,
        key: TypeKey,
        declared: bool,
    ) -> PluginResult<()> {
        let rust_type = core::any::type_name::<T>();
        if !declared && key.as_str().starts_with("rust:") && is_foreign(rust_type, port.defined_in)
        {
            if let Some(external) = &mut self.external_types {
                external.push(ExternalTypeRef {
                    owner: port.owner.to_string(),
                    port: port.port.to_string(),
                    rust_type,
                });
                return Ok(());
            }
            return Err(PluginError::UnkeyedForeignType {
                owner: port.owner.to_string(),
                port: port.port.to_string(),
                rust_type,
                key,
            });
        }
        self.register_boundary_type::<T>(key)
    }
}

/// A port type from another crate with no key in this registry, recorded instead of failing
/// with [`PluginError::UnkeyedForeignType`] while extracting a dynamic plugin's schema
/// ([`PluginRegistry::record_external_types`]): typically a type whose key a dependency plugin
/// that is not linked into the plugin maps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalTypeRef {
    /// Node or adapter id.
    pub owner: String,
    pub port: String,
    pub rust_type: &'static str,
}

impl PluginRegistry {
    /// Record unkeyed foreign port types ([`Self::external_types`]) instead of failing with
    /// [`PluginError::UnkeyedForeignType`]. For schema extraction only: such a port keeps its
    /// order-dependent `rust:` key, so installing for real still fails.
    pub fn record_external_types(&mut self) {
        self.external_types.get_or_insert_with(Vec::new);
    }

    /// Port types recorded by [`Self::record_external_types`].
    pub fn external_types(&self) -> &[ExternalTypeRef] {
        self.external_types.as_deref().unwrap_or_default()
    }
}

/// Where a macro uses a Rust type, for [`PluginRegistry::register_port_type`].
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct PortTypeUse<'a> {
    /// Node or adapter id, as reported in errors.
    pub owner: &'a str,
    pub port: &'a str,
    /// `module_path!()` of the declaring code.
    pub defined_in: &'a str,
}

/// Structural keys (`typeexpr:...`) describe a shape that many Rust types share.
fn is_structural(key: &TypeKey) -> bool {
    key.as_str().starts_with("typeexpr:")
}

/// Whether `rust_type` (a `type_name`) is a path into a crate other than the one `module_path`
/// belongs to and other than the standard library.
fn is_foreign(rust_type: &str, module_path: &str) -> bool {
    let crate_of = |path: &str| path.split("::").next().unwrap_or_default().to_string();
    let krate = crate_of(rust_type);
    let is_path = !krate.is_empty() && krate.chars().all(|c| c.is_alphanumeric() || c == '_');
    is_path && krate != crate_of(module_path) && !STD_CRATES.contains(&krate.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreign_means_another_non_std_crate() {
        assert!(is_foreign(
            "styx_core::frame::FrameLease",
            "my_plugin::nodes"
        ));
        assert!(!is_foreign("my_plugin::Frame", "my_plugin::nodes"));
        assert!(!is_foreign("my_plugin::Frame", "my_plugin"));
        assert!(!is_foreign("alloc::string::String", "my_plugin"));
        assert!(!is_foreign("(u8, u8)", "my_plugin"));
        assert!(!is_foreign("[u8; 4]", "my_plugin"));
    }

    #[test]
    fn schema_extraction_records_unkeyed_foreign_port_types() {
        type Foreign = daedalus_transport::Payload;
        let port = PortTypeUse {
            owner: "my_plugin:node",
            port: "frame",
            defined_in: "my_plugin::nodes",
        };
        let key = || TypeKey::new("rust:daedalus_transport::Payload");
        let mut registry = PluginRegistry::new();
        assert!(matches!(
            registry.register_port_type::<Foreign>(port, key(), false),
            Err(PluginError::UnkeyedForeignType { .. })
        ));
        registry.record_external_types();
        registry
            .register_port_type::<Foreign>(port, key(), false)
            .expect("recorded");
        assert_eq!(
            registry.external_types(),
            [ExternalTypeRef {
                owner: "my_plugin:node".into(),
                port: "frame".into(),
                rust_type: core::any::type_name::<Foreign>(),
            }]
        );
        assert!(!registry.boundary_types().contains_key(&key()));
    }
}
