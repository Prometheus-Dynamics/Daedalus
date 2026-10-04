//! Rust types behind transport keys: foreign-type keys, the fail-loud check for unkeyed
//! foreign port types, and the per-key [`RustTypeIdentity`] table that dynamic plugin
//! installation compares between host and plugin.

use super::*;
use daedalus_transport::RustTypeIdentity;

/// Crates whose types keep their `rust:` fallback key without an error.
const STD_CRATES: &[&str] = &["core", "alloc", "std"];

impl PluginRegistry {
    /// Record that payloads under `key` hold the Rust type `T`.
    ///
    /// Node macros, `#[adapt]`, [`Self::register_daedalus_type`] and
    /// [`Self::register_foreign_type`] call this; call it directly for a type the host wraps in
    /// payloads itself (`Payload::shared_with(key, Arc<T>, ..)`) without registering it otherwise,
    /// so dynamic plugins using the key are checked against it (see
    /// [`Self::boundary_types`]). Fails when `key` is already recorded for another Rust type.
    pub fn register_boundary_type<T: 'static>(
        &mut self,
        key: impl Into<TypeKey>,
    ) -> PluginResult<()> {
        self.ensure_open()?;
        let key = key.into();
        let new = RustTypeIdentity::of::<T>();
        match self.boundary_types.get(&key) {
            Some(existing) if existing.same_type(&new) => Ok(()),
            Some(existing) => Err(PluginError::BoundaryTypeConflict {
                key,
                existing: *existing,
                new,
            }),
            None => {
                self.boundary_types.insert(key, new);
                Ok(())
            }
        }
    }

    /// The Rust type recorded for each transport key (see [`Self::register_boundary_type`]).
    pub fn boundary_types(&self) -> &BTreeMap<TypeKey, RustTypeIdentity> {
        &self.boundary_types
    }

    /// Give a Rust type defined in another crate, which declares no key itself, the transport
    /// key `key` (what `#[plugin(foreign_types(Type = "key"))]` generates).
    ///
    /// Prefer the owning crate's own Daedalus integration when it has one: never mint a second
    /// key for a type whose crate already owns one. Registers the type with this registry's and
    /// the process-global typing registry (so node macros installed afterwards resolve it), a
    /// transport type declaration, and the boundary type.
    pub fn register_foreign_type<T: 'static>(&mut self, key: &str) -> PluginResult<()> {
        self.ensure_open()?;
        let expr = TypeExpr::opaque(key);
        self.type_registry.register_type::<T>(expr.clone());
        daedalus_data::typing::register_type::<T>(expr.clone());
        self.register_transport_type_decl(TypeKey::new(key), expr)?;
        self.register_boundary_type::<T>(TypeKey::new(key))
    }

    /// Macro support: check and record the Rust type `T` used at a node port or adapter end.
    ///
    /// `key` is the transport key the macro resolved for `T`; `declared` says whether it came
    /// from the type itself (`DaedalusTypeExpr`) or an explicit `type_key`/`from`/`to`. An
    /// undeclared `rust:` fallback key for a type from another crate depends on whether some
    /// other code registered the type first, so it fails with
    /// [`PluginError::UnkeyedForeignType`]. Structural keys (`typeexpr:...`) are skipped.
    #[doc(hidden)]
    pub fn register_port_type<T: 'static>(
        &mut self,
        port: PortTypeUse<'_>,
        key: TypeKey,
        declared: bool,
    ) -> PluginResult<()> {
        if key.as_str().starts_with("typeexpr:") {
            return Ok(());
        }
        let rust_type = std::any::type_name::<T>();
        if !declared && key.as_str().starts_with("rust:") && is_foreign(rust_type, port.defined_in)
        {
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
}
