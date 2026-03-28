//! Plugin abstraction: a self-contained bundle that installs descriptors into the
//! registry and returns handlers that the runtime can execute.
use crate::capabilities::CapabilityRegistry;
use crate::convert;
use crate::handler_registry::HandlerRegistry;
use daedalus_data::daedalus_type::DaedalusTypeExpr;
use daedalus_data::model::TypeExpr;
use daedalus_data::named_types::HostExportPolicy;
use daedalus_data::to_value::ToValue;
use daedalus_data::typing::{CompatibilityRule, RegisteredTypeCapabilities, TypeCompatibilityEdge};
use daedalus_registry::store::Registry;
use serde::de::DeserializeOwned;
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};

/// A plugin is the unit of composition for node bundles.
pub trait Plugin {
    /// Stable identifier for the plugin (e.g., bundle name).
    fn id(&self) -> &'static str;

    /// Install node descriptors and handlers into a plugin registry.
    fn install(&self, registry: &mut PluginRegistry) -> Result<(), &'static str>;
}

/// Extension trait so callers can say `registry.install_plugin(&plugin)` rather
/// than invoking the plugin directly.
pub trait RegistryPluginExt {
    fn install_plugin<P: Plugin>(&mut self, plugin: &P) -> Result<(), &'static str>;
}

/// Register a set of `DaedalusTypeExpr + ToValue` types as host-exportable values.
///
/// This is a convenience macro to reduce boilerplate in plugin `install()` functions.
///
/// ```ignore
/// register_daedalus_values!(registry, MyType, OtherType)?;
/// ```
#[macro_export]
macro_rules! register_daedalus_values {
    ($registry:expr, $( $ty:ty ),+ $(,)?) => {{
        $( $registry.register_daedalus_value::<$ty>()?; )+
        Ok::<(), &'static str>(())
    }};
}

/// Register a set of `DaedalusTypeExpr` types as named schemas (no `ToValue` required).
///
/// Useful for non-host-serialized types (e.g. large binary payloads) where you still want a
/// stable `TypeExpr::Opaque(<key>)` identity for UI typing and graph validation.
///
/// ```ignore
/// use daedalus_data::named_types::HostExportPolicy;
/// register_daedalus_types!(registry, HostExportPolicy::None, MyOpaqueType)?;
/// ```
#[macro_export]
macro_rules! register_daedalus_types {
    ($registry:expr, $export:expr, $( $ty:ty ),+ $(,)?) => {{
        $( $registry.register_daedalus_type::<$ty>($export)?; )+
        Ok::<(), &'static str>(())
    }};
}

/// Register `ToValue` serializers for container/derived types that do not have stable type keys.
///
/// ```ignore
/// register_to_value_serializers!(registry, Vec<MyType>, Arc<Vec<MyType>>);
/// ```
#[macro_export]
macro_rules! register_to_value_serializers {
    ($registry:expr, $( $ty:ty ),+ $(,)?) => {{
        $( $registry.register_to_value_serializer::<$ty>(); )+
    }};
}

impl RegistryPluginExt for PluginRegistry {
    fn install_plugin<P: Plugin>(&mut self, plugin: &P) -> Result<(), &'static str> {
        let prev = self.current_prefix.take();
        let combined_prefix = if let Some(parent) = &prev {
            crate::apply_node_prefix(parent, plugin.id())
        } else {
            plugin.id().to_string()
        };
        self.current_prefix = Some(combined_prefix);
        let res = plugin.install(self);
        self.current_prefix = prev;
        res
    }
}

/// Install a set of plugins, accumulating handlers. Stops at the first error.
pub fn install_all<P: Plugin>(
    registry: &mut PluginRegistry,
    plugins: impl IntoIterator<Item = P>,
) -> Result<HandlerRegistry, &'static str> {
    for plugin in plugins {
        registry.install_plugin(&plugin)?;
    }
    let mut handlers = HandlerRegistry::new();
    handlers.merge(std::mem::take(&mut registry.handlers));
    Ok(handlers)
}

/// Container for descriptors + handlers. All nodes are installed via plugins.
pub struct PluginRegistry {
    pub registry: Registry,
    pub handlers: HandlerRegistry,
    pub current_prefix: Option<String>,
    pub capabilities: CapabilityRegistry,
    pub const_coercers: crate::io::ConstCoercerMap,
    pub output_movers: crate::io::OutputMoverMap,
    pub value_serializers: crate::host_bridge::ValueSerializerMap,
    pub type_compatibilities: BTreeMap<(TypeExpr, TypeExpr), CompatibilityRule>,
    pub type_capabilities: BTreeMap<TypeExpr, BTreeSet<String>>,
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            registry: Registry::new(),
            handlers: HandlerRegistry::new(),
            current_prefix: None,
            capabilities: CapabilityRegistry::new(),
            const_coercers: crate::io::new_const_coercer_map(),
            output_movers: crate::io::new_output_mover_map(),
            value_serializers: crate::host_bridge::value_serializer_map(),
            type_compatibilities: BTreeMap::new(),
            type_capabilities: BTreeMap::new(),
        }
    }

    pub fn merge<N: NodeInstall>(&mut self) -> Result<(), &'static str> {
        N::register(self)
    }

    pub fn take_handlers(&mut self) -> HandlerRegistry {
        std::mem::take(&mut self.handlers)
    }

    /// Register an application-specific CPU-side conversion (available to all nodes).
    pub fn register_conversion<S, T>(&mut self, f: fn(&S) -> Option<T>)
    where
        S: 'static + Send + Sync + std::any::Any,
        T: 'static + Send + Sync + std::any::Any,
    {
        convert::register_conversion(f);
        // Also register a schema-level compatibility edge so the planner/typed host polling can
        // treat these types as coercible without needing separate manual wiring.
        let from = daedalus_data::typing::type_expr::<S>();
        let to = daedalus_data::typing::type_expr::<T>();
        self.register_type_compatibility(from, to);
    }

    /// Register a named schema keyed by a stable `TypeExpr::Opaque(<key>)` string.
    pub fn register_named_type(
        &mut self,
        key: impl Into<String>,
        expr: TypeExpr,
        export: HostExportPolicy,
    ) -> Result<(), &'static str> {
        daedalus_data::named_types::register_named_type(key, expr, export)
            .map_err(|_| "named type register failed")
    }

    /// Register a stable, Daedalus-facing schema identity for a Rust type.
    ///
    /// This links the Rust runtime type `T` to `TypeExpr::Opaque(T::TYPE_KEY)` for port typing,
    /// and registers the richer schema (`T::type_expr()`) for UI/tooling.
    pub fn register_daedalus_type<T: DaedalusTypeExpr>(
        &mut self,
        export: HostExportPolicy,
    ) -> Result<(), &'static str> {
        daedalus_data::typing::register_type::<T>(TypeExpr::opaque(T::TYPE_KEY));
        self.register_named_type(T::TYPE_KEY, T::type_expr(), export)
    }

    /// Register a stable schema identity *and* a `ToValue` serializer for host-visible transport.
    pub fn register_daedalus_value<T>(&mut self) -> Result<(), &'static str>
    where
        T: DaedalusTypeExpr + ToValue + Clone + Send + Sync + 'static,
    {
        self.register_daedalus_type::<T>(HostExportPolicy::Value)?;
        self.register_value_serializer::<T, _>(|v| v.to_value());
        Ok(())
    }

    /// Register a host-bridge value serializer for `T` using `ToValue`.
    ///
    /// Useful for container types like `Vec<T>` where you don't want a separate named type key.
    pub fn register_to_value_serializer<T>(&mut self)
    where
        T: ToValue + Clone + Send + Sync + 'static,
    {
        self.register_value_serializer::<T, _>(|v| v.to_value());
    }

    /// Register a conversion for constant default values.
    ///
    /// This is the preferred API for dynamic plugins so the host and plugin share a single
    /// coercer map stored in the host-owned `PluginRegistry`.
    pub fn register_const_coercer<T, F>(&mut self, coercer: F)
    where
        T: Any + Send + Sync + 'static,
        F: Fn(&daedalus_data::model::Value) -> Option<T> + Send + Sync + 'static,
    {
        let key = std::any::type_name::<T>();
        let mut guard = self
            .const_coercers
            .write()
            .expect("PluginRegistry.const_coercers lock poisoned");
        guard.insert(
            key,
            Box::new(move |v| coercer(v).map(|t| Box::new(t) as Box<dyn Any + Send + Sync>)),
        );
    }

    /// Register a serializer for outbound host-bridge values.
    ///
    /// This enables host-bridge serialization for plugin-defined structured payload types by
    /// converting them into `daedalus_data::model::Value`.
    pub fn register_value_serializer<T, F>(&mut self, serializer: F)
    where
        T: Any + Clone + Send + Sync + 'static,
        F: Fn(&T) -> daedalus_data::model::Value + Send + Sync + 'static,
    {
        crate::host_bridge::register_value_serializer_in::<T, F>(
            &self.value_serializers,
            serializer,
        );
    }

    /// Register an output mover to emit a typed output payload by value.
    pub fn register_output_mover<T, F>(&mut self, mover: F)
    where
        T: Any + Send + Sync + 'static,
        F: Fn(T) -> crate::executor::RuntimeValue + Send + Sync + 'static,
    {
        crate::io::register_output_mover_in::<T, F>(&self.output_movers, mover);
    }

    /// Register a type-compatibility edge to support dynamic port polling.
    ///
    /// Dynamic plugins should use this API so compat data is stored in the host registry.
    pub fn register_type_compatibility(&mut self, from: TypeExpr, to: TypeExpr) {
        self.register_type_compatibility_with_rule(from, to, CompatibilityRule::default());
    }

    /// Register a type-compatibility edge with semantic metadata for planner/runtime resolution.
    pub fn register_type_compatibility_with_rule(
        &mut self,
        from: TypeExpr,
        to: TypeExpr,
        rule: CompatibilityRule,
    ) {
        let from = from.normalize();
        let to = to.normalize();
        self.type_compatibilities
            .insert((from.clone(), to.clone()), rule.clone());
        daedalus_data::typing::register_compatibility_with_rule(from, to, rule);
    }

    /// Register a semantic capability for a Daedalus-facing type.
    pub fn register_type_capability(&mut self, ty: TypeExpr, capability: impl Into<String>) {
        self.register_type_capabilities(ty, [capability]);
    }

    /// Register semantic capabilities for a Daedalus-facing type.
    pub fn register_type_capabilities(
        &mut self,
        ty: TypeExpr,
        capabilities: impl IntoIterator<Item = impl Into<String>>,
    ) {
        let ty = ty.normalize();
        let entry = self.type_capabilities.entry(ty.clone()).or_default();
        let mut normalized = Vec::new();
        for capability in capabilities {
            let capability = capability.into();
            if !capability.trim().is_empty() && entry.insert(capability.clone()) {
                normalized.push(capability);
            }
        }
        if !normalized.is_empty() {
            daedalus_data::typing::register_type_capabilities(ty, normalized);
        }
    }

    /// Apply any registered compatibility edges to the host typing registry.
    pub fn apply_type_compatibilities(&self) {
        for ((from, to), rule) in &self.type_compatibilities {
            daedalus_data::typing::register_compatibility_with_rule(
                from.clone(),
                to.clone(),
                rule.clone(),
            );
        }
        for (ty, capabilities) in &self.type_capabilities {
            daedalus_data::typing::register_type_capabilities(ty.clone(), capabilities.clone());
        }
    }

    pub fn snapshot_type_compatibilities(&self) -> Vec<TypeCompatibilityEdge> {
        self.type_compatibilities
            .iter()
            .map(|((from, to), rule)| TypeCompatibilityEdge {
                from: from.clone(),
                to: to.clone(),
                rule: rule.clone(),
            })
            .collect()
    }

    pub fn snapshot_type_capabilities(&self) -> Vec<RegisteredTypeCapabilities> {
        self.type_capabilities
            .iter()
            .map(|(ty, capabilities)| RegisteredTypeCapabilities {
                ty: ty.clone(),
                capabilities: capabilities.clone(),
            })
            .collect()
    }

    /// Register an enum type for UI/typing and enable constant binding for it.
    ///
    /// This lets node function signatures take a strongly-typed enum (e.g. `mode: ExecMode`)
    /// while allowing JSON-authored graphs to provide the value as either:
    /// - `Value::Int(2)` (index into the registered variant list)
    /// - `Value::String("cpu")` (variant name)
    /// - `Value::Enum { name: "cpu", .. }` (variant name)
    ///
    /// The enum `T` must be `DeserializeOwned` so we can construct it from the variant name.
    pub fn register_enum<T>(&mut self, variants: impl IntoIterator<Item = impl Into<String>>)
    where
        T: Any + Send + Sync + 'static + DeserializeOwned,
    {
        daedalus_data::typing::register_enum::<T>(variants);

        fn resolve_enum_name<T: Any>(raw: &str) -> Option<String> {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return None;
            }
            daedalus_data::typing::lookup_type::<T>().and_then(|te| match te {
                daedalus_data::model::TypeExpr::Enum(vars) => vars
                    .iter()
                    .find(|ev| ev.name.eq_ignore_ascii_case(trimmed))
                    .map(|ev| ev.name.clone()),
                _ => None,
            })
        }

        fn resolve_enum_name_from_index<T: Any>(idx: i64) -> Option<String> {
            if idx < 0 {
                return None;
            }
            daedalus_data::typing::lookup_type::<T>().and_then(|te| match te {
                daedalus_data::model::TypeExpr::Enum(vars) => {
                    vars.get(idx as usize).map(|ev| ev.name.clone())
                }
                _ => None,
            })
        }

        self.register_const_coercer::<T, _>(|v| {
            let name = match v {
                daedalus_data::model::Value::Int(i) => resolve_enum_name_from_index::<T>(*i),
                daedalus_data::model::Value::String(s) => resolve_enum_name::<T>(s.as_ref()),
                daedalus_data::model::Value::Enum(ev) => resolve_enum_name::<T>(&ev.name),
                _ => None,
            }?;
            serde_json::from_value::<T>(serde_json::Value::String(name)).ok()
        });

        // Optional enum inputs are common in node signatures (e.g. `mode: Option<ExecMode>`).
        // Register a dedicated coercer so const/default values can bind directly without each
        // plugin having to duplicate Option<T> registration glue.
        self.register_const_coercer::<Option<T>, _>(|v| {
            let name = match v {
                daedalus_data::model::Value::Int(i) => resolve_enum_name_from_index::<T>(*i),
                daedalus_data::model::Value::String(s) => resolve_enum_name::<T>(s.as_ref()),
                daedalus_data::model::Value::Enum(ev) => resolve_enum_name::<T>(&ev.name),
                _ => None,
            }?;
            serde_json::from_value::<T>(serde_json::Value::String(name))
                .ok()
                .map(Some)
        });
    }

    /// Register a typed capability entry keyed by a string. The provided function operates
    /// on typed references; downcasting is handled internally.
    pub fn register_capability_typed<T, F>(&mut self, key: impl Into<String>, f: F)
    where
        T: Send + Sync + 'static,
        F: Fn(&T, &T) -> Result<T, crate::executor::NodeError> + Send + Sync + 'static,
    {
        let key_str = key.into();
        if let Ok(mut global) = crate::capabilities::global().write() {
            global.register_typed::<T, F>(key_str.clone(), f);
        }
    }

    /// Register a typed capability entry that takes three operands of the same type.
    pub fn register_capability_typed3<T, F>(&mut self, key: impl Into<String>, f: F)
    where
        T: Send + Sync + 'static,
        F: Fn(&T, &T, &T) -> Result<T, crate::executor::NodeError> + Send + Sync + 'static,
    {
        let key_str = key.into();
        if let Ok(mut global) = crate::capabilities::global().write() {
            global.register_typed3::<T, F>(key_str.clone(), f);
        }
    }
}

pub trait NodeInstall {
    fn register(into: &mut PluginRegistry) -> Result<(), &'static str>;
}

#[cfg(test)]
mod tests {
    use super::PluginRegistry;
    use daedalus_data::model::TypeExpr;
    use daedalus_data::typing::{
        CompatibilityKind, CompatibilityRule, compatibility_rule, has_type_capability,
    };

    #[test]
    fn plugin_registry_registers_semantic_compatibility_rules() {
        let mut registry = PluginRegistry::new();
        let from = TypeExpr::Opaque("test:plugin:compat:from".to_string());
        let to = TypeExpr::Opaque("test:plugin:compat:to".to_string());

        registry.register_type_compatibility_with_rule(
            from.clone(),
            to.clone(),
            CompatibilityRule {
                kind: CompatibilityKind::View,
                cost: 0,
                capabilities: ["view-compatible".to_string()].into_iter().collect(),
            },
        );

        let rule = compatibility_rule(&from, &to).expect("compatibility rule should be present");
        assert_eq!(rule.kind, CompatibilityKind::View);
        assert_eq!(rule.cost, 0);
        assert!(rule.capabilities.contains("view-compatible"));
    }

    #[test]
    fn plugin_registry_registers_type_capabilities() {
        let mut registry = PluginRegistry::new();
        let ty = TypeExpr::Opaque("test:plugin:capabilities".to_string());

        registry.register_type_capabilities(ty.clone(), ["croppable", "luma-readable"]);

        assert!(has_type_capability(&ty, "croppable"));
        assert!(has_type_capability(&ty, "luma-readable"));
    }

    #[test]
    fn plugin_registry_snapshot_helpers_include_registered_semantics() {
        let mut registry = PluginRegistry::new();
        let from = TypeExpr::Opaque("test:plugin:snapshot:from".to_string());
        let to = TypeExpr::Opaque("test:plugin:snapshot:to".to_string());

        registry.register_type_compatibility_with_rule(
            from.clone(),
            to.clone(),
            CompatibilityRule {
                kind: CompatibilityKind::Materialize,
                cost: 3,
                capabilities: ["cpu-materializable".to_string()].into_iter().collect(),
            },
        );
        registry.register_type_capabilities(from.clone(), ["croppable"]);

        let compatibilities = registry.snapshot_type_compatibilities();
        assert!(compatibilities.iter().any(|edge| {
            edge.from == from
                && edge.to == to
                && edge.rule.kind == CompatibilityKind::Materialize
                && edge.rule.cost == 3
                && edge.rule.capabilities.contains("cpu-materializable")
        }));

        let capabilities = registry.snapshot_type_capabilities();
        assert!(
            capabilities
                .iter()
                .any(|entry| { entry.ty == from && entry.capabilities.contains("croppable") })
        );
    }
}
