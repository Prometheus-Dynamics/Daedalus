//! Plugin abstraction: a self-contained bundle that installs descriptors into the
//! registry and returns handlers that the runtime can execute.
use crate::capabilities::CapabilityRegistry;
use crate::convert;
use crate::handler_registry::HandlerRegistry;
use daedalus_data::model::TypeExpr;
use daedalus_registry::store::Registry;
use serde::de::DeserializeOwned;
use std::any::Any;
use std::collections::BTreeSet;

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
    pub type_compatibilities: BTreeSet<(TypeExpr, TypeExpr)>,
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
            type_compatibilities: BTreeSet::new(),
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
        crate::host_bridge::register_value_serializer::<T, F>(serializer);
    }

    /// Register an output mover to emit a typed output payload by value.
    pub fn register_output_mover<T, F>(&mut self, mover: F)
    where
        T: Any + Send + Sync + 'static,
        F: Fn(T) -> crate::executor::EdgePayload + Send + Sync + 'static,
    {
        crate::io::register_output_mover_in::<T, F>(&self.output_movers, mover);
    }

    /// Register a type-compatibility edge to support dynamic port polling.
    ///
    /// Dynamic plugins should use this API so compat data is stored in the host registry.
    pub fn register_type_compatibility(&mut self, from: TypeExpr, to: TypeExpr) {
        self.type_compatibilities.insert((from, to));
    }

    /// Apply any registered compatibility edges to the host typing registry.
    pub fn apply_type_compatibilities(&self) {
        for (from, to) in &self.type_compatibilities {
            daedalus_data::typing::register_compatibility(from.clone(), to.clone());
        }
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
                daedalus_data::model::TypeExpr::Enum(vars) => vars.get(idx as usize).map(|ev| ev.name.clone()),
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
