use crate::NodeError;
use crate::io::NodeIo;
use crate::prelude::*;
use daedalus_data::model::Value;

pub const ENV_NODE_PERF_COUNTERS: &str = "DAEDALUS_NODE_PERF_COUNTERS";
pub const ENV_NODE_CPU_TIME: &str = "DAEDALUS_NODE_CPU_TIME";
pub const ENV_RUNTIME_POOL_SIZE: &str = "DAEDALUS_RUNTIME_POOL_SIZE";

/// Policy for invalid configuration values.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigPolicy {
    Clamp,
    Error,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeDebugConfig {
    pub node_perf_counters: bool,
    pub node_cpu_time: bool,
    pub pool_size: Option<usize>,
}

impl RuntimeDebugConfig {
    /// Reads the `DAEDALUS_*` debug variables from the process environment (`std` only; use
    /// [`Self::from_lookup`] elsewhere).
    #[cfg(feature = "std")]
    pub fn from_env() -> Self {
        let (config, warnings) =
            Self::from_lookup_with_diagnostics(|name| std::env::var(name).ok());
        #[cfg(feature = "tracing")]
        for warning in warnings {
            crate::trace::warn!(
                target: "daedalus_runtime::config",
                env = warning.name,
                value = %warning.value,
                reason = warning.reason,
                "runtime debug env value ignored"
            );
        }
        #[cfg(not(feature = "tracing"))]
        drop(warnings);
        config
    }

    pub fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Self {
        Self::from_lookup_with_diagnostics(&mut get).0
    }

    pub fn from_lookup_with_diagnostics(
        mut get: impl FnMut(&str) -> Option<String>,
    ) -> (Self, Vec<RuntimeDebugConfigEnvWarning>) {
        let mut warnings = Vec::new();
        let config = Self {
            node_perf_counters: env_bool(&mut get, ENV_NODE_PERF_COUNTERS, &mut warnings),
            node_cpu_time: env_bool(&mut get, ENV_NODE_CPU_TIME, &mut warnings),
            pool_size: env_usize(&mut get, ENV_RUNTIME_POOL_SIZE, &mut warnings),
        };
        (config, warnings)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeDebugConfigEnvWarning {
    pub name: &'static str,
    pub value: String,
    pub reason: &'static str,
}

fn env_bool(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
    warnings: &mut Vec<RuntimeDebugConfigEnvWarning>,
) -> bool {
    let Some(value) = get(name) else {
        return false;
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => {
            warnings.push(RuntimeDebugConfigEnvWarning {
                name,
                value,
                reason: "expected boolean: 1/0, true/false, yes/no, or on/off",
            });
            false
        }
    }
}

fn env_usize(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &'static str,
    warnings: &mut Vec<RuntimeDebugConfigEnvWarning>,
) -> Option<usize> {
    let value = get(name)?;
    match value.trim().parse::<usize>() {
        Ok(parsed) if parsed > 0 => Some(parsed),
        Ok(_) => {
            warnings.push(RuntimeDebugConfigEnvWarning {
                name,
                value,
                reason: "expected positive integer greater than zero",
            });
            None
        }
        Err(_) => {
            warnings.push(RuntimeDebugConfigEnvWarning {
                name,
                value,
                reason: "expected positive integer",
            });
            None
        }
    }
}

/// Record of a configuration value change after sanitization.
///
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigChange {
    pub port: &'static str,
    pub previous: Value,
    pub next: Value,
    pub policy: ConfigPolicy,
}

/// Sanitized configuration plus any changes applied.
///
#[derive(Debug, Clone, PartialEq)]
pub struct Sanitized<T> {
    pub value: T,
    pub changes: Vec<ConfigChange>,
}

/// Error returned from config validation or sanitization.
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    pub port: Option<&'static str>,
    pub message: String,
}

impl ConfigError {
    /// Create a new config error with no port context.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            port: None,
            message: message.into(),
        }
    }

    /// Create a new config error scoped to a specific port.
    pub fn for_port(port: &'static str, message: impl Into<String>) -> Self {
        Self {
            port: Some(port),
            message: message.into(),
        }
    }
}

impl core::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if let Some(port) = self.port {
            write!(f, "{port}: {}", self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl core::error::Error for ConfigError {}

/// Trait implemented by config structs generated by `#[derive(NodeConfig)]`.
///
/// Generated handlers keep a node's decoded config in a [`crate::const_cache::ConfigCache`] and
/// hand out clones (or borrows, for `&Config` parameters) until a config input changes.
pub trait NodeConfig: Clone + Send + Sync + 'static {
    /// The config ports; field types without a key of their own resolve through `types` (the
    /// installing registry's typing registry).
    fn ports(
        types: &daedalus_data::typing::TypeRegistry,
    ) -> Vec<daedalus_registry::capability::PortDecl>;
    /// The config port names, in field order.
    fn port_names() -> &'static [&'static str];
    fn metadata() -> alloc::collections::BTreeMap<String, Value> {
        alloc::collections::BTreeMap::new()
    }
    fn from_io(io: &NodeIo) -> Result<Self, NodeError>;
    fn sanitize(self) -> Result<Sanitized<Self>, ConfigError>;
    fn validate(&self) -> Result<(), ConfigError>;
    /// Register const coercers for the field types (see [`crate::const_coerce`]); called when
    /// a node taking this config installs.
    fn register_const_coercers(_coercers: &crate::io::ConstCoercerMap) {}
}

/// Emit warnings for config changes applied by sanitization.
///
#[cfg_attr(not(feature = "tracing"), allow(unused_variables))]
pub fn log_config_changes(node_id: &str, changes: &[ConfigChange]) {
    for change in changes {
        crate::trace::warn!(
            target: "daedalus_runtime::config",
            node = node_id,
            port = change.port,
            policy = ?change.policy,
            previous = ?change.previous,
            next = ?change.next,
            "node config sanitized"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::collections::BTreeMap;

    #[test]
    fn runtime_debug_config_parses_known_env_keys() {
        let values = BTreeMap::from([
            (ENV_NODE_PERF_COUNTERS.to_string(), "true".to_string()),
            (ENV_NODE_CPU_TIME.to_string(), "1".to_string()),
            (ENV_RUNTIME_POOL_SIZE.to_string(), "8".to_string()),
        ]);

        let config = RuntimeDebugConfig::from_lookup(|name| values.get(name).cloned());

        assert!(config.node_perf_counters);
        assert!(config.node_cpu_time);
        assert_eq!(config.pool_size, Some(8));
    }

    #[test]
    fn runtime_debug_config_keeps_invalid_values_disabled() {
        let values = BTreeMap::from([
            (ENV_NODE_PERF_COUNTERS.to_string(), "no".to_string()),
            (ENV_NODE_CPU_TIME.to_string(), "definitely".to_string()),
            (ENV_RUNTIME_POOL_SIZE.to_string(), "0".to_string()),
        ]);

        let config = RuntimeDebugConfig::from_lookup(|name| values.get(name).cloned());

        assert!(!config.node_perf_counters);
        assert!(!config.node_cpu_time);
        assert_eq!(config.pool_size, None);
    }

    #[test]
    fn runtime_debug_config_reports_ignored_env_values() {
        let values = BTreeMap::from([
            (ENV_NODE_CPU_TIME.to_string(), "definitely".to_string()),
            (ENV_RUNTIME_POOL_SIZE.to_string(), "0".to_string()),
        ]);

        let (config, warnings) =
            RuntimeDebugConfig::from_lookup_with_diagnostics(|name| values.get(name).cloned());

        assert!(!config.node_cpu_time);
        assert_eq!(config.pool_size, None);
        assert_eq!(
            warnings,
            vec![
                RuntimeDebugConfigEnvWarning {
                    name: ENV_NODE_CPU_TIME,
                    value: "definitely".to_string(),
                    reason: "expected boolean: 1/0, true/false, yes/no, or on/off",
                },
                RuntimeDebugConfigEnvWarning {
                    name: ENV_RUNTIME_POOL_SIZE,
                    value: "0".to_string(),
                    reason: "expected positive integer greater than zero",
                },
            ]
        );
    }
}
