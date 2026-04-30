use daedalus_core::metadata::{DYNAMIC_INPUTS_KEY, DYNAMIC_OUTPUTS_KEY};
use daedalus_data::model::Value;
use daedalus_registry::capability::NodeDecl;
use daedalus_runtime::handles::{NodeHandle, PortHandle};
use daedalus_runtime::host_bridge::{
    HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY, HostBridgeManager, bridge_handler,
};
use daedalus_runtime::plugins::{PluginError, PluginRegistry};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostBridgeInstallError {
    message: String,
}

impl HostBridgeInstallError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for HostBridgeInstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for HostBridgeInstallError {}

impl From<&'static str> for HostBridgeInstallError {
    fn from(message: &'static str) -> Self {
        Self::new(message)
    }
}

impl From<PluginError> for HostBridgeInstallError {
    fn from(error: PluginError) -> Self {
        Self::new(error.to_string())
    }
}

/// Register the host-bridge node declaration and handler.
///
/// The handler is wired to the provided manager so host code can push/pop payloads.
///
/// ```no_run
/// use daedalus::host_bridge::install_host_bridge;
/// use daedalus::runtime::host_bridge::HostBridgeManager;
/// use daedalus::runtime::plugins::PluginRegistry;
///
/// let mut registry = PluginRegistry::default();
/// let manager = HostBridgeManager::new();
/// let handle = install_host_bridge(&mut registry, manager).expect("host bridge");
/// assert!(!handle.id().is_empty());
/// ```
pub fn install_host_bridge(
    registry: &mut PluginRegistry,
    manager: HostBridgeManager,
) -> Result<NodeHandle, HostBridgeInstallError> {
    let prefix = registry.current_prefix.clone();
    let qualified_id = if let Some(pref) = prefix {
        format!("{pref}:{HOST_BRIDGE_ID}")
    } else {
        HOST_BRIDGE_ID.to_string()
    };

    let decl = NodeDecl::new(&qualified_id)
        .metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
        // Allow arbitrary host ports without schema; the planner treats
        // `Opaque("generic")` as a type variable and infers concrete types from graph edges.
        .metadata(
            DYNAMIC_INPUTS_KEY,
            Value::String(::std::borrow::Cow::from("generic")),
        )
        .metadata(
            DYNAMIC_OUTPUTS_KEY,
            Value::String(::std::borrow::Cow::from("generic")),
        );
    registry.register_node_decl(decl)?;

    let mut handler = bridge_handler(manager);
    registry
        .handlers
        .on_stateful(&qualified_id, move |node, ctx, io| handler(node, ctx, io));

    Ok(NodeHandle::new(qualified_id))
}

pub fn install_default_host_bridge(
    registry: &mut PluginRegistry,
) -> Result<HostBridgeManager, HostBridgeInstallError> {
    let manager = HostBridgeManager::new();
    install_host_bridge(registry, manager.clone())?;
    Ok(manager)
}

/// Build a host bridge port handle for convenience.
///
/// ```
/// use daedalus::host_bridge::host_port;
///
/// let port = host_port("bridge", "in");
/// assert_eq!(port.node_alias(), "bridge");
/// assert_eq!(port.port(), "in");
/// ```
pub fn host_port(alias: impl Into<String>, port: impl Into<String>) -> PortHandle {
    PortHandle::new(alias, port)
}
