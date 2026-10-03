use daedalus_registry::capability::NodeDecl;
use daedalus_runtime::handles::{NodeHandle, PortHandle};
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HostBridgeManager, bridge_handler};
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

    let decl = daedalus_planner::host_bridge_metadata()
        .into_iter()
        .fold(NodeDecl::new(&qualified_id), |decl, (key, value)| {
            decl.metadata(key, value)
        });
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
pub fn host_port(alias: impl Into<String>, port: impl Into<String>) -> PortHandle {
    PortHandle::new(alias, port)
}
