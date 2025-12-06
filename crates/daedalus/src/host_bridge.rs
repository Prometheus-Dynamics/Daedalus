use daedalus_data::model::Value;
use daedalus_registry::store::NodeDescriptorBuilder;
use daedalus_runtime::handles::{NodeHandle, PortHandle};
use daedalus_runtime::host_bridge::{
    HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY, HostBridgeManager, bridge_handler,
};
use daedalus_runtime::plugins::PluginRegistry;

/// Register the host-bridge node descriptor and handler.
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
/// assert!(!handle.id.is_empty());
/// ```
pub fn install_host_bridge(
    registry: &mut PluginRegistry,
    manager: HostBridgeManager,
) -> Result<NodeHandle, &'static str> {
    let prefix = registry.current_prefix.clone();
    let qualified_id = if let Some(pref) = prefix {
        format!("{pref}:{HOST_BRIDGE_ID}")
    } else {
        HOST_BRIDGE_ID.to_string()
    };

    let desc = NodeDescriptorBuilder::new(&qualified_id)
        .metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
        // Allow arbitrary host ports without schema in the registry; the planner treats
        // `Opaque("generic")` as a type variable and infers concrete types from graph edges.
        .metadata(
            "dynamic_inputs",
            Value::String(::std::borrow::Cow::from("generic")),
        )
        .metadata(
            "dynamic_outputs",
            Value::String(::std::borrow::Cow::from("generic")),
        )
        .build()
        .map_err(|_| "host bridge descriptor build failed")?;
    registry
        .registry
        .register_node(desc)
        .map_err(|_| "host bridge descriptor register failed")?;

    let mut handler = bridge_handler(manager);
    registry
        .handlers
        .on_stateful(&qualified_id, move |node, ctx, io| handler(node, ctx, io));

    Ok(NodeHandle::new(qualified_id))
}

/// Build a host bridge port handle for convenience.
///
/// ```
/// use daedalus::host_bridge::host_port;
///
/// let port = host_port("bridge", "in");
/// assert_eq!(port.node_alias, "bridge");
/// assert_eq!(port.port, "in");
/// ```
pub fn host_port(alias: impl Into<String>, port: impl Into<String>) -> PortHandle {
    PortHandle::new(alias, port)
}
