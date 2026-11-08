//! A plugin that exports a node group: `groups_example:pipe` is `double(add_one(x))`, placed on a
//! graph as one node and expanded into its two stages at plan time. The group is registered in
//! the plugin's install, so a host that loads this plugin (statically or as a dylib) gets the
//! group in its registry.

use daedalus::registry::capability::PluginManifest;
use daedalus::runtime::plugins::{
    NodeGroup, Plugin, PluginInstallContext, PluginResult, StagePort,
};
use daedalus::{macros::node, runtime::NodeError};

/// Canonical id of the exported group.
pub const PIPE_GROUP_ID: &str = "groups_example:pipe";

const PLUGIN_ID: &str = "groups_example";

#[node(id = "add_one", inputs("x"), outputs("out"))]
fn add_one(x: i32) -> Result<i32, NodeError> {
    Ok(x + 1)
}

#[node(id = "double", inputs("x"), outputs("out"))]
fn double(x: i32) -> Result<i32, NodeError> {
    Ok(x * 2)
}

/// The plugin. Hand-written rather than `declare_plugin!`: the macro merges its nodes after a
/// custom install body, so the group could not see its stages; here they merge first, once.
#[derive(Clone, Debug, Default)]
pub struct NodeGroupsPlugin;

impl Plugin for NodeGroupsPlugin {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(PLUGIN_ID).version(env!("CARGO_PKG_VERSION"))
    }

    fn install(&self, registry: &mut PluginInstallContext<'_>) -> PluginResult<()> {
        registry.merge::<AddOneNode>()?;
        registry.merge::<DoubleNode>()?;
        registry.register_node_group(
            NodeGroup::new(PIPE_GROUP_ID)
                .label("Add then double")
                .stage("add", "groups_example:add_one")
                .stage("double", "groups_example:double")
                .connect(StagePort::new("add", "out"), StagePort::new("double", "x"))
                .input("x", [StagePort::new("add", "x")])
                .output("out", StagePort::new("double", "out")),
        )
    }
}
