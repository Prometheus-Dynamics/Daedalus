//! Demonstrate nested plugin composition: a parent plugin installs a child plugin
//! so node IDs are prefixed `parent:child:*`.
//!
//! Build & run host:
//!   cargo run -p daedalus-ffi --example plugin_nested_host
//!
//! Build only the plugin:
//!   cargo build -p daedalus-ffi --example plugin_nested

#![crate_type = "cdylib"]

use daedalus::ffi::export_plugin;
use daedalus::macros::node;
use daedalus::runtime::plugins::{Plugin, PluginRegistry, RegistryPluginExt};

// Child plugin: a simple blur placeholder node (CPU stub for demo).
#[node(id = "child.blur", inputs("x"), outputs("x"))]
fn blur_node(x: i32) -> Result<i32, daedalus::runtime::NodeError> {
    Ok(x + 1)
}

#[node(id = "child.sharpen", inputs("x"), outputs("x"))]
fn sharpen_node(x: i32) -> Result<i32, daedalus::runtime::NodeError> {
    Ok(x - 1)
}

daedalus::declare_plugin!(ChildPlugin, "child", [blur_node, sharpen_node]);

#[derive(Clone, Debug, Default)]
pub struct ParentPlugin;

impl Plugin for ParentPlugin {
    fn id(&self) -> &'static str {
        "cv"
    }

    fn install(&self, reg: &mut PluginRegistry) -> Result<(), &'static str> {
        reg.install_plugin(&ChildPlugin::default())?;
        Ok(())
    }
}

// Export the parent plugin for dynamic loading via FFI.
export_plugin!(ParentPlugin);
