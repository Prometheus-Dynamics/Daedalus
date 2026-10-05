//! Nodes over Styx `FrameLease`s (feature `styx-framelease`).
//!
//! `FrameLease` belongs to Styx. Styx's own `daedalus` feature (`styx_core::daedalus`) registers
//! it under `styx:framelease`; without that feature the type has no key, so this plugin maps it
//! to the same key with `foreign_types`, and its graphs stay valid in hosts that install Styx's
//! `StyxFramesPlugin` instead.

#[cfg(feature = "styx-framelease")]
mod framelease_plugin {
    use daedalus::{
        macros::{node, plugin},
        runtime::NodeError,
    };
    use styx::prelude::*;

    #[node(id = "mark_timestamp", inputs("frame"), outputs("frame"))]
    fn mark_timestamp(mut frame: FrameLease) -> Result<FrameLease, NodeError> {
        frame.meta_mut().timestamp = frame.meta().timestamp.saturating_add(1);
        Ok(frame)
    }

    #[node(id = "touch_first_byte", inputs("frame"), outputs("frame"))]
    fn touch_first_byte(mut frame: FrameLease) -> Result<FrameLease, NodeError> {
        if let Some(mut plane) = frame.planes_mut().into_iter().next()
            && let Some(first) = plane.data().first_mut()
        {
            *first = first.saturating_add(1);
        }
        Ok(frame)
    }

    /// `framelease_dynamic:mark_timestamp` and `framelease_dynamic:touch_first_byte`.
    #[plugin(
        id = "framelease_dynamic",
        nodes(mark_timestamp, touch_first_byte),
        foreign_types(FrameLease = "styx:framelease")
    )]
    pub struct FrameLeaseDynamicPlugin;

    #[cfg(test)]
    mod tests {
        use super::FrameLeaseDynamicPlugin;
        use daedalus::PluginRegistry;
        use daedalus::runtime::plugins::RegistryPluginExt;
        use daedalus::transport::TypeKey;

        #[test]
        fn installs_with_the_styx_frame_key() {
            let mut registry = PluginRegistry::new();
            registry
                .install_plugin(&FrameLeaseDynamicPlugin::new())
                .expect("install");
            let decl = registry
                .transport_capabilities
                .nodes()
                .values()
                .find(|decl| decl.id.0 == "framelease_dynamic:mark_timestamp")
                .expect("node");
            assert_eq!(decl.inputs[0].type_key, TypeKey::new("styx:framelease"));
        }
    }
}

#[cfg(feature = "styx-framelease")]
pub use framelease_plugin::FrameLeaseDynamicPlugin;
