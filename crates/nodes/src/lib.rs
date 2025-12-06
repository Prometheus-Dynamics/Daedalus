//! Built-in nodes and bundles. See `PLAN.md` for staged work.
//! Bundles are feature-gated; registration order is deterministic.
//!
/// Re-export of the `#[node]` macro for convenience.
pub use daedalus_macros::node;
pub type NodeDescriptor = daedalus_registry::store::NodeDescriptor;

#[cfg(feature = "bundle-starter")]
mod bundle_starter;

#[cfg(feature = "bundle-utils")]
mod bundle_utils;

#[cfg(feature = "registry-adapter")]
pub mod registry_adapter;

#[cfg(feature = "planner-adapter")]
pub mod planner_adapter;

pub mod bundle_demo;
extern crate self as daedalus_nodes;
pub mod __macro_support {
    pub use daedalus_runtime::plugins::{NodeInstall, Plugin, PluginRegistry};
}

/// Declare a plugin struct that installs a set of node descriptors and handlers
/// in one shot. Each entry should correspond to a `#[node]`-annotated
/// function name in scope so `<name>_descriptor` and `<name>_handler` exist.
///
/// ```ignore
/// use daedalus_nodes::declare_plugin;
/// use daedalus_macros::node;
///
/// #[node(id = "demo:noop", inputs("in"), outputs("out"))]
/// fn noop(value: i64) -> Result<i64, daedalus_runtime::NodeError> {
///     Ok(value)
/// }
///
/// declare_plugin!(DemoPlugin, "demo", [noop]);
/// ```
#[macro_export]
macro_rules! declare_plugin {
    // Basic form (no hook).
    ($plugin:ident, $id:expr, [ $( $node:ident ),+ $(,)? ]) => {
        paste::paste! {
            #[derive(Clone, Debug)]
            pub struct $plugin {
                $(pub $node: [<$node Handle>]),+
            }

            impl $plugin {
                pub fn new() -> Self {
                    Self {
                        $($node: $node::handle().with_prefix($id)),+
                    }
                }

                $(
                    pub fn [<node_ $node>](&self) -> [<$node Handle>] {
                        $node::handle().with_prefix($id)
                    }
                )+
            }

            impl Default for $plugin {
                fn default() -> Self {
                    Self::new()
                }
            }

            impl $plugin {
                pub fn install(
                    &self,
                    registry: &mut $crate::__macro_support::PluginRegistry,
                ) -> Result<(), &'static str> {
                    $(
                        registry.merge::<$node>()?;
                    )+
                    Ok(())
                }
            }

            #[cfg(feature = "plugins")]
            impl $crate::__macro_support::Plugin for $plugin {
                fn id(&self) -> &'static str {
                    $id
                }

                fn install(
                    &self,
                    registry: &mut $crate::__macro_support::PluginRegistry,
                ) -> Result<(), &'static str> {
                    self.install(registry)
                }
            }
        }
    };

    // Form with an install hook: the block runs before node merges and can return
    // an error. Binding name is provided by the caller.
    ($plugin:ident, $id:expr, [ $( $node:ident ),+ $(,)? ], install = |$reg:ident| $body:block) => {
        paste::paste! {
            #[derive(Clone, Debug)]
            pub struct $plugin {
                $(pub $node: [<$node Handle>]),+
            }

            impl $plugin {
                pub fn new() -> Self {
                    Self {
                        $($node: $node::handle().with_prefix($id)),+
                    }
                }

                $(
                    pub fn [<node_ $node>](&self) -> [<$node Handle>] {
                        $node::handle().with_prefix($id)
                    }
                )+
            }

            impl Default for $plugin {
                fn default() -> Self {
                    Self::new()
                }
            }

            impl $plugin {
                pub fn install(
                    &self,
                    registry: &mut $crate::__macro_support::PluginRegistry,
                ) -> Result<(), &'static str> {
                    let $reg = registry;
                    (|| -> Result<(), &'static str> { $body; Ok(()) })()?;
                    $(
                        $reg.merge::<$node>()?;
                    )+
                    Ok(())
                }
            }

            #[cfg(feature = "plugins")]
            impl $crate::__macro_support::Plugin for $plugin {
                fn id(&self) -> &'static str {
                    $id
                }

                fn install(
                    &self,
                    registry: &mut $crate::__macro_support::PluginRegistry,
                ) -> Result<(), &'static str> {
                    self.install(registry)
                }
            }
        }
    };
}

/// Register all enabled bundles, returning descriptors in deterministic order.
///
/// ```
/// use daedalus_nodes::register_all;
/// let nodes = register_all();
/// let _ = nodes;
/// ```
pub fn register_all() -> Vec<NodeDescriptor> {
    let mut nodes: Vec<NodeDescriptor> = Vec::new();
    #[cfg(feature = "bundle-starter")]
    {
        nodes.extend(bundle_starter::nodes());
    }
    #[cfg(feature = "bundle-utils")]
    {
        nodes.extend(bundle_utils::nodes());
    }
    nodes.sort_by(|a, b| a.id.0.cmp(&b.id.0));
    nodes
}

#[cfg(all(test, feature = "bundle-starter"))]
mod tests {
    use super::*;
    use daedalus_core::compute::ComputeAffinity;

    #[test]
    fn deterministic_ordering() {
        let nodes = register_all();
        let mut sorted = nodes.clone();
        sorted.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        assert_eq!(nodes, sorted);
    }

    #[test]
    fn node_macro_attaches_metadata() {
        let first = register_all()
            .into_iter()
            .find(|n| n.id.0 == "starter.print")
            .expect("starter.print registered");
        assert_eq!(first.default_compute, ComputeAffinity::CpuOnly);
    }
}
