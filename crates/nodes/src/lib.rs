//! Built-in nodes and bundles. See `PLAN.md` for staged work.
//! Bundles are feature-gated; registration order is deterministic.
//!
/// Re-export of the `#[node]` macro for convenience.
pub use daedalus_macros::node;
pub type NodeDecl = daedalus_registry::capability::NodeDecl;

#[cfg(feature = "bundle-starter")]
mod bundle_starter;

#[cfg(feature = "bundle-utils")]
mod bundle_utils;

#[cfg(feature = "plugins")]
pub mod bundle_demo;
extern crate self as daedalus_nodes;
#[cfg(feature = "plugins")]
pub mod __macro_support {
    pub use daedalus_runtime::plugins::{
        NodeInstall, Plugin, PluginError, PluginInstallContext, PluginRegistry, PluginResult,
    };
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
#[cfg(feature = "plugins")]
#[macro_export]
macro_rules! declare_plugin {
    // Basic form with transport adapters.
    ($plugin:ident, $id:expr, [ $( $node:ident ),+ $(,)? ], adapters [ $( $adapter:ident ),+ $(,)? ]) => {
        paste::paste! {
            #[derive(Clone, Debug)]
            pub struct $plugin {
                $(pub $node: [<$node:camel Node Handle>]),+
            }

            impl $plugin {
                pub fn new() -> Self {
                    Self {
                        $($node: [<$node:camel Node>]::handle().with_prefix($id)),+
                    }
                }

                $(
                    pub fn [<node_ $node>](&self) -> [<$node:camel Node Handle>] {
                        [<$node:camel Node>]::handle().with_prefix($id)
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
                    registry: &mut $crate::__macro_support::PluginInstallContext<'_>,
                ) -> $crate::__macro_support::PluginResult<()> {
                    $(
                        [<register_ $adapter _adapter>](registry)?;
                    )+
                    $(
                        for __contract in [<$node:camel Node>]::boundary_contracts()? {
                            registry.boundary_contract(__contract)?;
                        }
                    )+
                    $(
                        registry.merge::<[<$node:camel Node>]>()?;
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
                    registry: &mut $crate::__macro_support::PluginInstallContext<'_>,
                ) -> $crate::__macro_support::PluginResult<()> {
                    self.install(registry)
                }
            }
        }
    };

    // Basic form (no hook).
    ($plugin:ident, $id:expr, [ $( $node:ident ),+ $(,)? ]) => {
        paste::paste! {
            #[derive(Clone, Debug)]
            pub struct $plugin {
                $(pub $node: [<$node:camel Node Handle>]),+
            }

            impl $plugin {
                pub fn new() -> Self {
                    Self {
                        $($node: [<$node:camel Node>]::handle().with_prefix($id)),+
                    }
                }

                $(
                    pub fn [<node_ $node>](&self) -> [<$node:camel Node Handle>] {
                        [<$node:camel Node>]::handle().with_prefix($id)
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
                    registry: &mut $crate::__macro_support::PluginInstallContext<'_>,
                ) -> $crate::__macro_support::PluginResult<()> {
                    $(
                        for __contract in [<$node:camel Node>]::boundary_contracts()? {
                            registry.boundary_contract(__contract)?;
                        }
                    )+
                    $(
                        registry.merge::<[<$node:camel Node>]>()?;
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
                    registry: &mut $crate::__macro_support::PluginInstallContext<'_>,
                ) -> $crate::__macro_support::PluginResult<()> {
                    self.install(registry)
                }
            }
        }
    };

    // Form with an install hook and transport adapters.
    ($plugin:ident, $id:expr, [ $( $node:ident ),+ $(,)? ], adapters [ $( $adapter:ident ),+ $(,)? ], install = |$reg:ident| $body:block) => {
        paste::paste! {
            #[derive(Clone, Debug)]
            pub struct $plugin {
                $(pub $node: [<$node:camel Node Handle>]),+
            }

            impl $plugin {
                pub fn new() -> Self {
                    Self {
                        $($node: [<$node:camel Node>]::handle().with_prefix($id)),+
                    }
                }

                $(
                    pub fn [<node_ $node>](&self) -> [<$node:camel Node Handle>] {
                        [<$node:camel Node>]::handle().with_prefix($id)
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
                    registry: &mut $crate::__macro_support::PluginInstallContext<'_>,
                ) -> $crate::__macro_support::PluginResult<()> {
                    let $reg = registry;
                    (|| -> $crate::__macro_support::PluginResult<()> { $body; Ok(()) })()?;
                    $(
                        [<register_ $adapter _adapter>]($reg)?;
                    )+
                    $(
                        for __contract in [<$node:camel Node>]::boundary_contracts()? {
                            $reg.boundary_contract(__contract)?;
                        }
                    )+
                    $(
                        $reg.merge::<[<$node:camel Node>]>()?;
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
                    registry: &mut $crate::__macro_support::PluginInstallContext<'_>,
                ) -> $crate::__macro_support::PluginResult<()> {
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
                $(pub $node: [<$node:camel Node Handle>]),+
            }

            impl $plugin {
                pub fn new() -> Self {
                    Self {
                        $($node: [<$node:camel Node>]::handle().with_prefix($id)),+
                    }
                }

                $(
                    pub fn [<node_ $node>](&self) -> [<$node:camel Node Handle>] {
                        [<$node:camel Node>]::handle().with_prefix($id)
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
                    registry: &mut $crate::__macro_support::PluginInstallContext<'_>,
                ) -> $crate::__macro_support::PluginResult<()> {
                    let $reg = registry;
                    (|| -> $crate::__macro_support::PluginResult<()> { $body; Ok(()) })()?;
                    $(
                        for __contract in [<$node:camel Node>]::boundary_contracts()? {
                            $reg.boundary_contract(__contract)?;
                        }
                    )+
                    $(
                        $reg.merge::<[<$node:camel Node>]>()?;
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
                    registry: &mut $crate::__macro_support::PluginInstallContext<'_>,
                ) -> $crate::__macro_support::PluginResult<()> {
                    self.install(registry)
                }
            }
        }
    };
}

/// Register all enabled bundles, returning descriptors in deterministic order.
///
/// ```ignore
/// use daedalus_nodes::register_all;
/// let nodes = register_all();
/// let _ = nodes;
/// ```
pub fn register_all() -> Vec<NodeDecl> {
    let mut nodes: Vec<NodeDecl> = Vec::new();
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

    #[test]
    fn deterministic_ordering() {
        let nodes = register_all();
        let mut sorted = nodes.clone();
        sorted.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        assert_eq!(nodes, sorted);
    }
}
