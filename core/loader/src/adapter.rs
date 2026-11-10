//! A plugin's backend as the planner's `Backend`: what a host puts in the list it plans with.

use std::sync::Arc;

use daedalus_abi::BoundaryPort;
use daedalus_backend::{
    CompileCx, Compiled, MemorySpace, NodeRef, Offer, PortRef, Refusal, Region, SpaceKind, Transfer,
};
use daedalus_types::NodeIndex;

use crate::{Plugin, RegionIn};

/// The memory every plugin backend's values live in. They share it, so a value passes from one
/// plugin's node to another's with no transfer.
pub const PLUGIN_SPACE: &str = "plugin.host";

/// One backend of a loaded plugin, as the planner sees it. It keeps the plugin, so the library
/// stays open while a planner or a host holds the backend.
pub struct PluginBackend {
    plugin: Arc<Plugin>,
    index: usize,
}

impl PluginBackend {
    /// The backend at `index` of `plugin`'s [`Plugin::backends`].
    pub fn new(plugin: Arc<Plugin>, index: usize) -> PluginBackend {
        PluginBackend { plugin, index }
    }

    /// Every backend `plugin` brings, wrapped.
    pub fn all(plugin: &Arc<Plugin>) -> Vec<PluginBackend> {
        (0..plugin.backends().len())
            .map(|index| PluginBackend::new(Arc::clone(plugin), index))
            .collect()
    }

    fn inner(&self) -> &crate::Backend {
        &self.plugin.backends()[self.index]
    }
}

/// A region's ports as the plugin is told them: by the node's place in the region.
fn ports(nodes: &[NodeIndex], refs: impl Iterator<Item = PortRef>) -> Vec<BoundaryPort> {
    let mut ports: Vec<BoundaryPort> = refs
        .filter_map(|port| {
            let at = nodes.iter().position(|node| *node == port.node)?;
            Some(BoundaryPort {
                node: at as u32,
                port: port.port.0,
                reserved: 0,
                type_index: 0,
                slot: 0,
            })
        })
        .collect();
    ports.sort_by_key(|port| (port.node, port.port));
    ports.dedup_by_key(|port| (port.node, port.port));
    ports
}

impl daedalus_backend::Backend for PluginBackend {
    fn id(&self) -> &str {
        self.inner().id()
    }

    /// A reloaded plugin is another generation, and a plan made with the old one is not reused.
    fn version(&self) -> u64 {
        self.plugin.generation().number()
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: PLUGIN_SPACE.into(),
            kind: SpaceKind::Host,
            readable: true,
        }]
    }

    fn transfers(&self) -> Vec<Transfer> {
        Vec::new()
    }

    /// An offer carries no node ids, so everything with a body for this backend is taken here.
    /// A node the plugin does not serve is refused when its region is compiled.
    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        offer
            .nodes
            .iter()
            .map(|node| NodeRef {
                node: node.node,
                body: node.body,
            })
            .collect()
    }

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let ids: Vec<&str> = region
            .nodes
            .iter()
            .map(|node| {
                cx.bodies
                    .iter()
                    .find(|body| body.node == *node)
                    .map_or("", |body| body.id)
            })
            .collect();
        // A plugin's steps are one a node, so each node's ports are named: the region's
        // boundary, and both ends of every edge inside it.
        let inputs = ports(
            region.nodes,
            region
                .inputs
                .iter()
                .copied()
                .chain(cx.edges.iter().map(|edge| edge.to)),
        );
        let outputs = ports(
            region.nodes,
            region
                .outputs
                .iter()
                .copied()
                .chain(cx.edges.iter().map(|edge| edge.from)),
        );
        let view = RegionIn {
            nodes: &ids,
            inputs: &inputs,
            outputs: &outputs,
        };
        let backend = self.inner();
        let takes = backend
            .can_take(&view)
            .map_err(|error| Refusal::new(Vec::new(), &error.to_string()))?;
        let refused: Vec<NodeIndex> = region
            .nodes
            .iter()
            .zip(&takes)
            .filter(|(_, takes)| !**takes)
            .map(|(node, _)| *node)
            .collect();
        if !refused.is_empty() {
            return Err(Refusal::new(
                refused,
                &format!("plugin backend `{}` does not serve this node", backend.id()),
            ));
        }
        let mut units = backend
            .compile(&view)
            .map_err(|error| Refusal::new(Vec::new(), &error.to_string()))?;
        // The loader numbers nodes by their place in the region. The planner wants its own.
        let graph_node =
            |local: NodeIndex| region.nodes.get(local.0 as usize).copied().unwrap_or(local);
        for unit in &mut units {
            for node in &mut unit.info.covers {
                *node = graph_node(*node);
            }
            for port in unit.info.reads.iter_mut().chain(&mut unit.info.writes) {
                port.node = graph_node(port.node);
            }
        }
        Ok(Compiled::Steps(units))
    }
}
