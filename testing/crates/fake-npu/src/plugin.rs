//! The same backend, loaded from a dylib plugin. A plugin's table has `can_take` and `compile`.
//! Its memory space and its transfer table are this crate's, on the host's side.

use std::sync::Arc;

use daedalus_abi::BoundaryPort;
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, PortRef, Refusal, Region, Transfer,
};
use daedalus_loader::{Plugin, RegionIn};
use daedalus_types::NodeIndex;

use crate::{BACKEND_ID, memory, take_all, transfers};

/// The fake NPU backend a loaded plugin carries.
pub struct PluginNpu {
    plugin: Arc<Plugin>,
    transfers: bool,
}

impl PluginNpu {
    /// The backend `fake-npu` of `plugin`. `None` when the plugin carries no such backend.
    pub fn from_plugin(plugin: Arc<Plugin>) -> Option<PluginNpu> {
        plugin.backend(BACKEND_ID)?;
        Some(PluginNpu {
            plugin,
            transfers: true,
        })
    }

    /// Which load of the plugin this is: the first is 1.
    pub fn generation(&self) -> u64 {
        self.plugin.generation().number()
    }
}

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

impl Backend for PluginNpu {
    fn id(&self) -> &str {
        BACKEND_ID
    }

    /// Another load of the plugin is another version: a plan made with the last one is not
    /// reused, and a reload onto it starts its nodes fresh.
    fn version(&self) -> u64 {
        self.generation()
    }

    fn memory(&self) -> Vec<MemorySpace> {
        memory()
    }

    fn transfers(&self) -> Vec<Transfer> {
        transfers(self.transfers)
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        take_all(offer)
    }

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let Some(backend) = self.plugin.backend(BACKEND_ID) else {
            return Err(Refusal::new(
                Vec::new(),
                "the plugin has no fake NPU backend",
            ));
        };
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
                "the fake NPU plugin does not serve this node",
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
