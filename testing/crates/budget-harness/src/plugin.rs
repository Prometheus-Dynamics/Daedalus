//! A node from a dylib plugin, ticked through the loader: the path a plugin's node takes.

use std::path::Path;
use std::sync::Arc;

use daedalus_abi::BoundaryPort;
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind, Transfer,
};
use daedalus_graph::{GraphBody, NodeDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_loader::{Loader, Plugin, RegionIn};
use daedalus_planner::{Config, plan};
use daedalus_registry::Registry;

use crate::{bridge, edge};

/// The plugin's one backend, as the planner sees it. Every region here is one node.
struct OneNode {
    plugin: Arc<Plugin>,
}

impl Backend for OneNode {
    fn id(&self) -> &str {
        self.plugin.backends()[0].id()
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: "plugin.host".into(),
            kind: SpaceKind::Host,
            readable: true,
        }]
    }

    fn transfers(&self) -> Vec<Transfer> {
        Vec::new()
    }

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
        let [node] = region.nodes else {
            return Err(Refusal::new(Vec::new(), "one node a region"));
        };
        let id = cx
            .bodies
            .iter()
            .find(|body| body.node == *node)
            .map_or("", |body| body.id);
        let port = |at: u16| BoundaryPort {
            node: 0,
            port: at,
            reserved: 0,
            type_index: 0,
            slot: 0,
        };
        let view = RegionIn {
            nodes: &[id],
            inputs: &[port(0)],
            outputs: &[port(0)],
        };
        let mut units = self.plugin.backends()[0]
            .compile(&view)
            .map_err(|error| Refusal::new(Vec::new(), &error.to_string()))?;
        for unit in &mut units {
            unit.info.covers = vec![*node];
            for port in unit.info.reads.iter_mut().chain(&mut unit.info.writes) {
                port.node = *node;
            }
        }
        Ok(Compiled::Steps(units))
    }
}

/// Loads the dylib fixture at `library` and a graph of its `pass` node between `in` and `out`.
pub fn plugin_node(library: &Path) -> LoadedHost {
    let plugin = Arc::new(Loader::new().open(library).expect("the plugin loads"));
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("the plugin's nodes join");
    let backend = OneNode { plugin };
    let graph = GraphBody {
        nodes: vec![
            bridge(&[("in", "u32"), ("out", "u32")]),
            NodeDoc::new("fixture.pass"),
        ],
        edges: vec![edge(0, "in", 1, "in"), edge(1, "out", 0, "out")],
        groups: Vec::new(),
    };
    let planned =
        plan(&graph, &registry, &[&backend], &Config::default()).expect("the graph plans");
    LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads")
}

/// The dylib fixture's node that hands a megabyte to the host: `in` is a u32, `out` is the bytes.
pub fn handover_node(library: &Path) -> LoadedHost {
    let plugin = Arc::new(Loader::new().open(library).expect("the plugin loads"));
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("the plugin's nodes join");
    let backend = OneNode { plugin };
    let graph = GraphBody {
        nodes: vec![
            bridge(&[("in", "u32"), ("out", "bytes")]),
            NodeDoc::new("fixture.handover"),
        ],
        edges: vec![edge(0, "in", 1, "in"), edge(1, "out", 0, "out")],
        groups: Vec::new(),
    };
    let planned =
        plan(&graph, &registry, &[&backend], &Config::default()).expect("the graph plans");
    LoadedHost::load(planned, LoadOptions::default()).expect("the plan loads")
}

/// One tick of the hand-over node, taken as the host takes any value, then dropped: the bytes are
/// released here. Returns how many bytes came out.
pub fn handover_tick(host: &mut LoadedHost, value: u32) -> usize {
    let io = host.host();
    io.feed_inline("in", &value.to_le_bytes()).expect("a feed");
    io.step().expect("a tick");
    let taken = io.take_value("out").expect("a take").expect("a value");
    let len = taken.bytes().expect("bytes").len();
    drop(taken);
    len
}

/// One tick of the plugin node: fed in place, stepped, taken.
pub fn plugin_tick(host: &mut LoadedHost, value: u32) -> u32 {
    let io = host.host();
    io.feed_inline("in", &value.to_le_bytes()).expect("a feed");
    io.step().expect("a tick");
    io.take_copy::<u32>("out")
        .expect("a take")
        .expect("a value")
}
