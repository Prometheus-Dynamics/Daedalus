//! A plugin's backend as the planner's `Backend`: what a host puts in the list it plans with.

use std::sync::Arc;

use std::borrow::Cow;

use daedalus_abi::{BoundaryPort, OwnedStep, SlotState, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    CompileCx, Compiled, MemorySpace, NodeRef, Offer, PortRef, Refusal, Region, SpaceKind,
    StepUnit, Transfer, TransferCost, UnitInfo,
};
use daedalus_types::{NodeIndex, PortIndex};

use crate::codec::{Codec, Direction};
use crate::convert::{Convert, direction_of};
use crate::{Plugin, RegionIn};

/// The memory every plugin backend's values live in. They share it, so a value passes from one
/// plugin's node to another's with no transfer. A value reaches it from a node linked into the
/// host, and leaves it for one, through [`CPU_WIRE`].
pub const PLUGIN_SPACE: &str = "plugin.host";

/// The CPU backend's space whose cells hold their type's wire form: the form a value has when it
/// crosses between a node linked into the host and a plugin's node.
pub const CPU_WIRE: &str = "cpu.wire";

pub const TO_PLUGIN: &str = "plugin.from_cpu_wire";
pub const FROM_PLUGIN: &str = "plugin.to_cpu_wire";

/// The two moves between the CPU backend and the plugin space, for any type. Each hands the
/// cell on as it is: the linked node on one side wrote, or reads, its type's wire form, and a
/// plugin's node reads and writes that form. Nothing is copied by the move.
///
/// There is no move between the plugin space and `cpu.host`, where a cell holds a Rust value in
/// the host's memory: such a value never reaches a plugin. A linked node whose output has no
/// wire form is refused when its step is built, by the node, the output and the type.
fn transfers() -> Vec<Transfer> {
    let entry = |id, from, to| Transfer {
        id,
        key: None,
        from: Cow::Borrowed(from),
        to: Cow::Borrowed(to),
        cost: TransferCost::Handoff,
        make: hand,
    };
    vec![
        entry(TO_PLUGIN, CPU_WIRE, PLUGIN_SPACE),
        entry(FROM_PLUGIN, PLUGIN_SPACE, CPU_WIRE),
    ]
}

fn hand(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(Hand))
}

/// Hands a cell on from one space to the other, as it is: the output is marked to take the
/// input's value by reference, which the runner settles when the step returns.
struct Hand;

impl OwnedStep for Hand {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let cells = slots.as_mut_slice();
        let [input, out] = cells else {
            cx.fault(None, "a plugin move has one input and one output");
            return StepEnd::Fault;
        };
        if input.state != SlotState::Full as u32 {
            cx.fault(None, "a plugin move was given no value");
            return StepEnd::Fault;
        }
        daedalus_abi::forward(out, 0);
        StepEnd::Done
    }
}

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
        transfers()
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
        let id_of = |node: &NodeIndex| {
            cx.bodies
                .iter()
                .find(|body| body.node == *node)
                .map_or("", |body| body.id)
        };
        // A codec's conversion is the host's to run, with the codec's own function: the plugin's
        // backend has no step for one. The rest are the plugin's nodes.
        let mut units = Vec::new();
        let mut theirs: Vec<NodeIndex> = Vec::new();
        for node in region.nodes {
            match self.conversion(id_of(node)) {
                Some((codec, direction)) => units.push(conversion_unit(codec, direction, *node)),
                None => theirs.push(*node),
            }
        }
        if !theirs.is_empty() {
            units.extend(self.compile_theirs(&theirs, region, cx, id_of)?);
        }
        Ok(Compiled::Steps(units))
    }
}

/// A conversion node as a unit: one input, one output, run by the host.
fn conversion_unit(codec: &Codec, direction: Direction, node: NodeIndex) -> StepUnit {
    let port = PortRef {
        node,
        port: PortIndex(0),
    };
    StepUnit {
        step: Step::boxed(Convert::new(codec.clone(), direction, node.0)),
        info: UnitInfo {
            covers: vec![node],
            reads: vec![port],
            writes: vec![port],
            absorbed: Vec::new(),
            may_pend: false,
            decodes: Vec::new(),
        },
        patch: None,
    }
}

impl PluginBackend {
    /// Which of the plugin's codecs' conversions the node `id` is, if it is one.
    fn conversion(&self, id: &str) -> Option<(&Codec, Direction)> {
        let codecs = self.plugin.codecs().iter();
        codecs
            .filter_map(|codec| Some((codec, direction_of(codec, id)?)))
            .next()
    }

    /// Compiles the plugin's own nodes of a region, `theirs`, through its backend.
    fn compile_theirs<'a>(
        &self,
        theirs: &[NodeIndex],
        region: &Region<'_>,
        cx: &CompileCx<'a>,
        id_of: impl Fn(&NodeIndex) -> &'a str,
    ) -> Result<Vec<StepUnit>, Refusal> {
        let ids: Vec<&str> = theirs.iter().map(id_of).collect();
        // A plugin's steps are one a node, so each node's ports are named: the region's
        // boundary, and both ends of every edge inside it.
        let inputs = ports(
            theirs,
            region
                .inputs
                .iter()
                .copied()
                .chain(cx.edges.iter().map(|edge| edge.to)),
        );
        let outputs = ports(
            theirs,
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
        let refused: Vec<NodeIndex> = theirs
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
        // The loader numbers nodes by their place among those it was given. The planner wants
        // its own.
        let graph_node = |local: NodeIndex| theirs.get(local.0 as usize).copied().unwrap_or(local);
        for unit in &mut units {
            for node in &mut unit.info.covers {
                *node = graph_node(*node);
            }
            for port in unit.info.reads.iter_mut().chain(&mut unit.info.writes) {
                port.node = graph_node(port.node);
            }
        }
        Ok(units)
    }
}
