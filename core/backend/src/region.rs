//! What a backend is given to compile.

use std::any::Any;
use std::collections::BTreeMap;

use daedalus_graph::{EdgePolicyDoc, Lit};
use daedalus_types::{BackendIndex, NodeIndex, PortIndex, TypeExpr};

use crate::{BodyIndex, MemorySpace};

/// For a node with no metadata.
pub static NO_METADATA: BTreeMap<String, Lit> = BTreeMap::new();

/// A port on a solved node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PortRef {
    pub node: NodeIndex,
    pub port: PortIndex,
}

/// An index into the rate table the planner builds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RateId(pub u32);

/// A value that has to move into this backend's memory before an input can read it. The backend
/// may absorb the move into its own work, and says so in the unit it returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Incoming<'a> {
    pub input: PortRef,
    pub from: &'a str,
    pub to: &'a str,
    pub transfer: &'a str,
}

/// A connected set of nodes on one backend. Inside it the backend can do anything, as long as
/// the boundary ports stay and every step lists the nodes it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Region<'a> {
    pub backend: BackendIndex,
    pub nodes: &'a [NodeIndex],
    pub inputs: &'a [PortRef],
    pub outputs: &'a [PortRef],
    pub incoming: &'a [Incoming<'a>],
    pub rate: RateId,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ConstOf<'a> {
    pub node: NodeIndex,
    pub port: &'a str,
    pub value: &'a Lit,
}

/// The code a node's author wrote for this backend.
#[derive(Clone, Copy, Debug)]
pub struct NodeBody<'a> {
    pub node: NodeIndex,
    /// The node's registered id.
    pub id: &'a str,
    pub body: BodyIndex,
    /// How many inputs and outputs this node has in this graph, counting fan-in ports.
    pub inputs: u16,
    pub outputs: u16,
    /// False when the graph asks that this node never be merged with its neighbours.
    pub merges: bool,
    /// Runs only when every input has a value, rather than when any has.
    pub fire_all: bool,
    pub label: Option<&'a str>,
    /// The graph's metadata for this node.
    pub metadata: &'a BTreeMap<String, Lit>,
    /// The backend's own payload: a function pointer or a generated stub.
    pub code: &'a (dyn Any + Send + Sync),
    /// Source, for bodies written as text.
    pub text: &'a str,
}

/// An edge with both ends in the region being compiled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InnerEdge<'a> {
    pub from: PortRef,
    pub to: PortRef,
    /// The edge's policy needs a real queue between its ends.
    pub queued: bool,
    pub policy: &'a EdgePolicyDoc,
}

/// A port on the region's boundary that the host feeds or reads, and what the host calls it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostBinding<'a> {
    pub port: PortRef,
    pub name: &'a str,
    pub policy: &'a EdgePolicyDoc,
}

/// What the consumers of one output do with its value, as the planner worked out from the graph.
/// A backend may keep an output's memory and refill it for the next tick only when the planner says
/// so: a value that is taken by value cannot have a second holder left behind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Sharing {
    /// Nothing is kept. The output is made new each tick.
    #[default]
    Fresh,
    /// Every consumer reads the value by reference, on the same backend. The producer may keep
    /// the value and refill it once no consumer holds it.
    Readers,
    /// Exactly one consumer takes the value by value, on the same backend. The value moves to
    /// that consumer, and the producer may keep a reference to refill it once the consumer has
    /// taken it.
    Taker,
}

/// The sharing of one output port of a region's node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutputSharing {
    pub port: PortRef,
    pub sharing: Sharing,
}

/// Only what a backend needs. It never sees the planner or the registry.
#[derive(Clone, Copy, Debug)]
pub struct CompileCx<'a> {
    /// Indexed by `TypeIndex`.
    pub types: &'a [TypeExpr],
    pub consts: &'a [ConstOf<'a>],
    pub spaces: &'a [MemorySpace],
    pub bodies: &'a [NodeBody<'a>],
    /// The edges with both ends in this region.
    pub edges: &'a [InnerEdge<'a>],
    pub host_inputs: &'a [HostBinding<'a>],
    pub host_outputs: &'a [HostBinding<'a>],
    /// The capacity of a FIFO edge that declares none, for a backend that lays out queues itself.
    pub fifo_capacity: u32,
    pub merge_allowed: bool,
    /// Treat every constant as fixed, even one the graph marks as changeable.
    pub freeze_params: bool,
    /// How each output of the region's nodes is consumed. A port that is not listed is `Fresh`.
    pub sharing: &'a [OutputSharing],
}
