//! What a backend is given to compile.

use std::any::Any;

use daedalus_graph::Lit;
use daedalus_types::{BackendIndex, NodeIndex, PortIndex, TypeExpr};

use crate::{BodyIndex, MemorySpace};

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
    pub body: BodyIndex,
    /// How many inputs and outputs this node has in this graph, counting fan-in ports.
    pub inputs: u16,
    pub outputs: u16,
    /// False when the graph asks that this node never be merged with its neighbours.
    pub merges: bool,
    /// The backend's own payload: a function pointer or a generated stub.
    pub code: &'a (dyn Any + Send + Sync),
    /// Source, for bodies written as text.
    pub text: &'a str,
}

/// An edge with both ends in the region being compiled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InnerEdge {
    pub from: PortRef,
    pub to: PortRef,
    /// The edge's policy needs a real queue between its ends.
    pub queued: bool,
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
    pub edges: &'a [InnerEdge],
    pub merge_allowed: bool,
}
