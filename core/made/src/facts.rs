//! What a host reads from a plan: each node's id, label and join rules, each edge's policy, the
//! type table and the stitched layout. Nothing here is live, so the same facts come from a plan
//! made in this process and from one loaded from bytes.

use std::collections::BTreeMap;

use daedalus_graph::{EdgePolicyDoc, Lit};
use daedalus_types::TypeExpr;

use crate::stitched::Stitched;

/// One node of the graph, as a host reads it.
#[derive(Clone, Debug, PartialEq)]
pub struct MadeNode {
    pub id: String,
    pub label: Option<String>,
    /// A host bridge: the node that carries values across the host's ports.
    pub bridge: bool,
    pub metadata: BTreeMap<String, Lit>,
    /// Whether the node fires only when all of its inputs have a value.
    pub fire_all: bool,
    /// The input ports that must have a value before the node runs.
    pub required: Vec<String>,
    pub reusable: bool,
    /// The input port names, in port order.
    pub inputs: Vec<String>,
    pub consts: BTreeMap<String, Lit>,
    /// The backend's place in the plan's backend list, for the region the node is in.
    pub backend: Option<u8>,
    /// The output port names, in port order. Empty in a plan stored before they were kept.
    pub outputs: Vec<String>,
}

/// One edge, by the node and port it feeds, and its policy.
#[derive(Clone, Debug, PartialEq)]
pub struct MadeEdge {
    pub to_node: u32,
    pub to_port: String,
    pub policy: EdgePolicyDoc,
}

/// The facts of one plan.
#[derive(Clone, Debug, PartialEq)]
pub struct PlanFacts {
    pub nodes: Vec<MadeNode>,
    pub edges: Vec<MadeEdge>,
    pub types: Vec<TypeExpr>,
    pub stitched: Stitched,
}
