//! Solving: everything that needs no backend.

mod adapter;
pub mod catalog;
pub mod consts;
pub mod convert;
pub mod defaults;
mod dynamic;
pub mod exclusive;
pub mod expand;
pub mod host_inputs;
pub mod hydrate;
pub mod port_types;
pub mod ports;
pub mod rewrite;
mod suggest;
pub mod typecheck;

use std::collections::BTreeMap;

use daedalus_graph::{EdgeDoc, Lit, NodeDoc};
use daedalus_registry::{Fire, TypeRef};
use daedalus_types::Literal;
use daedalus_value::Access;

use crate::pipeline::{Config, SolveError};
use catalog::Catalog;

/// A port of a solved node.
#[derive(Clone, Debug, PartialEq)]
pub struct Port {
    pub name: String,
    pub ty: TypeRef,
    pub access: Access,
    pub optional: bool,
    pub default: Option<Literal>,
    /// Declared generic: its type came from unification, not the declaration.
    pub generic: bool,
}

/// A node as the solve stages see it: the document's fields, and what they have worked out.
#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub doc: NodeDoc,
    /// The outermost group this node came from.
    pub group: Option<String>,
    pub fire: Fire,
    /// Input ports that must have a value.
    pub required: Vec<String>,
    pub reusable: bool,
    pub shareable: bool,
    pub inputs: Vec<Port>,
    pub outputs: Vec<Port>,
    /// The value of each port that has one, from the graph or its declared default.
    pub consts: BTreeMap<String, Lit>,
}

impl Node {
    pub(crate) fn new(doc: NodeDoc, group: Option<String>) -> Node {
        Node {
            doc,
            group,
            fire: Fire::default(),
            required: Vec::new(),
            reusable: false,
            shareable: false,
            inputs: Vec::new(),
            outputs: Vec::new(),
            consts: BTreeMap::new(),
        }
    }
}

/// A graph with no groups left in it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub edges: Vec<EdgeDoc>,
    /// The rewrites that ran, in order.
    pub applied: Vec<rewrite::Applied>,
}

impl Work<'_> {
    /// Adds a node the planner chose, as if the graph had named it. `None` if nothing declares it.
    pub(crate) fn insert_node(&mut self, id: &str) -> Option<u32> {
        let entry = self.catalog.entry(id)?;
        let mut node = Node::new(NodeDoc::new(id), None);
        defaults::apply_declared(&mut node, entry.decl);
        ports::fill_node(&mut node, entry, false, &mut Vec::new());
        self.graph.nodes.push(node);
        Some(self.graph.nodes.len() as u32 - 1)
    }
}

/// What every stage gets. There is no global state.
pub struct Work<'a> {
    pub graph: Graph,
    pub catalog: Catalog<'a>,
    pub config: &'a Config,
    pub errors: Vec<SolveError>,
}
