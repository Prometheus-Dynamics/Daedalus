//! Solving: everything that needs no backend.

pub mod catalog;
pub mod consts;
pub mod defaults;
mod dynamic;
pub mod expand;
pub mod hydrate;
pub mod port_types;
pub mod ports;
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
}

/// What every stage gets. There is no global state.
pub struct Work<'a> {
    pub graph: Graph,
    pub catalog: Catalog<'a>,
    pub config: &'a Config,
    pub errors: Vec<SolveError>,
}
