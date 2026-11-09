//! Solving: everything that needs no backend.

pub mod catalog;
pub mod defaults;
pub mod expand;
pub mod hydrate;
mod suggest;

use daedalus_graph::{EdgeDoc, NodeDoc};
use daedalus_registry::Fire;

use crate::pipeline::{Config, SolveError};
use catalog::Catalog;

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
