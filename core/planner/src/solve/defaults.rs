//! Copies what the declaration says onto the solved node. A value the graph set wins.

use std::fmt;

use daedalus_registry::{Fire, NodeDecl};

use super::{Node, Work};
use crate::pipeline::SolveError;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DefaultsError {
    BadFire { node: String, value: String },
}

impl fmt::Display for DefaultsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DefaultsError::BadFire { node, value } => {
                write!(
                    f,
                    "node `{node}`: fire must be \"any\" or \"all\", not `{value}`"
                )
            }
        }
    }
}

impl std::error::Error for DefaultsError {}

/// Copies a declaration's defaults onto a node.
pub(super) fn apply_declared(node: &mut Node, decl: &NodeDecl) {
    node.fire = decl.fire;
    node.reusable = decl.reusable;
    node.shareable = decl.shareable;
    node.required = decl
        .inputs
        .iter()
        .filter(|port| !port.optional && port.default.is_none())
        .map(|port| port.name.to_string())
        .collect();
}

pub fn run(work: &mut Work<'_>) {
    let catalog = work.catalog;
    for node in &mut work.graph.nodes {
        if let Some(decl) = catalog.decl(&node.doc.id) {
            apply_declared(node, decl);
        }
        node.reusable = node.doc.reusable.unwrap_or(node.reusable);
        node.shareable |= node.doc.shareable;
        match node.doc.fire.as_deref() {
            None => {}
            Some("any") => node.fire = Fire::Any,
            Some("all") => node.fire = Fire::All,
            Some(value) => work
                .errors
                .push(SolveError::Defaults(DefaultsError::BadFire {
                    node: node.doc.id.clone(),
                    value: value.to_string(),
                })),
        }
    }
}
