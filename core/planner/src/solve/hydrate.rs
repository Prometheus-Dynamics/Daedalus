//! Every node id must have at least one body.

use std::collections::BTreeSet;
use std::fmt;

use daedalus_graph::NodeKindDoc;

use super::Work;
use super::suggest::suggest_nodes;
use crate::pipeline::SolveError;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HydrateError {
    NodeMissing {
        id: String,
        suggestions: Vec<String>,
    },
}

impl fmt::Display for HydrateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HydrateError::NodeMissing { id, suggestions } if suggestions.is_empty() => {
                write!(f, "no node `{id}` is registered")
            }
            HydrateError::NodeMissing { id, suggestions } => {
                write!(
                    f,
                    "no node `{id}` is registered; did you mean {}?",
                    suggestions.join(", ")
                )
            }
        }
    }
}

impl std::error::Error for HydrateError {}

pub fn run(work: &mut Work<'_>) {
    let mut missing = BTreeSet::new();
    for node in &work.graph.nodes {
        if node.doc.kind == NodeKindDoc::Node && work.catalog.entry(&node.doc.id).is_none() {
            missing.insert(node.doc.id.clone());
        }
    }
    for id in missing {
        let suggestions = suggest_nodes(&work.catalog, &id);
        work.errors
            .push(SolveError::Hydrate(HydrateError::NodeMissing {
                id,
                suggestions,
            }));
    }
}
