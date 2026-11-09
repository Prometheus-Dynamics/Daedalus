//! The one place a caller can change the graph during solving.

use std::fmt;

use daedalus_graph::NodeDoc;

use super::catalog::Catalog;
use super::defaults::apply_declared;
use super::{Graph, Node, Work};
use crate::pipeline::SolveError;

/// What a rewrite did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Applied {
    pub id: String,
    pub version: u32,
    pub summary: String,
    pub changed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RewriteError {
    pub id: String,
    pub message: String,
}

impl fmt::Display for RewriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rewrite `{}` failed: {}", self.id, self.message)
    }
}

impl std::error::Error for RewriteError {}

/// What a rewrite can look at.
pub struct RewriteCx<'a> {
    pub catalog: Catalog<'a>,
}

impl RewriteCx<'_> {
    /// A new node with its declared defaults. An id nothing declares fails hydrate later.
    pub fn node(&self, id: &str) -> Node {
        let mut node = Node::new(NodeDoc::new(id), None);
        if let Some(decl) = self.catalog.decl(id) {
            apply_declared(&mut node, decl);
        }
        node
    }
}

/// A change to the solved graph. Its id and version are part of the plan's identity.
pub trait Rewrite: Send + Sync {
    fn id(&self) -> &str;
    fn version(&self) -> u32;
    /// Returns what happened: a summary, and whether the graph changed.
    fn apply(&self, graph: &mut Graph, cx: &RewriteCx<'_>) -> Result<(String, bool), String>;
}

pub fn run(work: &mut Work<'_>) {
    let cx = RewriteCx {
        catalog: work.catalog,
    };
    for rewrite in &work.config.rewrites {
        match rewrite.apply(&mut work.graph, &cx) {
            Ok((summary, changed)) => work.graph.applied.push(Applied {
                id: rewrite.id().to_string(),
                version: rewrite.version(),
                summary,
                changed,
            }),
            Err(message) => work.errors.push(SolveError::Rewrite(RewriteError {
                id: rewrite.id().to_string(),
                message,
            })),
        }
    }
}
