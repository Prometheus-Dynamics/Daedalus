//! Cutting what several graphs compute identically out into one upstream graph.
//!
//! Two graphs that run the same nodes on the same host inputs need do that work once. The nodes
//! they agree on move to an upstream graph, and each graph keeps the rest, fed by the upstream's
//! outputs through new host inputs. Whether to do this at all is the caller's decision.

mod classes;
mod split;

use daedalus_graph::GraphBody;

use crate::solve::Graph;

/// The name of the host bridge that carries the upstream's outputs, in the upstream and in each
/// remainder.
pub const UPSTREAM_HOST: &str = "daedalus.upstream";

/// A graph's own host output that a shared node now produces, so nothing in the remainder does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostAlias {
    pub host: String,
    pub port: String,
    pub upstream_port: String,
}

/// What is left of one graph.
#[derive(Clone, Debug, PartialEq)]
pub struct Remainder {
    pub graph: GraphBody,
    /// Whether anything other than host bridges is left.
    pub has_nodes: bool,
    /// The upstream outputs this graph reads, through inputs of the same name on `UPSTREAM_HOST`.
    pub links: Vec<String>,
    pub aliases: Vec<HostAlias>,
}

/// One node of the upstream, and the nodes it replaced as `(graph, node)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedNode {
    pub upstream_node: u32,
    pub replaces: Vec<(u32, u32)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Split {
    /// `None` when no two graphs have anything in common.
    pub upstream: Option<GraphBody>,
    /// One per input graph, in order.
    pub remainders: Vec<Remainder>,
    pub shared: Vec<SharedNode>,
}

/// Finds the prefix several solved graphs have in common and cuts it out.
///
/// A node qualifies only if it is flagged shareable and every source it has is a host input or
/// another shared node, so the upstream is a prefix of each graph that uses it. Graphs agree on a
/// host input when its bridge, name, type and held policy all match.
pub fn split_shared(graphs: &[&Graph]) -> Split {
    split::build(graphs, &classes::classify(graphs))
}
