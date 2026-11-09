//! The layout of a domain, as data and as text.

#![cfg(feature = "std")]

use std::fmt;
use std::string::String;
use std::vec::Vec;

use super::{Domain, Link};

/// One graph and what it's linked with.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DomainGraph {
    pub name: String,
    pub nodes: Vec<String>,
    pub upstreams: Vec<String>,
    pub consumers: Vec<String>,
}

/// The layout of a domain. Print it with `{}`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DomainExplanation {
    /// Graphs in tick order.
    pub graphs: Vec<DomainGraph>,
    pub links: Vec<Link>,
}

impl DomainExplanation {
    pub fn graph(&self, name: &str) -> Option<&DomainGraph> {
        self.graphs.iter().find(|graph| graph.name == name)
    }
}

impl fmt::Display for DomainExplanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for graph in &self.graphs {
            write!(f, "graph {}", graph.name)?;
            if !graph.consumers.is_empty() {
                write!(f, " -> {}", graph.consumers.join(", "))?;
            }
            writeln!(f, " [{}]", graph.nodes.join(", "))?;
        }
        for link in &self.links {
            writeln!(f, "{}", link.describe())?;
        }
        Ok(())
    }
}

fn distinct(mut names: Vec<String>) -> Vec<String> {
    names.sort_unstable();
    names.dedup();
    names
}

impl Domain {
    /// How the graphs are laid out.
    pub fn explain(&self) -> DomainExplanation {
        let graphs = self
            .order
            .iter()
            .map(|&at| {
                let graph = &self.graphs[at];
                let related = |pick: &dyn Fn(&Link) -> Option<String>| {
                    distinct(self.links.iter().filter_map(pick).collect())
                };
                DomainGraph {
                    name: graph.name.clone(),
                    nodes: graph.host.node_keys().map(String::from).collect(),
                    upstreams: related(&|link: &Link| {
                        (link.to_graph == graph.name).then(|| link.from_graph.clone())
                    }),
                    consumers: related(&|link: &Link| {
                        (link.from_graph == graph.name).then(|| link.to_graph.clone())
                    }),
                }
            })
            .collect();
        DomainExplanation {
            graphs,
            links: self.links.clone(),
        }
    }
}
