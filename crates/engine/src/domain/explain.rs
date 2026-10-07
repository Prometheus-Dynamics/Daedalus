//! [`ExecutionDomain::explain`]: which graphs run upstream of which, the zero-copy fan-out
//! links between them, domain inputs, and the nodes structural sharing merged.

use crate::prelude::*;
use core::fmt;

use daedalus_runtime::executor::NodeHandler;

use super::{ExecutionDomain, LinkMode};

/// The layout of a domain. Print it with `{}`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DomainExplanation {
    /// Graphs in tick order.
    pub graphs: Vec<DomainGraphExplanation>,
    pub inputs: Vec<DomainInputExplanation>,
    pub links: Vec<DomainLinkExplanation>,
    /// Nodes several loaded graphs shared ([`ExecutionDomain::load_shared`]).
    pub shared_nodes: Vec<DomainSharedNode>,
}

/// One graph of a domain.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DomainGraphExplanation {
    pub name: String,
    /// Node labels besides the host bridge.
    pub nodes: Vec<String>,
    /// Graphs it is linked from.
    pub upstreams: Vec<String>,
    /// Graphs its outputs are linked to: more than one means its nodes run once for all of
    /// them.
    pub consumers: Vec<String>,
    /// `(port, reader graph, reader port)`: outputs the host reads through the domain.
    pub taps: Vec<(String, String, String)>,
}

/// A domain input and the graph inputs one push feeds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DomainInputExplanation {
    pub name: String,
    /// `(graph, port)` pairs, each fed an `Arc` clone.
    pub targets: Vec<(String, String)>,
}

/// One fan-out edge: an upstream host output fed to a downstream host input as an `Arc` clone
/// of the payload (no copy).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainLinkExplanation {
    pub from: String,
    pub from_port: String,
    pub to: String,
    pub to_port: String,
    pub mode: LinkMode,
    /// The output's and the input's type keys, when the plans resolved them.
    pub from_type: Option<String>,
    pub to_type: Option<String>,
}

impl DomainLinkExplanation {
    /// The downstream consumes the payload as forwarded: no adapter runs on its input edge
    /// (the types match or the input takes whatever arrives).
    pub fn zero_copy(&self) -> bool {
        match (&self.from_type, &self.to_type) {
            (Some(from), Some(to)) => from == to,
            _ => true,
        }
    }
}

/// A node structural sharing runs once for several graphs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DomainSharedNode {
    /// Its label in the upstream graph.
    pub node: String,
    pub node_id: String,
    /// The graph that runs it.
    pub upstream: String,
    /// The loaded graphs that contained it.
    pub graphs: Vec<String>,
}

impl<H: NodeHandler + Send + Sync + 'static> ExecutionDomain<H> {
    /// The domain's graphs, links, inputs and shared nodes.
    pub fn explain(&self) -> DomainExplanation {
        let name = |index: usize| self.members[index].name.clone();
        let mut links = Vec::new();
        let mut graphs = Vec::with_capacity(self.order.len());
        for &index in &self.order {
            let member = &self.members[index];
            let mut consumers = Vec::new();
            let mut taps = Vec::new();
            for source in &member.sources {
                for target in &source.targets {
                    if !consumers.contains(&name(target.member)) {
                        consumers.push(name(target.member));
                    }
                    links.push(DomainLinkExplanation {
                        from: member.name.clone(),
                        from_port: source.port.to_string(),
                        to: name(target.member),
                        to_port: target.port.to_string(),
                        mode: target.mode,
                        from_type: target.types.0.as_ref().map(ToString::to_string),
                        to_type: target.types.1.as_ref().map(ToString::to_string),
                    });
                }
                taps.extend(
                    source
                        .taps
                        .iter()
                        .map(|tap| (source.port.to_string(), tap.graph.clone(), tap.port.clone())),
                );
            }
            let upstreams = self
                .members
                .iter()
                .filter(|other| {
                    other
                        .sources
                        .iter()
                        .flat_map(|source| &source.targets)
                        .any(|target| target.member == index)
                })
                .map(|other| other.name.clone())
                .collect();
            let nodes = member
                .graph
                .runtime_plan()
                .nodes
                .iter()
                .filter(|node| !daedalus_planner::is_host_bridge_metadata(&node.metadata))
                .map(|node| node.label.clone().unwrap_or_else(|| node.id.clone()))
                .collect();
            graphs.push(DomainGraphExplanation {
                name: member.name.clone(),
                nodes,
                upstreams,
                consumers,
                taps,
            });
        }
        let inputs = self
            .routes
            .iter()
            .map(|route| DomainInputExplanation {
                name: route.name.to_string(),
                targets: route
                    .targets
                    .iter()
                    .map(|(member, port, _)| (name(*member), port.to_string()))
                    .collect(),
            })
            .collect();
        DomainExplanation {
            graphs,
            inputs,
            links,
            shared_nodes: self.shared_nodes.clone(),
        }
    }
}

impl DomainExplanation {
    pub fn graph(&self, name: &str) -> Option<&DomainGraphExplanation> {
        self.graphs.iter().find(|graph| graph.name == name)
    }
}

impl fmt::Display for DomainExplanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let order: Vec<&str> = self.graphs.iter().map(|g| g.name.as_str()).collect();
        writeln!(
            f,
            "domain: {} graphs, tick order {}",
            self.graphs.len(),
            order.join(" -> ")
        )?;
        for input in &self.inputs {
            let targets: Vec<String> = input
                .targets
                .iter()
                .map(|(graph, port)| format!("{graph}.{port}"))
                .collect();
            writeln!(
                f,
                "input {} -> {} (Arc clone each)",
                input.name,
                targets.join(", ")
            )?;
        }
        for graph in &self.graphs {
            write!(
                f,
                "graph {} ({} nodes: {})",
                graph.name,
                graph.nodes.len(),
                graph.nodes.join(", ")
            )?;
            if !graph.upstreams.is_empty() {
                write!(f, " <- {}", graph.upstreams.join(", "))?;
            }
            if graph.consumers.len() > 1 {
                write!(
                    f,
                    "; shared: runs once for {} graphs ({})",
                    graph.consumers.len(),
                    graph.consumers.join(", ")
                )?;
            }
            writeln!(f)?;
            for (port, reader, reader_port) in &graph.taps {
                writeln!(
                    f,
                    "  tap {}.{port} -> host as {reader}.{reader_port}",
                    graph.name
                )?;
            }
        }
        for link in &self.links {
            let types = match (&link.from_type, &link.to_type) {
                (Some(from), Some(to)) if from != to => {
                    format!("{from} -> {to}, adapted downstream")
                }
                (Some(ty), _) | (_, Some(ty)) => ty.clone(),
                _ => "untyped".to_string(),
            };
            writeln!(
                f,
                "link {}.{} -> {}.{} [{}] {}: Arc clone{}",
                link.from,
                link.from_port,
                link.to,
                link.to_port,
                link.mode.as_str(),
                types,
                if link.zero_copy() { ", zero-copy" } else { "" }
            )?;
        }
        for node in &self.shared_nodes {
            writeln!(
                f,
                "shared node {} ({}) in {}: computed once for {}",
                node.node,
                node.node_id,
                node.upstream,
                node.graphs.join(", ")
            )?;
        }
        Ok(())
    }
}
