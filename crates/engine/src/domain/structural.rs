//! Structural sharing: load several graphs into one domain and run the node subgraphs they
//! compute identically once ([`daedalus_planner::split_shared_upstream`]), for nodes marked
//! [`NODE_SHAREABLE_META_KEY`].

use crate::prelude::*;
use alloc::collections::BTreeMap;

use daedalus_data::model::Value;
use daedalus_planner::{
    Graph, GraphDocument, HostPortTypes, NodeInstance, descriptor_metadata_value,
    set_host_input_shared, split_shared_upstream,
};
use daedalus_runtime::NODE_SHAREABLE_META_KEY;
use daedalus_runtime::handler_registry::HandlerRegistry;
use daedalus_runtime::handles::PortId;
use daedalus_runtime::plugins::PluginRegistry;

use super::{DomainSharedNode, ExecutionDomain, LinkMode};
use crate::engine::Engine;
use crate::error::EngineError;

/// Name [`ExecutionDomain::load_shared`] gives the shared upstream graph (made unique against
/// the loaded graphs' names).
pub const SHARED_UPSTREAM_GRAPH: &str = "shared";

/// Whether `node` may be shared: [`NODE_SHAREABLE_META_KEY`] `true` on the graph node or on its
/// registry declaration (`#[node(shareable)]`).
pub fn is_shareable(registry: &PluginRegistry, node: &NodeInstance) -> bool {
    let flagged = |value: Option<&Value>| matches!(value, Some(Value::Bool(true)));
    flagged(node.metadata.get(NODE_SHAREABLE_META_KEY))
        || registry
            .transport_capabilities
            .nodes()
            .get(&node.id)
            .is_some_and(|decl| {
                flagged(descriptor_metadata_value(decl, NODE_SHAREABLE_META_KEY).as_ref())
            })
}

/// The host inputs of `graph`'s host bridge.
fn bridge_inputs(graph: &Graph) -> Vec<String> {
    graph
        .host_bridges()
        .next()
        .map(|(node, _)| graph.nodes[node.0].outputs.clone())
        .unwrap_or_default()
}

/// Declare `ports` shared on `graph`'s host bridge (`HOST_SHARED_INPUTS_KEY`).
fn declare_shared<'a>(graph: &mut Graph, ports: impl Iterator<Item = &'a String>) {
    let bridge = graph.host_bridges().next().map(|(node, _)| node.0);
    if let Some(bridge) = bridge {
        for port in ports {
            set_host_input_shared(&mut graph.nodes[bridge].metadata, port);
        }
    }
}

impl ExecutionDomain<HandlerRegistry> {
    /// Compile `graphs` into one domain, running what they compute identically once.
    ///
    /// Nodes that several graphs have with the same id, constants, metadata and inputs (the same
    /// host inputs or equal shared nodes), and that are [shareable](is_shareable), are cut out
    /// into one upstream graph named [`SHARED_UPSTREAM_GRAPH`]. Each graph keeps its other
    /// nodes, compiled as its own domain graph under its name, with a host input per upstream
    /// output it reads, linked latest-only (typed as the upstream output, so its planned adapters
    /// still apply). Every original host input becomes a domain input of the same name, routed
    /// to the upstream and each graph that still reads it. A host output a shared node produced
    /// is served by the upstream: read it with [`ExecutionDomain::take_payload`] under the
    /// original graph and port. A graph left with no nodes of its own is not compiled; its
    /// outputs are all served that way. Without anything to share every graph is compiled as is.
    pub fn load_shared<N: Into<String>>(
        engine: &Engine,
        registry: &PluginRegistry,
        graphs: impl IntoIterator<Item = (N, Graph)>,
    ) -> Result<Self, EngineError> {
        let (names, graphs): (Vec<String>, Vec<Graph>) = graphs
            .into_iter()
            .map(|(name, graph)| (name.into(), graph))
            .unzip();
        let split = split_shared_upstream(&graphs, |node| is_shareable(registry, node));
        let mut domain = Self::new();
        let mut upstream_name = SHARED_UPSTREAM_GRAPH.to_string();
        while names.contains(&upstream_name) {
            upstream_name.push('_');
        }
        // Inputs the domain hands to several graphs (routed more than once, or linked) are
        // declared shared, so by-value consumers get a planned copy.
        let upstream_inputs = split
            .upstream
            .as_ref()
            .map(bridge_inputs)
            .unwrap_or_default();
        let mut routes: BTreeMap<String, usize> = BTreeMap::new();
        let parts_inputs = split
            .parts
            .iter()
            .filter(|p| p.has_nodes)
            .flat_map(|p| &p.inputs);
        for port in upstream_inputs.iter().chain(parts_inputs) {
            *routes.entry(port.to_ascii_lowercase()).or_default() += 1;
        }
        let shared = |port: &str| {
            routes
                .get(&port.to_ascii_lowercase())
                .is_some_and(|&n| n > 1)
        };
        let mut output_types = Vec::new();
        if let Some(mut upstream) = split.upstream {
            declare_shared(&mut upstream, upstream_inputs.iter().filter(|p| shared(p)));
            let host = upstream_inputs.clone();
            let graph = engine.compile_registry(registry, upstream)?;
            output_types = graph.host_outputs();
            domain.add_graph(upstream_name.clone(), graph)?;
            for port in host {
                domain.route_input(PortId::new(port.clone()), &upstream_name, PortId::new(port))?;
            }
        }
        for (name, mut part) in names.iter().zip(split.parts) {
            if part.has_nodes {
                let bridge = part.graph.host_bridges().next().map(|(node, _)| node.0);
                if let Some(bridge) = bridge {
                    let metadata = &mut part.graph.nodes[bridge].metadata;
                    let mut types = HostPortTypes::from_node_metadata(metadata);
                    for link in &part.links {
                        let ty = output_types
                            .iter()
                            .find(|port| port.name() == link.upstream_port)
                            .and_then(|port| port.type_expr.clone());
                        if let Some(ty) = ty {
                            types.declare(true, &link.port, ty);
                        }
                    }
                    types.write_to_node_metadata(metadata);
                }
                let linked = part.links.iter().map(|link| &link.port);
                let routed = part.inputs.iter().filter(|p| shared(p));
                declare_shared(&mut part.graph, linked.chain(routed));
                let graph = engine.compile_registry(registry, part.graph)?;
                domain.add_graph(name.clone(), graph)?;
                for port in &part.inputs {
                    domain.route_input(
                        PortId::new(port.clone()),
                        name,
                        PortId::new(port.clone()),
                    )?;
                }
                for link in &part.links {
                    domain.link(
                        &upstream_name,
                        PortId::new(link.upstream_port.clone()),
                        name,
                        PortId::new(link.port.clone()),
                        LinkMode::Latest,
                    )?;
                }
            }
            let upstream = domain.index(&upstream_name);
            for alias in &part.aliases {
                let Some(upstream) = upstream else { break };
                domain.add_tap(
                    upstream,
                    PortId::new(alias.upstream_port.clone()),
                    name.clone(),
                    Some(alias.port.clone()),
                )?;
            }
        }
        domain.shared_nodes = split
            .nodes
            .into_iter()
            .map(|node| DomainSharedNode {
                node: node.label,
                node_id: node.node_id,
                upstream: upstream_name.clone(),
                graphs: node.graphs.iter().map(|&g| names[g].clone()).collect(),
            })
            .collect();
        Ok(domain)
    }

    /// [`Self::load_shared`] for documents, after checking each one's plugin requirements.
    pub fn load_shared_documents<N: Into<String>>(
        engine: &Engine,
        registry: &PluginRegistry,
        documents: impl IntoIterator<Item = (N, GraphDocument)>,
    ) -> Result<Self, EngineError> {
        let mut graphs = Vec::new();
        for (name, document) in documents {
            engine.check_document(registry, &document)?;
            graphs.push((name, document.into_graph()));
        }
        Self::load_shared(engine, registry, graphs)
    }
}
