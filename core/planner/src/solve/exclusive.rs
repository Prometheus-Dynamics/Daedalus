//! A consumer that takes its input by value needs a value nobody else can see.

use std::collections::BTreeMap;
use std::fmt;

use daedalus_graph::{EdgeDoc, EdgePolicyDoc, HostInputPolicy, NodeKindDoc, PortRefDoc};
use daedalus_registry::ConversionEntry;
use daedalus_types::TypeKey;
use daedalus_value::Access;

use super::Work;
use super::convert::key_of;
use super::port_types::display;
use crate::pipeline::SolveError;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExclusiveError {
    /// A value must be copied and no conversion from its type to itself exists.
    CopyMissing {
        node: String,
        port: String,
        ty: String,
    },
}

impl fmt::Display for ExclusiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExclusiveError::CopyMissing { node, port, ty } => write!(
                f,
                "`{node}`.{port} takes `{ty}` by value from a shared source, and no copy for that type is registered"
            ),
        }
    }
}

impl std::error::Error for ExclusiveError {}

fn copy_for<'t>(table: &'t [ConversionEntry], key: &TypeKey) -> Option<&'t ConversionEntry> {
    table
        .iter()
        .filter(|conversion| conversion.from == *key && conversion.to == *key)
        .min_by_key(|conversion| (conversion.cost, conversion.id))
}

/// Whether more than one reader can see the value on this edge's source.
fn source_is_shared(work: &Work<'_>, edge: &EdgeDoc, fanout: &BTreeMap<(u32, &str), u32>) -> bool {
    let doc = &work.graph.nodes[edge.from.node as usize].doc;
    let policy = doc
        .host_inputs
        .get(&edge.from.port)
        .copied()
        .unwrap_or_default();
    let held = doc.kind == NodeKindDoc::HostBridge && policy != HostInputPolicy::Consume;
    held || fanout[&(edge.from.node, edge.from.port.as_str())] > 1
}

pub fn run(work: &mut Work<'_>) {
    let table = work.catalog.conversions();
    let edges = std::mem::take(&mut work.graph.edges);
    let mut fanout: BTreeMap<(u32, &str), u32> = BTreeMap::new();
    for edge in &edges {
        *fanout
            .entry((edge.from.node, edge.from.port.as_str()))
            .or_default() += 1;
    }
    let mut out = Vec::with_capacity(edges.len());
    for edge in &edges {
        let Some(port) = work
            .graph
            .nodes
            .get(edge.to.node as usize)
            .and_then(|node| node.inputs.iter().find(|port| port.name == edge.to.port))
        else {
            out.push(edge.clone());
            continue;
        };
        let by_value = matches!(port.access, Access::Move | Access::Modify);
        if !by_value
            || (edge.from.node as usize) >= work.graph.nodes.len()
            || !source_is_shared(work, edge, &fanout)
        {
            out.push(edge.clone());
            continue;
        }
        let ty = port.ty.clone();
        let copy = key_of(&ty).and_then(|key| copy_for(&table, &key));
        let Some(index) = copy.and_then(|copy| work.insert_node(copy.id)) else {
            let node = work.graph.nodes[edge.to.node as usize].doc.id.clone();
            work.errors
                .push(SolveError::Exclusive(ExclusiveError::CopyMissing {
                    node,
                    port: edge.to.port.clone(),
                    ty: display(&ty),
                }));
            out.push(edge.clone());
            continue;
        };
        let node = &work.graph.nodes[index as usize];
        let (input, output) = (node.inputs[0].name.clone(), node.outputs[0].name.clone());
        out.push(EdgeDoc {
            from: edge.from.clone(),
            to: PortRefDoc {
                node: index,
                port: input,
            },
            policy: EdgePolicyDoc::default(),
        });
        out.push(EdgeDoc {
            from: PortRefDoc {
                node: index,
                port: output,
            },
            to: edge.to.clone(),
            policy: edge.policy.clone(),
        });
    }
    work.graph.edges = out;
}
