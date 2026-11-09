//! Building what a backend is offered.

use daedalus_backend::{BodyIndex, OfferNode, PortInfo};
use daedalus_types::{NodeIndex, TypeIndex};

use crate::{Entry, PortDecl, Registry, TypeRef};

struct Planned {
    node: NodeIndex,
    body: BodyIndex,
    inputs: (usize, usize),
    outputs: (usize, usize),
    needs: &'static [&'static str],
}

/// The storage behind an [`Offer`](daedalus_backend::Offer). Call [`OfferData::nodes`] and wrap
/// the result in an `Offer`.
pub struct OfferData {
    ports: Vec<PortInfo<'static>>,
    planned: Vec<Planned>,
}

impl OfferData {
    pub fn nodes(&self) -> Vec<OfferNode<'_>> {
        self.planned
            .iter()
            .map(|p| OfferNode {
                node: p.node,
                body: p.body,
                inputs: &self.ports[p.inputs.0..p.inputs.1],
                outputs: &self.ports[p.outputs.0..p.outputs.1],
                needs: p.needs,
            })
            .collect()
    }
}

impl Registry {
    /// What to offer `backend`: the graph's nodes, given as index and id, that have a body for
    /// it, in the order given. `type_index` maps a port type to the solved table. A node with a
    /// port type it can't map is left out.
    pub fn offer_data(
        &self,
        backend: &str,
        nodes: &[(NodeIndex, &str)],
        type_index: impl Fn(&TypeRef) -> Option<TypeIndex>,
    ) -> OfferData {
        let mut data = OfferData {
            ports: Vec::new(),
            planned: Vec::new(),
        };
        for (node, id) in nodes {
            let Some(entry) = self.entry(id) else {
                continue;
            };
            let Some(at) = entry.bodies.iter().position(|b| b.backend == backend) else {
                continue;
            };
            let start = data.ports.len();
            let inputs = push_ports(&mut data.ports, entry, entry.decl.inputs, &type_index);
            let outputs = inputs.and_then(|inputs| {
                push_ports(&mut data.ports, entry, entry.decl.outputs, &type_index)
                    .map(|outputs| (inputs, outputs))
            });
            let Some((inputs, outputs)) = outputs else {
                data.ports.truncate(start);
                continue;
            };
            data.planned.push(Planned {
                node: *node,
                body: BodyIndex(at as u32),
                inputs,
                outputs,
                needs: entry.bodies[at].needs,
            });
        }
        data
    }
}

fn push_ports(
    out: &mut Vec<PortInfo<'static>>,
    entry: &Entry,
    ports: &'static [PortDecl],
    type_index: &impl Fn(&TypeRef) -> Option<TypeIndex>,
) -> Option<(usize, usize)> {
    let start = out.len();
    for port in ports {
        out.push(PortInfo {
            name: port.name,
            ty: type_index(&entry.port_type(port))?,
            access: port.access,
            optional: port.optional,
        });
    }
    Some((start, out.len()))
}
