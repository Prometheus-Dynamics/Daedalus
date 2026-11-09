//! Puts conversion nodes on edges whose two ends declare different types.

use std::fmt;

use daedalus_graph::{EdgeDoc, EdgePolicyDoc, PortRefDoc};
use daedalus_registry::TypeRef;
use daedalus_types::{Scalar, TypeExpr, TypeKey};

use super::Work;
use super::adapter::Routes;
use super::consts::scalar_of;
use super::port_types::{display, same};
use crate::pipeline::SolveError;

const NUMERIC_NOTE: &str = "a numeric conversion may lose data, and is never implicit";

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConvertError {
    ConverterMissing {
        from_node: String,
        from_port: String,
        to_node: String,
        to_port: String,
        from_type: String,
        to_type: String,
        note: Option<&'static str>,
    },
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConvertError::ConverterMissing {
                from_node,
                from_port,
                to_node,
                to_port,
                from_type,
                to_type,
                note,
            } => {
                write!(
                    f,
                    "no conversion from `{from_type}` to `{to_type}` for `{from_node}`.{from_port} -> `{to_node}`.{to_port}"
                )?;
                match note {
                    Some(note) => write!(f, " ({note})"),
                    None => Ok(()),
                }
            }
        }
    }
}

impl std::error::Error for ConvertError {}

pub(crate) fn key_of(ty: &TypeRef) -> Option<TypeKey> {
    match ty {
        TypeRef::Key(key) | TypeRef::Expr(TypeExpr::Opaque(key)) => Some(key.clone()),
        TypeRef::Expr(TypeExpr::Scalar(scalar)) => Some(scalar.key()),
        _ => None,
    }
}

fn numeric(ty: &TypeRef) -> bool {
    let scalar = key_of(ty).and_then(|key| scalar_of(&key));
    scalar.is_some_and(|scalar| {
        !matches!(
            scalar,
            Scalar::Unit | Scalar::Bool | Scalar::String | Scalar::Bytes
        )
    })
}

/// A port's type and whether it was generic. `None` if the port stage failed on it.
fn end(work: &Work<'_>, node: u32, port: &str, input: bool) -> Option<(TypeRef, bool)> {
    let node = work.graph.nodes.get(node as usize)?;
    let ports = if input { &node.inputs } else { &node.outputs };
    ports
        .iter()
        .find(|known| known.name == port)
        .map(|known| (known.ty.clone(), known.generic))
}

/// The edges through `path`, ending with the original edge's policy.
fn route_edge(work: &mut Work<'_>, edge: &EdgeDoc, path: &[&str], out: &mut Vec<EdgeDoc>) {
    let mut from = edge.from.clone();
    for id in path {
        let Some(index) = work.insert_node(id) else {
            continue; // The table came from this registry, so every id is there.
        };
        let node = &work.graph.nodes[index as usize];
        let (input, output) = (node.inputs[0].name.clone(), node.outputs[0].name.clone());
        out.push(EdgeDoc {
            from,
            to: PortRefDoc {
                node: index,
                port: input,
            },
            policy: EdgePolicyDoc::default(),
        });
        from = PortRefDoc {
            node: index,
            port: output,
        };
    }
    out.push(EdgeDoc {
        from,
        to: edge.to.clone(),
        policy: edge.policy.clone(),
    });
}

pub fn run(work: &mut Work<'_>) {
    let mut routes = Routes::new(&work.catalog.conversions());
    let edges = std::mem::take(&mut work.graph.edges);
    let mut out = Vec::with_capacity(edges.len());
    for edge in edges {
        let from = end(work, edge.from.node, &edge.from.port, false);
        let to = end(work, edge.to.node, &edge.to.port, true);
        let (Some((from_ty, false)), Some((to_ty, false))) = (from, to) else {
            out.push(edge);
            continue;
        };
        if same(&from_ty, &to_ty) {
            out.push(edge);
            continue;
        }
        let path = match (key_of(&from_ty), key_of(&to_ty)) {
            (Some(a), Some(b)) => routes.path(&a, &b),
            _ => None,
        };
        if let Some(path) = path {
            route_edge(work, &edge, &path, &mut out);
            continue;
        }
        let name = |index: u32| work.graph.nodes[index as usize].doc.id.clone();
        let note = (numeric(&from_ty) && numeric(&to_ty)).then_some(NUMERIC_NOTE);
        work.errors
            .push(SolveError::Convert(ConvertError::ConverterMissing {
                from_node: name(edge.from.node),
                from_port: edge.from.port.clone(),
                to_node: name(edge.to.node),
                to_port: edge.to.port.clone(),
                from_type: display(&from_ty),
                to_type: display(&to_ty),
                note,
            }));
        out.push(edge);
    }
    work.graph.edges = out;
}
