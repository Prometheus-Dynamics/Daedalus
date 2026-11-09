//! Unifies generic ports over the whole graph and checks every edge.

use std::collections::BTreeMap;
use std::fmt;

use daedalus_registry::TypeRef;

use super::port_types::{display, generic_var, same};
use super::{Port, Work};
use crate::pipeline::SolveError;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TypeCheckError {
    TypeMismatch {
        from_node: String,
        from_port: String,
        to_node: String,
        to_port: String,
        from_type: String,
        to_type: String,
    },
}

impl fmt::Display for TypeCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeCheckError::TypeMismatch {
                from_node,
                from_port,
                to_node,
                to_port,
                from_type,
                to_type,
            } => write!(
                f,
                "`{from_node}`.{from_port} is `{from_type}` but `{to_node}`.{to_port} takes `{to_type}`"
            ),
        }
    }
}

impl std::error::Error for TypeCheckError {}

#[derive(Default)]
struct Sets {
    parent: Vec<usize>,
    bound: Vec<Option<TypeRef>>,
}

impl Sets {
    fn add(&mut self, bound: Option<TypeRef>) -> usize {
        self.parent.push(self.parent.len());
        self.bound.push(bound);
        self.parent.len() - 1
    }

    fn find(&mut self, mut at: usize) -> usize {
        while self.parent[at] != at {
            self.parent[at] = self.parent[self.parent[at]];
            at = self.parent[at];
        }
        at
    }

    /// Joins two sets. On a conflict they stay apart and both types come back.
    fn union(&mut self, a: usize, b: usize) -> Result<(), (TypeRef, TypeRef)> {
        let (a, b) = (self.find(a), self.find(b));
        if a == b {
            return Ok(());
        }
        if let (Some(x), Some(y)) = (&self.bound[a], &self.bound[b])
            && !same(x, y)
        {
            return Err((x.clone(), y.clone()));
        }
        let bound = self.bound[a].take().or_else(|| self.bound[b].take());
        self.parent[b] = a;
        self.bound[a] = bound;
        Ok(())
    }
}

type PortKey = (u32, bool, String);
/// The set a port is in, and whether the port is generic.
type PortSlot = (usize, bool);

fn add_ports(
    sets: &mut Sets,
    index: u32,
    is_input: bool,
    ports: &[Port],
    named: &mut BTreeMap<(u32, String), usize>,
    out: &mut BTreeMap<PortKey, PortSlot>,
) {
    for port in ports {
        let set = match generic_var(&port.ty) {
            Some(Some(name)) => *named
                .entry((index, name.to_string()))
                .or_insert_with(|| sets.add(None)),
            Some(None) => sets.add(None),
            None => sets.add(Some(port.ty.clone())),
        };
        out.insert((index, is_input, port.name.clone()), (set, port.generic));
    }
}

pub fn run(work: &mut Work<'_>) {
    let mut sets = Sets::default();
    let mut named = BTreeMap::new();
    let mut slots = BTreeMap::new();
    for (index, node) in work.graph.nodes.iter().enumerate() {
        add_ports(
            &mut sets,
            index as u32,
            true,
            &node.inputs,
            &mut named,
            &mut slots,
        );
        add_ports(
            &mut sets,
            index as u32,
            false,
            &node.outputs,
            &mut named,
            &mut slots,
        );
    }
    for edge in &work.graph.edges {
        let from = slots.get(&(edge.from.node, false, edge.from.port.clone()));
        let to = slots.get(&(edge.to.node, true, edge.to.port.clone()));
        let (Some(&(from, from_generic)), Some(&(to, to_generic))) = (from, to) else {
            continue; // The port stage already reported it.
        };
        if !from_generic && !to_generic {
            continue; // Two declared types: a conversion's business.
        }
        if let Err((from_type, to_type)) = sets.union(from, to) {
            let name = |index: u32| work.graph.nodes[index as usize].doc.id.clone();
            work.errors
                .push(SolveError::Types(TypeCheckError::TypeMismatch {
                    from_node: name(edge.from.node),
                    from_port: edge.from.port.clone(),
                    to_node: name(edge.to.node),
                    to_port: edge.to.port.clone(),
                    from_type: display(&from_type),
                    to_type: display(&to_type),
                }));
        }
    }
    for (index, node) in work.graph.nodes.iter_mut().enumerate() {
        for (is_input, ports) in [(true, &mut node.inputs), (false, &mut node.outputs)] {
            for port in ports.iter_mut() {
                let key = (index as u32, is_input, port.name.clone());
                if let Some(&(set, _)) = slots.get(&key) {
                    let root = sets.find(set);
                    if let Some(bound) = &sets.bound[root] {
                        port.ty = bound.clone();
                    }
                }
            }
        }
    }
}
