//! Compiles one region and checks what came back.

use std::collections::BTreeSet;

use daedalus_backend::{
    Backend, CompileCx, Compiled, ConstOf, Incoming, NodeBody, PortRef, Refusal, Region, UnitInfo,
};
use daedalus_registry::Registry;
use daedalus_types::NodeIndex;

use super::Formed;
use super::RegionError;
use crate::assign::Assignment;
use crate::solve::Graph;
use crate::solve::type_table::TypeTable;

pub(super) struct Compiling<'a> {
    pub graph: &'a Graph,
    pub registry: &'a Registry,
    pub types: &'a TypeTable,
    pub assignment: &'a Assignment,
    pub merge_allowed: bool,
}

/// Runs the backend's compile for one region.
pub(super) fn compile_region(
    cx: &Compiling<'_>,
    formed: &Formed,
    backend: &dyn Backend,
    incoming: &[Incoming<'_>],
) -> Result<Compiled, Refusal> {
    let nodes: Vec<NodeIndex> = formed.nodes.iter().map(|&node| NodeIndex(node)).collect();
    let mut consts = Vec::new();
    let mut bodies = Vec::new();
    for &node in &formed.nodes {
        let solved = &cx.graph.nodes[node as usize];
        for (port, value) in &solved.consts {
            consts.push(ConstOf {
                node: NodeIndex(node),
                port,
                value,
            });
        }
        let entry = cx.registry.entry(&solved.doc.id);
        let body = cx.assignment.bodies[node as usize];
        if let (Some(entry), Some(body)) = (entry, body)
            && let Some(record) = entry.bodies.get(body.0 as usize)
        {
            bodies.push(NodeBody {
                node: NodeIndex(node),
                body,
                inputs: solved.inputs.len() as u16,
                outputs: solved.outputs.len() as u16,
                code: record.code,
                text: record.text,
            });
        }
    }
    let spaces = backend.memory();
    let region = Region {
        backend: formed.backend,
        nodes: &nodes,
        inputs: &formed.inputs,
        outputs: &formed.outputs,
        incoming,
        rate: formed.rate,
    };
    backend.compile(
        &region,
        &CompileCx {
            types: cx.types.exprs(),
            consts: &consts,
            spaces: &spaces,
            bodies: &bodies,
            merge_allowed: cx.merge_allowed,
        },
    )
}

pub(super) fn units(compiled: &Compiled) -> Vec<&UnitInfo> {
    match compiled {
        Compiled::Steps(steps) => steps.iter().map(|step| &step.info).collect(),
        Compiled::Blob { units, .. } | Compiled::Source { units, .. } => units.iter().collect(),
    }
}

fn set_of(ports: &[PortRef]) -> BTreeSet<PortRef> {
    ports.iter().copied().collect()
}

/// R4 and R5 on what a backend returned.
pub(super) fn check_contract(
    formed: &Formed,
    region: u32,
    backend: &str,
    compiled: &Compiled,
) -> Result<(), RegionError> {
    let units = units(compiled);
    let fail = |rule: &'static str, detail: String| RegionError::BackendContract {
        backend: backend.to_string(),
        region,
        rule,
        detail,
    };
    let wanted: BTreeSet<u32> = formed.nodes.iter().copied().collect();
    let covered: BTreeSet<u32> = units
        .iter()
        .flat_map(|unit| unit.covers.iter().map(|node| node.0))
        .collect();
    if covered != wanted {
        return Err(fail(
            "R5",
            format!("units cover {covered:?}, the region is {wanted:?}"),
        ));
    }
    let reads: BTreeSet<PortRef> = units.iter().flat_map(|u| u.reads.iter().copied()).collect();
    if !extends(&reads, &formed.inputs, &wanted) {
        return Err(fail(
            "R4",
            format!("reads {reads:?}, boundary inputs {:?}", formed.inputs),
        ));
    }
    let writes: BTreeSet<PortRef> = units
        .iter()
        .flat_map(|u| u.writes.iter().copied())
        .collect();
    if !extends(&writes, &formed.outputs, &wanted) {
        return Err(fail(
            "R4",
            format!("writes {writes:?}, boundary outputs {:?}", formed.outputs),
        ));
    }
    Ok(())
}

/// A unit must name every boundary port. It may also name ports inside its region, which is how
/// it asks for a slot on an internal edge or a place for a port with no edge.
fn extends(named: &BTreeSet<PortRef>, boundary: &[PortRef], nodes: &BTreeSet<u32>) -> bool {
    let boundary = set_of(boundary);
    boundary.is_subset(named) && named.iter().all(|port| nodes.contains(&port.node.0))
}
