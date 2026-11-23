//! The made form as bytes: a magic, a format number, an FNV hash of the body, then the body as one
//! record. Each field is little-endian, and a count is checked against the bytes left when it is read.

use std::collections::BTreeMap;

use daedalus_backend::{Decode, PortRef, Sharing, UnitInfo};
use daedalus_graph::{EdgePolicyDoc, HostInputPolicy, Lit, LitField};
use daedalus_plan::{PlanData, Span, StepKind, StepRecord};
use daedalus_types::{Scalar, TypeExpr, fnv1a64};
use daedalus_wire::WireError;
use daedalus_wire::codec::Writer;

use crate::error::MadeError;
use crate::facts::{MadeEdge, MadeNode, PlanFacts};
use crate::plan::{MadeMove, MadePlan, MadeRegion};
use crate::recipe::{RecipeBody, RecipeHost, RegionRecipe};
use crate::stitched::{HostPort, Item, Stitched, UnitId};

/// The first bytes of a made plan.
pub const MAGIC: &[u8; 8] = b"DAEDMADE";
/// The format this build writes and reads. A new layout takes a new number.
pub const FORMAT: u16 = 1;
/// Bytes before the body: the magic, the format and the hash.
pub const HEADER: usize = MAGIC.len() + 2 + 8;

/// Writes a made plan.
pub fn encode(plan: &MadePlan) -> Result<Vec<u8>, MadeError> {
    let mut body = Writer::new();
    body.record(|w| write_plan(w, plan))
        .map_err(MadeError::Wire)?;
    let mut out = Vec::with_capacity(HEADER + body.buf.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&FORMAT.to_le_bytes());
    out.extend_from_slice(&fnv1a64(&body.buf).to_le_bytes());
    out.extend_from_slice(&body.buf);
    Ok(out)
}

type Out<'w> = &'w mut Writer;

fn write_plan(w: Out<'_>, plan: &MadePlan) -> Result<(), WireError> {
    w.record(|w| write_facts(w, &plan.facts))?;
    w.len(plan.backends.len())?;
    for backend in &plan.backends {
        w.str(backend)?;
    }
    w.len(plan.regions.len())?;
    for region in &plan.regions {
        w.record(|w| write_region(w, region))?;
    }
    w.len(plan.moves.len())?;
    for mv in &plan.moves {
        w.record(|w| write_move(w, mv))?;
    }
    Ok(())
}

fn write_facts(w: Out<'_>, facts: &PlanFacts) -> Result<(), WireError> {
    w.len(facts.nodes.len())?;
    for node in &facts.nodes {
        w.record(|w| write_node(w, node))?;
    }
    w.len(facts.edges.len())?;
    for edge in &facts.edges {
        w.record(|w| write_edge(w, edge))?;
    }
    w.len(facts.types.len())?;
    for ty in &facts.types {
        write_type(w, ty, 0)?;
    }
    w.record(|w| write_stitched(w, &facts.stitched))
}

pub(crate) fn write_node(w: Out<'_>, node: &MadeNode) -> Result<(), WireError> {
    write_node_as_first_stored(w, node)?;
    // Added at the record's end, so a plan stored before it still reads (`read_node`).
    write_strs(w, &node.outputs)
}

/// The record as plans were first stored, before the output names.
pub(crate) fn write_node_as_first_stored(w: Out<'_>, node: &MadeNode) -> Result<(), WireError> {
    w.str(&node.id)?;
    write_opt_str(w, node.label.as_deref())?;
    w.bool(node.bridge);
    write_lits(w, &node.metadata)?;
    w.bool(node.fire_all);
    write_strs(w, &node.required)?;
    w.bool(node.reusable);
    write_strs(w, &node.inputs)?;
    write_lits(w, &node.consts)?;
    w.bool(node.backend.is_some());
    w.u8(node.backend.unwrap_or(0));
    Ok(())
}

fn write_edge(w: Out<'_>, edge: &MadeEdge) -> Result<(), WireError> {
    w.u32(edge.to_node);
    w.str(&edge.to_port)?;
    write_policy(w, &edge.policy)
}

pub(crate) fn write_stitched(w: Out<'_>, stitched: &Stitched) -> Result<(), WireError> {
    write_stitched_as_first_stored(w, stitched)?;
    // Added at the record's end, so a plan stored before it still reads (`read_stitched`).
    write_input_policies(w, &stitched.host_inputs)
}

/// The record as plans were first stored, before the host inputs' policies.
pub(crate) fn write_stitched_as_first_stored(
    w: Out<'_>,
    stitched: &Stitched,
) -> Result<(), WireError> {
    w.len(stitched.layout.len())?;
    for item in &stitched.layout {
        write_item(w, item)?;
    }
    w.len(stitched.slots.len())?;
    for slot in &stitched.slots {
        w.u32(slot.space);
        w.bool(slot.ty.is_some());
        w.u32(slot.ty.unwrap_or(0));
        w.u32(slot.consumers);
    }
    write_strs(w, &stitched.spaces)?;
    w.len(stitched.transfers.len())?;
    for used in &stitched.transfers {
        w.u64(used.table as u64);
        w.str(&used.id)?;
    }
    write_ports(w, &stitched.host_inputs)?;
    write_ports(w, &stitched.host_outputs)?;
    write_plan_data(w, &stitched.plan)
}

/// One tag a host input, in `host_inputs` order.
pub(crate) fn write_input_policies(w: Out<'_>, inputs: &[HostPort]) -> Result<(), WireError> {
    w.len(inputs.len())?;
    for port in inputs {
        w.u8(input_policy_tag(port.policy));
    }
    Ok(())
}

fn input_policy_tag(policy: HostInputPolicy) -> u8 {
    match policy {
        HostInputPolicy::Consume => 0,
        HostInputPolicy::Held => 1,
        HostInputPolicy::Shared => 2,
    }
}

fn write_item(w: Out<'_>, item: &Item) -> Result<(), WireError> {
    match item {
        Item::Run {
            unit,
            covers,
            reads,
            writes,
        } => {
            w.u8(0);
            write_unit(w, *unit);
            write_u32s(w, covers)?;
            write_u32s(w, reads)?;
            write_u32s(w, writes)
        }
        Item::Move {
            from_slot,
            to_slot,
            transfer,
        } => {
            w.u8(1);
            w.u32(*from_slot);
            w.u32(*to_slot);
            w.u32(*transfer);
            Ok(())
        }
        Item::Wait { unit } => {
            w.u8(2);
            write_unit(w, *unit);
            Ok(())
        }
    }
}

fn write_unit(w: Out<'_>, unit: UnitId) {
    w.u32(unit.region);
    w.u32(unit.unit);
}

fn write_ports(w: Out<'_>, ports: &[HostPort]) -> Result<(), WireError> {
    w.len(ports.len())?;
    for port in ports {
        w.u32(port.node);
        w.str(&port.port)?;
        w.u32(port.slot);
    }
    Ok(())
}

fn write_plan_data(w: Out<'_>, plan: &PlanData) -> Result<(), WireError> {
    w.len(plan.steps.len())?;
    for step in &plan.steps {
        write_step(w, step);
    }
    write_u32s(w, &plan.nodes)?;
    write_u32s(w, &plan.slots)?;
    write_u32s(w, &plan.spaces)?;
    write_u32s(w, &plan.types)?;
    write_u32s(w, &plan.bodies)?;
    write_u32s(w, &plan.extra)
}

fn write_step(w: Out<'_>, step: &StepRecord) {
    w.u8(step.kind as u8);
    w.u32(step.body);
    write_span(w, step.covers);
    write_span(w, step.reads);
    write_span(w, step.writes);
}

fn write_span(w: Out<'_>, span: Span) {
    w.u32(span.start);
    w.u32(span.len);
}

fn write_region(w: Out<'_>, region: &MadeRegion) -> Result<(), WireError> {
    w.u8(region.backend);
    w.record(|w| write_recipe(w, &region.recipe))?;
    w.len(region.units.len())?;
    for unit in &region.units {
        w.record(|w| write_unit_info(w, unit))?;
    }
    w.u64(region.decl_hash);
    Ok(())
}

fn write_recipe(w: Out<'_>, recipe: &RegionRecipe) -> Result<(), WireError> {
    w.str(&recipe.backend)?;
    write_u32s(w, &recipe.nodes)?;
    write_ports_ref(w, &recipe.inputs)?;
    write_ports_ref(w, &recipe.outputs)?;
    w.u32(recipe.rate.0);
    w.len(recipe.incoming.len())?;
    for incoming in &recipe.incoming {
        write_port(w, incoming.input);
        w.str(&incoming.from)?;
        w.str(&incoming.to)?;
        w.str(&incoming.transfer)?;
    }
    write_strs(w, &recipe.placed)?;
    write_strs(w, &recipe.read_in)?;
    w.len(recipe.consts.len())?;
    for of in &recipe.consts {
        w.u32(of.node);
        w.str(&of.port)?;
        write_lit(w, &of.value, 0)?;
    }
    w.len(recipe.bodies.len())?;
    for body in &recipe.bodies {
        w.record(|w| write_body(w, body))?;
    }
    w.len(recipe.edges.len())?;
    for edge in &recipe.edges {
        write_port(w, edge.from);
        write_port(w, edge.to);
        w.bool(edge.queued);
        write_policy(w, &edge.policy)?;
    }
    write_hosts(w, &recipe.host_inputs)?;
    write_hosts(w, &recipe.host_outputs)?;
    w.len(recipe.sharing.len())?;
    for output in &recipe.sharing {
        write_port(w, output.port);
        w.u8(match output.sharing {
            Sharing::Fresh => 0,
            Sharing::Readers => 1,
            Sharing::Taker => 2,
        });
    }
    w.len(recipe.lent.len())?;
    for lent in &recipe.lent {
        write_port(w, lent.port);
        w.str(&lent.space)?;
        w.str(&lent.offering)?;
        write_strs(w, &lent.readers)?;
    }
    w.u32(recipe.fifo_capacity);
    w.bool(recipe.merge_allowed);
    w.bool(recipe.freeze_params);
    Ok(())
}

fn write_body(w: Out<'_>, body: &RecipeBody) -> Result<(), WireError> {
    w.u32(body.node);
    w.str(&body.id)?;
    w.str(&body.backend)?;
    w.u16(body.inputs);
    w.u16(body.outputs);
    w.bool(body.merges);
    w.bool(body.fire_all);
    write_opt_str(w, body.label.as_deref())?;
    write_lits(w, &body.metadata)
}

fn write_hosts(w: Out<'_>, hosts: &[RecipeHost]) -> Result<(), WireError> {
    w.len(hosts.len())?;
    for host in hosts {
        write_port(w, host.port);
        w.str(&host.name)?;
        write_policy(w, &host.policy)?;
    }
    Ok(())
}

fn write_move(w: Out<'_>, mv: &MadeMove) -> Result<(), WireError> {
    match mv {
        MadeMove::Backend { id, backend, index } => {
            w.u8(0);
            w.str(id)?;
            w.str(backend)?;
            w.u32(*index);
        }
        MadeMove::Record {
            id,
            backend,
            key,
            from,
            to,
        } => {
            w.u8(1);
            w.str(id)?;
            w.str(backend)?;
            w.str(key.as_str())?;
            w.str(from)?;
            w.str(to)?;
        }
    }
    Ok(())
}

fn write_unit_info(w: Out<'_>, unit: &UnitInfo) -> Result<(), WireError> {
    let covers: Vec<u32> = unit.covers.iter().map(|node| node.0).collect();
    write_u32s(w, &covers)?;
    write_ports_ref(w, &unit.reads)?;
    write_ports_ref(w, &unit.writes)?;
    write_u32s(w, &unit.absorbed)?;
    w.bool(unit.may_pend);
    w.len(unit.decodes.len())?;
    for decode in &unit.decodes {
        write_decode(w, decode)?;
    }
    Ok(())
}

fn write_decode(w: Out<'_>, decode: &Decode) -> Result<(), WireError> {
    write_port(w, decode.input);
    w.str(&decode.ty)?;
    w.str(&decode.reason)
}

fn write_port(w: Out<'_>, port: PortRef) {
    w.u32(port.node.0);
    w.u16(port.port.0);
}

fn write_ports_ref(w: Out<'_>, ports: &[PortRef]) -> Result<(), WireError> {
    w.len(ports.len())?;
    for port in ports {
        write_port(w, *port);
    }
    Ok(())
}

fn write_u32s(w: Out<'_>, values: &[u32]) -> Result<(), WireError> {
    w.len(values.len())?;
    for value in values {
        w.u32(*value);
    }
    Ok(())
}

fn write_strs(w: Out<'_>, values: &[String]) -> Result<(), WireError> {
    w.len(values.len())?;
    for value in values {
        w.str(value)?;
    }
    Ok(())
}

fn write_opt_str(w: Out<'_>, value: Option<&str>) -> Result<(), WireError> {
    w.bool(value.is_some());
    match value {
        Some(text) => w.str(text),
        None => Ok(()),
    }
}

fn write_lits(w: Out<'_>, map: &BTreeMap<String, Lit>) -> Result<(), WireError> {
    w.len(map.len())?;
    for (key, value) in map {
        w.str(key)?;
        write_lit(w, value, 0)?;
    }
    Ok(())
}

/// A literal's nesting is written out in full. A reader stops at `MAX_DEPTH`, so corrupt bytes
/// can't recurse without end.
pub(crate) const MAX_DEPTH: u32 = 32;

fn write_lit(w: Out<'_>, lit: &Lit, depth: u32) -> Result<(), WireError> {
    if depth > MAX_DEPTH {
        return Err(WireError::TooDeep);
    }
    match lit {
        Lit::Unit => w.u8(0),
        Lit::Bool(value) => {
            w.u8(1);
            w.bool(*value);
        }
        Lit::Int(value) => {
            w.u8(2);
            w.i64(*value);
        }
        Lit::Float(value) => {
            w.u8(3);
            w.f64(*value);
        }
        Lit::Str(text) => {
            w.u8(4);
            w.str(text)?;
        }
        Lit::Bytes(bytes) => {
            w.u8(5);
            w.bytes(bytes)?;
        }
        Lit::List(items) => {
            w.u8(6);
            w.len(items.len())?;
            for item in items {
                write_lit(w, item, depth + 1)?;
            }
        }
        Lit::Struct(fields) => {
            w.u8(7);
            w.len(fields.len())?;
            for LitField { name, value } in fields {
                w.str(name)?;
                write_lit(w, value, depth + 1)?;
            }
        }
        Lit::Enum { name, payload } => {
            w.u8(8);
            w.str(name)?;
            w.bool(payload.is_some());
            if let Some(payload) = payload {
                write_lit(w, payload, depth + 1)?;
            }
        }
    }
    Ok(())
}

fn write_policy(w: Out<'_>, policy: &EdgePolicyDoc) -> Result<(), WireError> {
    match policy {
        EdgePolicyDoc::Fifo => w.u8(0),
        EdgePolicyDoc::LatestOnly => w.u8(1),
        EdgePolicyDoc::Bounded { capacity, overflow } => {
            w.u8(2);
            w.u32(*capacity);
            w.str(overflow)?;
        }
        EdgePolicyDoc::PerTick => w.u8(3),
    }
    Ok(())
}

/// A type's tree. A scalar is its discriminant, and each composite is a tag and its parts.
fn write_type(w: Out<'_>, ty: &TypeExpr, depth: u32) -> Result<(), WireError> {
    if depth > MAX_DEPTH {
        return Err(WireError::TooDeep);
    }
    match ty {
        TypeExpr::Scalar(scalar) => {
            w.u8(0);
            w.u8(*scalar as u8);
        }
        TypeExpr::Opaque(key) => {
            w.u8(1);
            w.str(key.as_str())?;
        }
        TypeExpr::Optional(inner) => {
            w.u8(2);
            write_type(w, inner, depth + 1)?;
        }
        TypeExpr::List(inner) => {
            w.u8(3);
            write_type(w, inner, depth + 1)?;
        }
        TypeExpr::Map(key, value) => {
            w.u8(4);
            write_type(w, key, depth + 1)?;
            write_type(w, value, depth + 1)?;
        }
        TypeExpr::Tuple(items) => {
            w.u8(5);
            w.len(items.len())?;
            for item in items.iter() {
                write_type(w, item, depth + 1)?;
            }
        }
        TypeExpr::Struct(fields) => {
            w.u8(6);
            w.len(fields.len())?;
            for field in fields.iter() {
                w.str(field.name)?;
                write_type(w, &field.ty, depth + 1)?;
            }
        }
        TypeExpr::Enum(variants) => {
            w.u8(7);
            w.len(variants.len())?;
            for variant in variants.iter() {
                w.str(variant.name)?;
                w.bool(variant.payload.is_some());
                if let Some(payload) = &variant.payload {
                    write_type(w, payload, depth + 1)?;
                }
            }
        }
    }
    Ok(())
}

/// The scalars in discriminant order. A scalar is written as its discriminant.
pub(crate) const SCALARS: [Scalar; 16] = [
    Scalar::Unit,
    Scalar::Bool,
    Scalar::I8,
    Scalar::I16,
    Scalar::I32,
    Scalar::Int,
    Scalar::ISize,
    Scalar::U8,
    Scalar::U16,
    Scalar::U32,
    Scalar::U64,
    Scalar::USize,
    Scalar::F32,
    Scalar::Float,
    Scalar::String,
    Scalar::Bytes,
];

/// Sharing from its byte, the inverse of the writer's match.
pub(crate) fn sharing_of(tag: u8) -> Option<Sharing> {
    Some(match tag {
        0 => Sharing::Fresh,
        1 => Sharing::Readers,
        2 => Sharing::Taker,
        _ => return None,
    })
}

/// A step kind from its byte, the inverse of `kind as u8`.
pub(crate) fn kind_of(tag: u8) -> Option<StepKind> {
    Some(match tag {
        0 => StepKind::Call,
        1 => StepKind::Transfer,
        2 => StepKind::Wait,
        3 => StepKind::If,
        4 => StepKind::ForEach,
        5 => StepKind::Until,
        _ => return None,
    })
}
