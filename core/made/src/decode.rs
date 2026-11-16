//! Reads a made plan written by `encode`. Every count is checked against the bytes left, every tag
//! against the tags this format defines, and a type's `'static` parts come from a table that keeps
//! each distinct one once. Bytes that don't parse give an error, never a panic.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::string::String;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::vec::Vec;

use daedalus_backend::{Decode, OutputSharing, PortRef, RateId, UnitInfo};
use daedalus_graph::{EdgePolicyDoc, Lit, LitField};
use daedalus_plan::{PlanData, Span, StepRecord};
use daedalus_types::{Field, NodeIndex, PortIndex, TypeExpr, TypeKey, Variant, fnv1a64};
use daedalus_wire::WireError;
use daedalus_wire::codec::Reader;

use crate::encode::{FORMAT, HEADER, MAGIC, SCALARS, kind_of, sharing_of};
use crate::error::MadeError;
use crate::facts::{MadeEdge, MadeNode, PlanFacts};
use crate::plan::{MadeMove, MadePlan, MadeRegion};
use crate::recipe::{
    RecipeBody, RecipeConst, RecipeEdge, RecipeHost, RecipeIncoming, RecipeLent, RegionRecipe,
};
use crate::stitched::{HostPort, Item, SlotInfo, Stitched, TransferUse, UnitId};

/// Reads a made plan. Bytes that were changed or cut short are refused by the hash or a field.
pub fn decode(bytes: &[u8]) -> Result<MadePlan, MadeError> {
    if bytes.len() < HEADER || bytes[..MAGIC.len()] != MAGIC[..] {
        return Err(MadeError::NotAPlan);
    }
    let format = u16::from_le_bytes([bytes[8], bytes[9]]);
    if format != FORMAT {
        return Err(MadeError::Format(format));
    }
    let mut hash = [0u8; 8];
    hash.copy_from_slice(&bytes[10..HEADER]);
    let body = &bytes[HEADER..];
    if fnv1a64(body) != u64::from_le_bytes(hash) {
        return Err(MadeError::Corrupt);
    }
    let mut reader = Reader::new(body);
    let plan = reader.record(read_plan).map_err(MadeError::Wire)?;
    reader.finish().map_err(MadeError::Wire)?;
    Ok(plan)
}

fn bad(what: &'static str) -> WireError {
    WireError::BadTag { what, tag: 0 }
}

fn read_plan(r: &mut Reader<'_>) -> Result<MadePlan, WireError> {
    let facts = r.record(read_facts)?;
    let backends = read_strs(r)?;
    let count = r.count()?;
    let mut regions = Vec::with_capacity(count);
    for _ in 0..count {
        regions.push(r.record(read_region)?);
    }
    let count = r.count()?;
    let mut moves = Vec::with_capacity(count);
    for _ in 0..count {
        moves.push(r.record(read_move)?);
    }
    Ok(MadePlan {
        facts,
        backends,
        regions,
        moves,
    })
}

fn read_facts(r: &mut Reader<'_>) -> Result<PlanFacts, WireError> {
    let count = r.count()?;
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(r.record(read_node)?);
    }
    let count = r.count()?;
    let mut edges = Vec::with_capacity(count);
    for _ in 0..count {
        edges.push(r.record(read_edge)?);
    }
    let count = r.count()?;
    let mut types = Vec::with_capacity(count);
    for _ in 0..count {
        types.push(read_type(r, 0)?);
    }
    let stitched = r.record(read_stitched)?;
    Ok(PlanFacts {
        nodes,
        edges,
        types,
        stitched,
    })
}

fn read_node(r: &mut Reader<'_>) -> Result<MadeNode, WireError> {
    Ok(MadeNode {
        id: r.str()?,
        label: read_opt_str(r)?,
        bridge: r.bool()?,
        metadata: read_lits(r)?,
        fire_all: r.bool()?,
        required: read_strs(r)?,
        reusable: r.bool()?,
        inputs: read_strs(r)?,
        consts: read_lits(r)?,
        backend: {
            let has = r.bool()?;
            let index = r.u8()?;
            has.then_some(index)
        },
    })
}

fn read_edge(r: &mut Reader<'_>) -> Result<MadeEdge, WireError> {
    Ok(MadeEdge {
        to_node: r.u32()?,
        to_port: r.str()?,
        policy: read_policy(r)?,
    })
}

fn read_stitched(r: &mut Reader<'_>) -> Result<Stitched, WireError> {
    let count = r.count()?;
    let mut layout = Vec::with_capacity(count);
    for _ in 0..count {
        layout.push(read_item(r)?);
    }
    let count = r.count()?;
    let mut slots = Vec::with_capacity(count);
    for _ in 0..count {
        let space = r.u32()?;
        let has = r.bool()?;
        let ty = r.u32()?;
        let consumers = r.u32()?;
        slots.push(SlotInfo {
            space,
            ty: has.then_some(ty),
            consumers,
        });
    }
    let spaces = read_strs(r)?;
    let count = r.count()?;
    let mut transfers = Vec::with_capacity(count);
    for _ in 0..count {
        let table = usize::try_from(r.u64()?).map_err(|_| bad("transfer table"))?;
        transfers.push(TransferUse {
            table,
            id: r.str()?,
        });
    }
    let host_inputs = read_ports(r)?;
    let host_outputs = read_ports(r)?;
    let plan = read_plan_data(r)?;
    Ok(Stitched {
        layout,
        slots,
        spaces,
        transfers,
        host_inputs,
        host_outputs,
        plan,
    })
}

fn read_item(r: &mut Reader<'_>) -> Result<Item, WireError> {
    Ok(match r.u8()? {
        0 => Item::Run {
            unit: read_unit(r)?,
            covers: read_u32s(r)?,
            reads: read_u32s(r)?,
            writes: read_u32s(r)?,
        },
        1 => Item::Move {
            from_slot: r.u32()?,
            to_slot: r.u32()?,
            transfer: r.u32()?,
        },
        2 => Item::Wait {
            unit: read_unit(r)?,
        },
        tag => {
            return Err(WireError::BadTag {
                what: "layout item",
                tag: u32::from(tag),
            });
        }
    })
}

fn read_unit(r: &mut Reader<'_>) -> Result<UnitId, WireError> {
    Ok(UnitId {
        region: r.u32()?,
        unit: r.u32()?,
    })
}

fn read_ports(r: &mut Reader<'_>) -> Result<Vec<HostPort>, WireError> {
    let count = r.count()?;
    let mut ports = Vec::with_capacity(count);
    for _ in 0..count {
        ports.push(HostPort {
            node: r.u32()?,
            port: r.str()?,
            slot: r.u32()?,
        });
    }
    Ok(ports)
}

fn read_plan_data(r: &mut Reader<'_>) -> Result<PlanData, WireError> {
    let count = r.count()?;
    let mut steps = Vec::with_capacity(count);
    for _ in 0..count {
        let tag = r.u8()?;
        let kind = kind_of(tag).ok_or(WireError::BadTag {
            what: "step kind",
            tag: u32::from(tag),
        })?;
        steps.push(StepRecord {
            kind,
            body: r.u32()?,
            covers: read_span(r)?,
            reads: read_span(r)?,
            writes: read_span(r)?,
        });
    }
    Ok(PlanData {
        steps,
        nodes: read_u32s(r)?,
        slots: read_u32s(r)?,
        spaces: read_u32s(r)?,
        types: read_u32s(r)?,
        bodies: read_u32s(r)?,
        extra: read_u32s(r)?,
    })
}

fn read_span(r: &mut Reader<'_>) -> Result<Span, WireError> {
    Ok(Span {
        start: r.u32()?,
        len: r.u32()?,
    })
}

fn read_region(r: &mut Reader<'_>) -> Result<MadeRegion, WireError> {
    let backend = r.u8()?;
    let recipe = r.record(read_recipe)?;
    let count = r.count()?;
    let mut units = Vec::with_capacity(count);
    for _ in 0..count {
        units.push(r.record(read_unit_info)?);
    }
    Ok(MadeRegion {
        backend,
        recipe,
        units,
        decl_hash: r.u64()?,
    })
}

fn read_recipe(r: &mut Reader<'_>) -> Result<RegionRecipe, WireError> {
    let backend = r.str()?;
    let nodes = read_u32s(r)?;
    let inputs = read_port_refs(r)?;
    let outputs = read_port_refs(r)?;
    let rate = RateId(r.u32()?);
    let count = r.count()?;
    let mut incoming = Vec::with_capacity(count);
    for _ in 0..count {
        incoming.push(RecipeIncoming {
            input: read_port(r)?,
            from: r.str()?,
            to: r.str()?,
            transfer: r.str()?,
        });
    }
    let placed = read_strs(r)?;
    let read_in = read_strs(r)?;
    let count = r.count()?;
    let mut consts = Vec::with_capacity(count);
    for _ in 0..count {
        consts.push(RecipeConst {
            node: r.u32()?,
            port: r.str()?,
            value: read_lit(r, 0)?,
        });
    }
    let count = r.count()?;
    let mut bodies = Vec::with_capacity(count);
    for _ in 0..count {
        bodies.push(r.record(read_body)?);
    }
    let count = r.count()?;
    let mut edges = Vec::with_capacity(count);
    for _ in 0..count {
        edges.push(RecipeEdge {
            from: read_port(r)?,
            to: read_port(r)?,
            queued: r.bool()?,
            policy: read_policy(r)?,
        });
    }
    let host_inputs = read_hosts(r)?;
    let host_outputs = read_hosts(r)?;
    let count = r.count()?;
    let mut sharing = Vec::with_capacity(count);
    for _ in 0..count {
        let port = read_port(r)?;
        let tag = r.u8()?;
        let sharing_at = sharing_of(tag).ok_or(WireError::BadTag {
            what: "sharing",
            tag: u32::from(tag),
        })?;
        sharing.push(OutputSharing {
            port,
            sharing: sharing_at,
        });
    }
    let count = r.count()?;
    let mut lent = Vec::with_capacity(count);
    for _ in 0..count {
        lent.push(RecipeLent {
            port: read_port(r)?,
            space: r.str()?,
            offering: r.str()?,
            readers: read_strs(r)?,
        });
    }
    Ok(RegionRecipe {
        backend,
        nodes,
        inputs,
        outputs,
        rate,
        incoming,
        placed,
        read_in,
        consts,
        bodies,
        edges,
        host_inputs,
        host_outputs,
        sharing,
        lent,
        fifo_capacity: r.u32()?,
        merge_allowed: r.bool()?,
        freeze_params: r.bool()?,
    })
}

fn read_body(r: &mut Reader<'_>) -> Result<RecipeBody, WireError> {
    Ok(RecipeBody {
        node: r.u32()?,
        id: r.str()?,
        backend: r.str()?,
        inputs: r.u16()?,
        outputs: r.u16()?,
        merges: r.bool()?,
        fire_all: r.bool()?,
        label: read_opt_str(r)?,
        metadata: read_lits(r)?,
    })
}

fn read_hosts(r: &mut Reader<'_>) -> Result<Vec<RecipeHost>, WireError> {
    let count = r.count()?;
    let mut hosts = Vec::with_capacity(count);
    for _ in 0..count {
        hosts.push(RecipeHost {
            port: read_port(r)?,
            name: r.str()?,
            policy: read_policy(r)?,
        });
    }
    Ok(hosts)
}

fn read_move(r: &mut Reader<'_>) -> Result<MadeMove, WireError> {
    Ok(match r.u8()? {
        0 => MadeMove::Backend {
            id: r.str()?,
            backend: r.str()?,
            index: r.u32()?,
        },
        1 => MadeMove::Record {
            id: r.str()?,
            backend: r.str()?,
            key: type_key(&r.str()?)?,
            from: r.str()?,
            to: r.str()?,
        },
        tag => {
            return Err(WireError::BadTag {
                what: "move",
                tag: u32::from(tag),
            });
        }
    })
}

fn read_unit_info(r: &mut Reader<'_>) -> Result<UnitInfo, WireError> {
    let covers = read_u32s(r)?.into_iter().map(NodeIndex).collect();
    let reads = read_port_refs(r)?;
    let writes = read_port_refs(r)?;
    let absorbed = read_u32s(r)?;
    let may_pend = r.bool()?;
    let count = r.count()?;
    let mut decodes = Vec::with_capacity(count);
    for _ in 0..count {
        decodes.push(Decode {
            input: read_port(r)?,
            ty: Cow::Owned(r.str()?),
            reason: Cow::Owned(r.str()?),
        });
    }
    Ok(UnitInfo {
        covers,
        reads,
        writes,
        absorbed,
        may_pend,
        decodes,
    })
}

fn read_port(r: &mut Reader<'_>) -> Result<PortRef, WireError> {
    Ok(PortRef {
        node: NodeIndex(r.u32()?),
        port: PortIndex(r.u16()?),
    })
}

fn read_port_refs(r: &mut Reader<'_>) -> Result<Vec<PortRef>, WireError> {
    let count = r.count()?;
    let mut ports = Vec::with_capacity(count);
    for _ in 0..count {
        ports.push(read_port(r)?);
    }
    Ok(ports)
}

fn read_u32s(r: &mut Reader<'_>) -> Result<Vec<u32>, WireError> {
    let count = r.count()?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(r.u32()?);
    }
    Ok(values)
}

fn read_strs(r: &mut Reader<'_>) -> Result<Vec<String>, WireError> {
    let count = r.count()?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(r.str()?);
    }
    Ok(values)
}

fn read_opt_str(r: &mut Reader<'_>) -> Result<Option<String>, WireError> {
    if r.bool()? {
        Ok(Some(r.str()?))
    } else {
        Ok(None)
    }
}

fn read_lits(r: &mut Reader<'_>) -> Result<BTreeMap<String, Lit>, WireError> {
    let count = r.count()?;
    let mut map = BTreeMap::new();
    for _ in 0..count {
        let key = r.str()?;
        let value = read_lit(r, 0)?;
        map.insert(key, value);
    }
    Ok(map)
}

fn read_lit(r: &mut Reader<'_>, depth: u32) -> Result<Lit, WireError> {
    if depth > crate::encode::MAX_DEPTH {
        return Err(WireError::TooDeep);
    }
    Ok(match r.u8()? {
        0 => Lit::Unit,
        1 => Lit::Bool(r.bool()?),
        2 => Lit::Int(r.i64()?),
        3 => Lit::Float(r.f64()?),
        4 => Lit::Str(r.str()?),
        5 => Lit::Bytes(r.bytes()?),
        6 => {
            let count = r.count()?;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(read_lit(r, depth + 1)?);
            }
            Lit::List(items)
        }
        7 => {
            let count = r.count()?;
            let mut fields = Vec::with_capacity(count);
            for _ in 0..count {
                let name = r.str()?;
                let value = read_lit(r, depth + 1)?;
                fields.push(LitField { name, value });
            }
            Lit::Struct(fields)
        }
        8 => {
            let name = r.str()?;
            let payload = if r.bool()? {
                Some(Box::new(read_lit(r, depth + 1)?))
            } else {
                None
            };
            Lit::Enum { name, payload }
        }
        tag => {
            return Err(WireError::BadTag {
                what: "constant",
                tag: u32::from(tag),
            });
        }
    })
}

fn read_policy(r: &mut Reader<'_>) -> Result<EdgePolicyDoc, WireError> {
    Ok(match r.u8()? {
        0 => EdgePolicyDoc::Fifo,
        1 => EdgePolicyDoc::LatestOnly,
        2 => EdgePolicyDoc::Bounded {
            capacity: r.u32()?,
            overflow: r.str()?,
        },
        tag => {
            return Err(WireError::BadTag {
                what: "edge policy",
                tag: u32::from(tag),
            });
        }
    })
}

fn read_type(r: &mut Reader<'_>, depth: u32) -> Result<TypeExpr, WireError> {
    if depth > crate::encode::MAX_DEPTH {
        return Err(WireError::TooDeep);
    }
    Ok(match r.u8()? {
        0 => {
            let index = usize::from(r.u8()?);
            let scalar = *SCALARS.get(index).ok_or(WireError::BadTag {
                what: "scalar",
                tag: index as u32,
            })?;
            TypeExpr::Scalar(scalar)
        }
        1 => TypeExpr::Opaque(type_key(&r.str()?)?),
        2 => TypeExpr::Optional(expr(read_type(r, depth + 1)?)),
        3 => TypeExpr::List(expr(read_type(r, depth + 1)?)),
        4 => {
            let key = read_type(r, depth + 1)?;
            let value = read_type(r, depth + 1)?;
            TypeExpr::Map(expr(key), expr(value))
        }
        5 => {
            let count = r.count()?;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(read_type(r, depth + 1)?);
            }
            TypeExpr::Tuple(exprs(items))
        }
        6 => {
            let count = r.count()?;
            let mut fields = Vec::with_capacity(count);
            for _ in 0..count {
                let name = text(&r.str()?);
                let ty = read_type(r, depth + 1)?;
                fields.push(Field { name, ty });
            }
            TypeExpr::Struct(fields_of(fields))
        }
        7 => {
            let count = r.count()?;
            let mut variants = Vec::with_capacity(count);
            for _ in 0..count {
                let name = text(&r.str()?);
                let payload = if r.bool()? {
                    Some(read_type(r, depth + 1)?)
                } else {
                    None
                };
                variants.push(Variant { name, payload });
            }
            TypeExpr::Enum(variants_of(variants))
        }
        tag => {
            return Err(WireError::BadTag {
                what: "type",
                tag: u32::from(tag),
            });
        }
    })
}

fn type_key(text: &str) -> Result<TypeKey, WireError> {
    TypeKey::shared(text).map_err(|_| bad("type key"))
}

/// A table of the `'static` values a type needs, each kept once for the life of the process. A
/// value is keyed by its text, so a plan loaded again adds nothing.
struct Table<V: ?Sized + 'static>(OnceLock<Mutex<HashMap<String, &'static V>>>);

impl<V: ?Sized> Table<V> {
    const fn new() -> Table<V> {
        Table(OnceLock::new())
    }

    fn get(&'static self, key: String, make: impl FnOnce() -> &'static V) -> &'static V {
        let map = self.0.get_or_init(Mutex::default);
        let mut map = map.lock().unwrap_or_else(PoisonError::into_inner);
        map.entry(key).or_insert_with(make)
    }
}

static TEXT: Table<str> = Table::new();
static EXPR: Table<TypeExpr> = Table::new();
static EXPRS: Table<[TypeExpr]> = Table::new();
static FIELDS: Table<[Field]> = Table::new();
static VARIANTS: Table<[Variant]> = Table::new();

fn text(value: &str) -> &'static str {
    TEXT.get(String::from(value), || String::from(value).leak())
}

fn expr(ty: TypeExpr) -> &'static TypeExpr {
    EXPR.get(std::format!("{ty:?}"), || {
        std::boxed::Box::leak(std::boxed::Box::new(ty))
    })
}

fn exprs(list: Vec<TypeExpr>) -> &'static [TypeExpr] {
    EXPRS.get(std::format!("{list:?}"), || list.leak())
}

fn fields_of(list: Vec<Field>) -> &'static [Field] {
    FIELDS.get(std::format!("{list:?}"), || list.leak())
}

fn variants_of(list: Vec<Variant>) -> &'static [Variant] {
    VARIANTS.get(std::format!("{list:?}"), || list.leak())
}
