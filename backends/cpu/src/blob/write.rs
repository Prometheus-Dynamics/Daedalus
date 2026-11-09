//! Storing a region for the small runner.

use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;

use daedalus_abi::NO_SLOT;
use daedalus_backend::{
    Backend, CompileCx, Compiled, HostBinding, MemorySpace, NodeRef, Offer, PortRef, Refusal,
    Region, Transfer, UnitInfo,
};
use daedalus_graph::{EdgePolicyDoc, Lit};
use daedalus_plan::{
    PlanData, PlanError, Span, StepKind, StepRecord, crc32, encode as encode_plan,
};
use daedalus_registry::{NodeDecl, TypeRef};
use daedalus_runner::ArenaBody;
use daedalus_types::NodeIndex;

use super::library::{Library, name_hash, type_hash};
use super::{CONST_BIT, Overflow, PORT_SHIFT};
use crate::backend::{BACKEND_ID, CpuBackend, body_of};
use crate::param_mark::{checked_spec, marks};
use crate::queue_shape::queue_shape;
use crate::scalar_kind::ScalarKind;

/// Where an input gets its value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Absent,
    Edge(u32),
    Const(u32),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepDesc {
    /// Position in the library.
    pub entry: u32,
    pub node: u32,
    pub inputs: Vec<Source>,
    /// (output port, edge).
    pub outputs: Vec<(u32, u32)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EdgeDesc {
    pub capacity: u32,
    pub overflow: Overflow,
    pub size: u32,
    pub key: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConstDesc {
    pub kind: ScalarKind,
    pub bits: u64,
}

/// A constant that may be changed while running, and the range it stays in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParamDesc {
    pub value: u32,
    pub min: u64,
    pub max: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostDesc {
    pub name: String,
    pub key: u32,
    /// The edges a host input feeds, or the one edge a host output reads.
    pub edges: Vec<u32>,
}

/// Everything a stored plan says.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlanDesc {
    pub library: u64,
    pub steps: Vec<StepDesc>,
    pub edges: Vec<EdgeDesc>,
    pub consts: Vec<ConstDesc>,
    pub params: Vec<ParamDesc>,
    pub host_inputs: Vec<HostDesc>,
    pub host_outputs: Vec<HostDesc>,
}

fn push_pair(words: &mut Vec<u32>, value: u64) {
    words.push(value as u32);
    words.push((value >> 32) as u32);
}

fn span(start: usize, len: usize) -> Span {
    Span {
        start: start as u32,
        len: len as u32,
    }
}

/// The stored plan, ending in its checksum. The same description gives the same bytes.
pub fn encode(plan: &PlanDesc) -> Result<Vec<u8>, PlanError> {
    let mut data = PlanData::default();
    for (at, step) in plan.steps.iter().enumerate() {
        let reads = step.inputs.iter().map(|input| match *input {
            Source::Absent => NO_SLOT,
            Source::Edge(edge) => edge,
            Source::Const(index) => CONST_BIT | index,
        });
        let reads_at = data.slots.len();
        data.slots.extend(reads);
        let writes_at = data.slots.len();
        data.slots.extend(
            step.outputs
                .iter()
                .map(|(port, edge)| (port << PORT_SHIFT) | edge),
        );
        data.nodes.push(step.node);
        data.steps.push(StepRecord {
            kind: StepKind::Call,
            body: step.entry,
            covers: span(at, 1),
            reads: span(reads_at, step.inputs.len()),
            writes: span(writes_at, step.outputs.len()),
        });
    }
    data.types = plan.edges.iter().map(|edge| edge.key).collect();
    let words = &mut data.extra;
    push_pair(words, plan.library);
    words.push(plan.edges.len() as u32);
    for edge in &plan.edges {
        words.extend([edge.capacity, edge.overflow as u32, edge.size]);
    }
    words.push(plan.consts.len() as u32);
    for constant in &plan.consts {
        words.push(constant.kind as u32);
        push_pair(words, constant.bits);
    }
    words.push(plan.params.len() as u32);
    for param in &plan.params {
        words.push(param.value);
        push_pair(words, param.min);
        push_pair(words, param.max);
    }
    words.push(plan.host_inputs.len() as u32);
    for port in &plan.host_inputs {
        words.extend([name_hash(&port.name), port.key, port.edges.len() as u32]);
        words.extend(&port.edges);
    }
    words.push(plan.host_outputs.len() as u32);
    for port in &plan.host_outputs {
        let edge = port.edges.first().copied().unwrap_or(u32::MAX);
        words.extend([name_hash(&port.name), port.key, edge]);
    }
    let mut bytes = Vec::new();
    encode_plan(&data, &mut bytes)?;
    let check = crc32(&bytes);
    bytes.extend_from_slice(&check.to_le_bytes());
    Ok(bytes)
}

/// The CPU backend, storing each region for a firmware that links `library`.
#[derive(Clone, Copy, Debug)]
pub struct BlobBackend {
    pub library: &'static Library,
}

fn scalar_bits(kind: ScalarKind, value: &Lit) -> Option<u64> {
    let integer = |low: i128, high: i128, value: &Lit| {
        let Lit::Int(value) = value else { return None };
        let value = i128::from(*value);
        (low..=high).contains(&value).then_some(value as u64)
    };
    let width = u32::from(kind.size()) * 8;
    Some(match kind {
        ScalarKind::Bool => match value {
            Lit::Bool(flag) => u64::from(*flag),
            _ => return None,
        },
        ScalarKind::I8 | ScalarKind::I16 | ScalarKind::I32 | ScalarKind::I64 => {
            let bits = integer(-(1 << (width - 1)), (1 << (width - 1)) - 1, value)?;
            if width == 64 {
                bits
            } else {
                bits & ((1 << width) - 1)
            }
        }
        ScalarKind::U8 | ScalarKind::U16 | ScalarKind::U32 | ScalarKind::U64 => {
            integer(0, (1 << width) - 1, value)?
        }
        ScalarKind::F32 => match value {
            Lit::Float(number) => u64::from((*number as f32).to_bits()),
            Lit::Int(number) => u64::from((*number as f32).to_bits()),
            _ => return None,
        },
        ScalarKind::F64 => match value {
            Lit::Float(number) => number.to_bits(),
            Lit::Int(number) => (*number as f64).to_bits(),
            _ => return None,
        },
    })
}

fn key_text(ty: &TypeRef) -> Option<&str> {
    match ty {
        TypeRef::Key(key) => Some(key.as_str()),
        _ => None,
    }
}

/// One node, while the plan is being put together.
struct Member {
    node: NodeIndex,
    entry: u32,
    decl: &'static NodeDecl,
    body: ArenaBody,
    inputs: u16,
}

impl Member {
    fn input(&self, port: usize) -> Option<(u32, u32)> {
        if let Some(declared) = self.decl.inputs.get(port) {
            return Some((
                type_hash(&declared.ty)?,
                u32::from(*self.body.inputs.get(port)?),
            ));
        }
        Some((
            type_hash(&self.decl.fanin.first()?.ty)?,
            u32::from(self.body.fan?),
        ))
    }

    fn output(&self, port: usize) -> Option<(u32, u32)> {
        let declared = self.decl.outputs.get(port)?;
        Some((
            type_hash(&declared.ty)?,
            u32::from(*self.body.outputs.get(port)?),
        ))
    }
}

fn members(
    library: &Library,
    region: &Region<'_>,
    cx: &CompileCx<'_>,
) -> Result<Vec<Member>, Refusal> {
    let mut found = Vec::with_capacity(region.nodes.len());
    for &node in region.nodes {
        let refuse = |why: &str| Refusal::new(vec![node], why);
        let body = cx
            .bodies
            .iter()
            .find(|body| body.node == node)
            .ok_or_else(|| refuse("no body for this node"))?;
        let entry = library
            .position(body.id)
            .ok_or_else(|| refuse("this node isn't in the firmware's library"))?;
        let (decl, arena) = library.entry(entry).ok_or_else(|| {
            refuse("this node can't run in a blob: its ports need to be small Copy values, its state must have nothing to drop, and it can't take a config")
        })?;
        found.push(Member {
            node,
            entry: entry as u32,
            decl,
            body: arena,
            inputs: body.inputs,
        });
    }
    Ok(found)
}

fn member_of(all: &[Member], port: PortRef) -> Result<&Member, Refusal> {
    all.iter()
        .find(|member| member.node == port.node)
        .ok_or_else(|| Refusal::new(vec![port.node], "this port isn't on a node in the region"))
}

fn add_edge(
    plan: &mut PlanDesc,
    policy: &EdgePolicyDoc,
    fifo: u32,
    (key, size): (u32, u32),
) -> u32 {
    let (capacity, overflow) = queue_shape(policy, fifo);
    plan.edges.push(EdgeDesc {
        capacity,
        overflow,
        size,
        key,
    });
    plan.edges.len() as u32 - 1
}

fn input_type(all: &[Member], port: PortRef) -> Result<(u32, u32), Refusal> {
    member_of(all, port)?
        .input(usize::from(port.port.0))
        .ok_or_else(|| Refusal::new(vec![port.node], "this input has no fixed type"))
}

fn output_type(all: &[Member], port: PortRef) -> Result<(u32, u32), Refusal> {
    member_of(all, port)?
        .output(usize::from(port.port.0))
        .ok_or_else(|| Refusal::new(vec![port.node], "this output has no fixed type"))
}

/// Every wire in and through the region, by port.
struct Wires {
    feeds: Vec<(PortRef, u32)>,
    drains: Vec<(PortRef, u32)>,
}

fn host_inputs(
    plan: &mut PlanDesc,
    all: &[Member],
    bindings: &[HostBinding<'_>],
    fifo: u32,
    wires: &mut Wires,
) -> Result<(), Refusal> {
    let mut names: Vec<&str> = Vec::new();
    for binding in bindings {
        if !names.contains(&binding.name) {
            names.push(binding.name);
        }
    }
    for name in names {
        let mut edges = Vec::new();
        let mut key = 0;
        for binding in bindings.iter().filter(|binding| binding.name == name) {
            let shaped = input_type(all, binding.port)?;
            key = shaped.0;
            let edge = add_edge(plan, binding.policy, fifo, shaped);
            wires.feeds.push((binding.port, edge));
            edges.push(edge);
        }
        plan.host_inputs.push(HostDesc {
            name: name.to_string(),
            key,
            edges,
        });
    }
    Ok(())
}

fn describe(
    library: &Library,
    region: &Region<'_>,
    cx: &CompileCx<'_>,
) -> Result<PlanDesc, Refusal> {
    let all = members(library, region, cx)?;
    let mut plan = PlanDesc {
        library: library.hash,
        ..PlanDesc::default()
    };
    let mut wires = Wires {
        feeds: Vec::new(),
        drains: Vec::new(),
    };
    for edge in cx.edges {
        let shaped = output_type(&all, edge.from)?;
        let index = add_edge(&mut plan, edge.policy, cx.fifo_capacity, shaped);
        wires.feeds.push((edge.to, index));
        wires.drains.push((edge.from, index));
    }
    host_inputs(
        &mut plan,
        &all,
        cx.host_inputs,
        cx.fifo_capacity,
        &mut wires,
    )?;
    for binding in cx.host_outputs {
        let shaped = output_type(&all, binding.port)?;
        let edge = add_edge(&mut plan, binding.policy, cx.fifo_capacity, shaped);
        wires.drains.push((binding.port, edge));
        plan.host_outputs.push(HostDesc {
            name: binding.name.to_string(),
            key: shaped.0,
            edges: vec![edge],
        });
    }
    for port in region.inputs {
        if !wires.feeds.iter().any(|(fed, _)| fed == port) {
            return Err(Refusal::new(
                vec![port.node],
                "a blob holds a whole graph, and this input comes from another backend",
            ));
        }
    }
    for port in region.outputs {
        if !wires.drains.iter().any(|(fed, _)| fed == port) {
            return Err(Refusal::new(
                vec![port.node],
                "a blob holds a whole graph, and this output goes to another backend",
            ));
        }
    }
    for member in &all {
        let step = step_of(&mut plan, member, &wires, cx)?;
        plan.steps.push(step);
    }
    Ok(plan)
}

fn step_of(
    plan: &mut PlanDesc,
    member: &Member,
    wires: &Wires,
    cx: &CompileCx<'_>,
) -> Result<StepDesc, Refusal> {
    let refuse = |why: String| Refusal::new(vec![member.node], &why);
    let marked = marks(body_of(member.node, cx)?, cx)?;
    let params_before = plan.params.len();
    let mut inputs = Vec::with_capacity(usize::from(member.inputs));
    for port in 0..member.inputs {
        let at = PortRef {
            node: member.node,
            port: daedalus_types::PortIndex(port),
        };
        let wired = wires.feeds.iter().find(|(fed, _)| *fed == at);
        let name = member.decl.inputs.get(usize::from(port)).map(|p| p.name);
        let constant = cx
            .consts
            .iter()
            .find(|one| one.node == member.node && Some(one.port) == name);
        inputs.push(match (wired, constant) {
            (Some((_, edge)), _) => Source::Edge(*edge),
            (None, Some(one)) => {
                let declared = &member.decl.inputs[usize::from(port)];
                let kind = key_text(&declared.ty)
                    .and_then(ScalarKind::from_key)
                    .ok_or_else(|| {
                        refuse(alloc::format!(
                            "input `{}`: a blob holds only scalar constants",
                            one.port
                        ))
                    })?;
                let bits = scalar_bits(kind, one.value).ok_or_else(|| {
                    refuse(alloc::format!(
                        "input `{}`: the constant doesn't fit",
                        one.port
                    ))
                })?;
                plan.consts.push(ConstDesc { kind, bits });
                let index = plan.consts.len() as u32 - 1;
                if let Some(mark) = marked.iter().find(|mark| mark.port == one.port) {
                    let body = body_of(member.node, cx)?;
                    let spec = checked_spec(body, mark, kind, one.value)?;
                    plan.params.push(ParamDesc {
                        value: index,
                        min: spec.min.bits(),
                        max: spec.max.bits(),
                    });
                }
                Source::Const(index)
            }
            (None, None) => Source::Absent,
        });
    }
    if plan.params.len() - params_before != marked.len() {
        return Err(refuse(String::from(
            "every marked parameter must be an input with a scalar constant",
        )));
    }
    let mut outputs = Vec::new();
    for port in 0..member.decl.outputs.len() {
        let at = PortRef {
            node: member.node,
            port: daedalus_types::PortIndex(port as u16),
        };
        outputs.extend(
            wires
                .drains
                .iter()
                .filter(|(from, _)| *from == at)
                .map(|(_, edge)| (port as u32, *edge)),
        );
    }
    Ok(StepDesc {
        entry: member.entry,
        node: member.node.0,
        inputs,
        outputs,
    })
}

impl Backend for BlobBackend {
    fn id(&self) -> &str {
        BACKEND_ID
    }

    fn memory(&self) -> Vec<MemorySpace> {
        CpuBackend.memory()
    }

    fn transfers(&self) -> Vec<Transfer> {
        Vec::new()
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        CpuBackend.can_take(offer)
    }

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let plan = describe(self.library, region, cx)?;
        let bytes = encode(&plan)
            .map_err(|_| Refusal::new(Vec::new(), "the plan is too large to store"))?;
        Ok(Compiled::Blob {
            bytes,
            units: vec![UnitInfo {
                covers: region.nodes.to_vec(),
                reads: region.inputs.to_vec(),
                writes: region.outputs.to_vec(),
                absorbed: Vec::new(),
                may_pend: false,
            }],
        })
    }
}
