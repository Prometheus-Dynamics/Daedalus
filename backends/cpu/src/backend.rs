//! The backend itself.

use core::any::Any;

use alloc::borrow::Cow;
use alloc::format;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use daedalus_abi::{NO_NODE, Step};
use daedalus_backend::{
    Backend, CompileCx, Compiled, Decode, MemorySpace, NodeBody, NodeRef, Offer, PortRef, Refusal,
    Region, Sharing, SpaceKind, StepUnit, Transfer, UnitInfo,
};
use daedalus_types::{NodeIndex, PortIndex};

use crate::steps::{CpuBody, Keep, Setup};

pub const BACKEND_ID: &str = "cpu";
pub const HOST_SPACE: &str = "cpu.host";
/// The space whose cells hold their type's wire form: where an output a worker reads is placed.
/// Cells in [`HOST_SPACE`] hold Rust values in memory and never reach a worker.
pub const WIRE_SPACE: &str = "cpu.wire";

/// The region's outputs the planner placed in [`WIRE_SPACE`].
pub(crate) fn wire_outputs(region: &Region<'_>) -> Vec<PortRef> {
    region
        .outputs
        .iter()
        .zip(region.placed)
        .filter(|(_, space)| **space == WIRE_SPACE)
        .map(|(port, _)| *port)
        .collect()
}

/// The ports of a region that touch [`WIRE_SPACE`].
#[derive(Default)]
pub(crate) struct Wiring {
    /// Outputs written as their wire form, for a worker to read.
    pub outputs: Vec<PortRef>,
    /// Inputs that read a value in its wire form: one from a worker, or one a node in this
    /// region wrote for a worker.
    pub inputs: Vec<PortRef>,
}

impl Wiring {
    pub(crate) fn of(region: &Region<'_>, cx: &CompileCx<'_>) -> Wiring {
        let outputs = wire_outputs(region);
        let from_workers = region
            .inputs
            .iter()
            .zip(region.read_in)
            .filter(|(_, space)| **space == WIRE_SPACE)
            .map(|(port, _)| *port);
        let beside_workers = cx
            .edges
            .iter()
            .filter(|edge| outputs.contains(&edge.from))
            .map(|edge| edge.to);
        let mut inputs: Vec<PortRef> = from_workers.chain(beside_workers).collect();
        inputs.sort_unstable();
        inputs.dedup();
        Wiring { outputs, inputs }
    }
}

/// How node `node` reads its inputs in [`WIRE_SPACE`]: the step's wire mask
/// ([`Setup::wire_inputs`]) and the decodes the plan lists, or why it can't read one.
fn wire_reads(
    node: NodeIndex,
    body: &NodeBody<'_>,
    shape: Option<&crate::shape::BodyShape>,
    wiring: &Wiring,
) -> Result<(u32, Vec<Decode>), String> {
    use crate::shape::{Role, WireRead};
    use crate::steps::ports::{FAN_IN_WIRE, wire_bit};
    let mut decodes = Vec::new();
    let (mut mask, mut fanned) = (0u32, 0usize);
    for input in wiring.inputs.iter().filter(|input| input.node == node) {
        let Some(shape) = shape else {
            return Err(format!(
                "{} is made at run time and reads no wire form, so it can't read a value from a worker",
                body.id
            ));
        };
        let fixed = shape
            .params
            .iter()
            .filter(|param| has_port(param.role))
            .count();
        let at = usize::from(input.port.0);
        let param = shape.params.iter().find(|param| match at < fixed {
            true => has_port(param.role) && usize::from(param.at) == at,
            false => param.role == Role::FanIn,
        });
        let Some(param) = param else { continue };
        match at < fixed {
            true if wire_bit(at) == 0 => {
                return Err(format!(
                    "input {} of {} comes from a worker, and a CPU node reads values from workers on its first 31 inputs only",
                    param.port, body.id
                ));
            }
            true => mask |= wire_bit(at),
            false => {
                mask |= FAN_IN_WIRE;
                fanned += 1;
            }
        }
        let ty = (param.ty)();
        match param.wire {
            WireRead::Refused => {
                return Err(format!(
                    "input {} of {} comes from a worker, but {ty} has no wire form: add #[derive(WireForm)] to {ty}",
                    param.port, body.id
                ));
            }
            WireRead::Decoded => decodes.push(Decode {
                input: *input,
                ty: Cow::Borrowed(ty),
                reason: Cow::Borrowed(
                    "decoded from its wire form in cpu.wire, where a worker reads or wrote it",
                ),
            }),
            WireRead::Viewed => {}
        }
    }
    let fan_ports = usize::from(body.inputs).saturating_sub(shape.map_or(0, |shape| {
        shape.params.iter().filter(|p| has_port(p.role)).count()
    }));
    if fanned != 0 && fanned != fan_ports {
        return Err(format!(
            "{}'s fan-in takes values from workers and from CPU nodes; its ports must all come from one or the other",
            body.id
        ));
    }
    Ok((mask, decodes))
}

/// Whether a parameter is read from an input port of its own.
fn has_port(role: crate::shape::Role) -> bool {
    use crate::shape::Role;
    matches!(
        role,
        Role::Read | Role::Take | Role::Modify | Role::OptRead | Role::OptTake | Role::OptModify
    )
}

/// Runs `#[node]` functions on the host's CPU.
#[derive(Clone, Copy, Debug, Default)]
pub struct CpuBackend;

pub(crate) fn ports(node: NodeIndex, count: u16) -> Vec<PortRef> {
    (0..count)
        .map(|port| PortRef {
            node,
            port: PortIndex(port),
        })
        .collect()
}

/// Builds one node's step.
pub(crate) fn step_of(
    node: NodeIndex,
    body: &NodeBody<'_>,
    cx: &CompileCx<'_>,
    wiring: &Wiring,
) -> Result<(Step, Vec<Decode>), Refusal> {
    // A body is a `#[node]` function, or a closure a host made at run time.
    enum Make<'b> {
        Function(&'b CpuBody),
        Closure(&'b crate::body::CpuStepBody),
    }
    let make = if let Some(cpu) = body.code.downcast_ref::<CpuBody>() {
        Make::Function(cpu)
    } else if let Some(made) = body.code.downcast_ref::<crate::body::CpuStepBody>() {
        Make::Closure(made)
    } else {
        return Err(Refusal::new(vec![node], "this body isn't a CPU function"));
    };
    let keep: Vec<Keep> = (0..body.outputs)
        .map(|port| {
            let port = PortRef {
                node,
                port: PortIndex(port),
            };
            if wiring.outputs.contains(&port) {
                Keep::Wire
            } else {
                keep_of(cx, port)
            }
        })
        .collect();
    let shape = match make {
        Make::Function(cpu) => Some(&cpu.shape),
        Make::Closure(_) => None,
    };
    let refused = |reason: String| Refusal::new(vec![node], &reason);
    let (wire_inputs, decodes) = wire_reads(node, body, shape, wiring).map_err(refused)?;
    let setup = Setup {
        node,
        inputs: body.inputs,
        consts: cx.consts,
        keep: &keep,
        wire_inputs,
        id: body.id,
    };
    let step = match make {
        Make::Function(cpu) => (cpu.make)(&setup),
        // A body made at run time writes no wire form, so it can't feed a worker.
        Make::Closure(_) if keep.contains(&Keep::Wire) => Err(format!(
            "{} is made at run time and writes no wire form, so its outputs can't go to a worker",
            body.id
        )),
        Make::Closure(made) => (made.make)(&setup),
    }
    .map_err(refused)?;
    Ok((step, decodes))
}

/// What the planner says an output keeps between ticks.
fn keep_of(cx: &CompileCx<'_>, port: PortRef) -> Keep {
    match cx.sharing.iter().find(|one| one.port == port) {
        Some(one) => match one.sharing {
            Sharing::Fresh => Keep::Fresh,
            Sharing::Readers => Keep::Readers,
            Sharing::Taker => Keep::Taker,
        },
        None => Keep::Fresh,
    }
}

pub(crate) fn body_of<'a>(
    node: NodeIndex,
    cx: &'a CompileCx<'a>,
) -> Result<&'a NodeBody<'a>, Refusal> {
    cx.bodies
        .iter()
        .find(|body| body.node == node)
        .ok_or_else(|| Refusal::new(vec![node], &format!("no body for node {}", node.0)))
}

pub(crate) fn unit(
    node: NodeIndex,
    cx: &CompileCx<'_>,
    wiring: &Wiring,
) -> Result<StepUnit, Refusal> {
    let body = body_of(node, cx)?;
    let (step, decodes) = step_of(node, body, cx, wiring)?;
    Ok(StepUnit {
        step,
        info: UnitInfo {
            covers: vec![node],
            reads: ports(node, body.inputs),
            writes: ports(node, body.outputs),
            absorbed: Vec::new(),
            may_pend: false,
            decodes,
        },
    })
}

impl Backend for CpuBackend {
    fn id(&self) -> &str {
        BACKEND_ID
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![
            MemorySpace {
                id: Cow::Borrowed(HOST_SPACE),
                kind: SpaceKind::Host,
                readable: true,
            },
            MemorySpace {
                id: Cow::Borrowed(WIRE_SPACE),
                kind: SpaceKind::Host,
                readable: true,
            },
        ]
    }

    /// An output can be written as its wire form, for a worker to read.
    fn placements(&self) -> Vec<Cow<'static, str>> {
        vec![Cow::Borrowed(WIRE_SPACE)]
    }

    fn transfers(&self) -> Vec<Transfer> {
        Vec::new()
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        offer
            .nodes
            .iter()
            .map(|node| NodeRef {
                node: node.node,
                body: node.body,
            })
            .collect()
    }

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        #[cfg(feature = "merge")]
        if cx.merge_allowed {
            return crate::merge::compile(region, cx).map(Compiled::Steps);
        }
        let wiring = Wiring::of(region, cx);
        let units = region.nodes.iter().map(|&node| unit(node, cx, &wiring));
        Ok(Compiled::Steps(units.collect::<Result<_, _>>()?))
    }

    /// A `#[transfer]` written for this backend. It takes the value and nothing else, so its
    /// step has one input cell and one output cell, and no node to name in a fault.
    fn bind_transfer(
        &self,
        code: &(dyn Any + Send + Sync),
        cx: &CompileCx<'_>,
    ) -> Option<Result<Step, Refusal>> {
        let cpu = code.downcast_ref::<CpuBody>()?;
        if cpu.shape.params.len() != 1 {
            return Some(Err(Refusal::whole_region(
                "a CPU transfer takes the value and nothing else",
            )));
        }
        let setup = Setup {
            node: NodeIndex(NO_NODE),
            inputs: 1,
            consts: cx.consts,
            keep: &[],
            wire_inputs: 0,
            id: "transfer",
        };
        Some((cpu.make)(&setup).map_err(|reason| Refusal::whole_region(&reason)))
    }
}
