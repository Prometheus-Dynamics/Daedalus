//! The backend itself.

use core::any::Any;

use alloc::borrow::Cow;
use alloc::format;
use alloc::vec;
use alloc::vec::Vec;

use daedalus_abi::{NO_NODE, Step};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeBody, NodeRef, Offer, PortRef, Refusal, Region,
    SpaceKind, StepUnit, Transfer, UnitInfo,
};
use daedalus_types::{NodeIndex, PortIndex};

use crate::steps::{CpuBody, Setup};

pub const BACKEND_ID: &str = "cpu";
pub const HOST_SPACE: &str = "cpu.host";

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
) -> Result<Step, Refusal> {
    let Some(cpu) = body.code.downcast_ref::<CpuBody>() else {
        return Err(Refusal::new(vec![node], "this body isn't a CPU function"));
    };
    let setup = Setup {
        node,
        inputs: body.inputs,
        consts: cx.consts,
    };
    (cpu.make)(&setup).map_err(|reason| Refusal::new(vec![node], &reason))
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

pub(crate) fn unit(node: NodeIndex, cx: &CompileCx<'_>) -> Result<StepUnit, Refusal> {
    let body = body_of(node, cx)?;
    let step = step_of(node, body, cx)?;
    Ok(StepUnit {
        step,
        info: UnitInfo {
            covers: vec![node],
            reads: ports(node, body.inputs),
            writes: ports(node, body.outputs),
            absorbed: Vec::new(),
            may_pend: false,
        },
    })
}

impl Backend for CpuBackend {
    fn id(&self) -> &str {
        BACKEND_ID
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: Cow::Borrowed(HOST_SPACE),
            kind: SpaceKind::Host,
            readable: true,
        }]
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
        let units = region.nodes.iter().map(|&node| unit(node, cx));
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
        };
        Some((cpu.make)(&setup).map_err(|reason| Refusal::whole_region(&reason)))
    }
}
