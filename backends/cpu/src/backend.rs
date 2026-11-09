//! The backend itself.

use alloc::borrow::Cow;
use alloc::format;
use alloc::vec;
use alloc::vec::Vec;

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

fn ports(node: NodeIndex, count: u16) -> Vec<PortRef> {
    (0..count)
        .map(|port| PortRef {
            node,
            port: PortIndex(port),
        })
        .collect()
}

fn unit(node: NodeIndex, body: &NodeBody<'_>, cx: &CompileCx<'_>) -> Result<StepUnit, Refusal> {
    let Some(cpu) = body.code.downcast_ref::<CpuBody>() else {
        return Err(Refusal::new(vec![node], "this body isn't a CPU function"));
    };
    let setup = Setup {
        node,
        inputs: body.inputs,
        consts: cx.consts,
    };
    let step = (cpu.make)(&setup).map_err(|reason| Refusal::new(vec![node], &reason))?;
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
        let mut units = Vec::with_capacity(region.nodes.len());
        for &node in region.nodes {
            let Some(body) = cx.bodies.iter().find(|body| body.node == node) else {
                return Err(Refusal::new(
                    vec![node],
                    &format!("no body for node {}", node.0),
                ));
            };
            units.push(unit(node, body, cx)?);
        }
        Ok(Compiled::Steps(units))
    }
}
