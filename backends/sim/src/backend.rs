//! The backend itself.

use std::borrow::Cow;

use daedalus_backend::{
    Backend, CompileCx, Compiled, GeneratedCode, MemorySpace, NodeRef, Offer, Refusal, Region,
    SpaceKind, StepUnit, Transfer, TransferCost, UnitInfo,
};
use daedalus_types::NodeIndex;

use crate::steps::{make_transfer, step_for};
use crate::{BACKEND_ID, DEVICE, HOST, IMPORT, TO_DEVICE, TO_HOST, UNAVAILABLE};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SimForm {
    #[default]
    Steps,
    Blob,
    Source,
}

/// Every knob a planner test turns.
#[derive(Clone, Copy, Debug)]
pub struct SimConfig {
    /// Node ids refused in `compile`. A sim body's `text` is its node's id.
    pub refuse: &'static [&'static str],
    pub merge: bool,
    pub form: SimForm,
    /// Node indexes whose steps fault.
    pub faults: &'static [u32],
    /// Do imports as part of the step reading them.
    pub absorb: bool,
    /// Offer no way between the two spaces.
    pub no_transfers: bool,
    /// Report every unit as able to return `Pending`.
    pub pending: bool,
    /// Break the contract: forget one boundary output.
    pub drop_output: bool,
    /// Break the contract: report no covered nodes.
    pub omit_covers: bool,
}

impl Default for SimConfig {
    fn default() -> SimConfig {
        SimConfig {
            refuse: &[],
            merge: false,
            form: SimForm::Steps,
            faults: &[],
            absorb: true,
            no_transfers: false,
            pending: false,
            drop_output: false,
            omit_covers: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SimBackend {
    pub config: SimConfig,
}

impl SimBackend {
    pub fn new(config: SimConfig) -> SimBackend {
        SimBackend { config }
    }

    fn refuses(&self, node: NodeIndex, cx: &CompileCx<'_>) -> bool {
        cx.bodies
            .iter()
            .find(|body| body.node == node)
            .is_some_and(|body| self.config.refuse.contains(&body.text))
    }

    fn unit(&self, region: &Region<'_>, nodes: &[NodeIndex]) -> UnitInfo {
        let absorbed = region
            .incoming
            .iter()
            .enumerate()
            .filter(|(_, incoming)| {
                self.config.absorb
                    && incoming.transfer == IMPORT
                    && nodes.contains(&incoming.input.node)
            })
            .map(|(index, _)| index as u32)
            .collect();
        let touching = |ports: &[daedalus_backend::PortRef]| {
            ports
                .iter()
                .copied()
                .filter(|port| nodes.contains(&port.node))
                .collect()
        };
        UnitInfo {
            covers: nodes.to_vec(),
            reads: touching(region.inputs),
            writes: touching(region.outputs),
            absorbed,
            may_pend: self.config.pending,
        }
    }

    fn units(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Vec<UnitInfo> {
        let mut units: Vec<UnitInfo> = if self.config.merge && cx.merge_allowed {
            vec![self.unit(region, region.nodes)]
        } else {
            region
                .nodes
                .iter()
                .map(|node| self.unit(region, &[*node]))
                .collect()
        };
        if self.config.drop_output
            && let Some(unit) = units.iter_mut().rev().find(|unit| !unit.writes.is_empty())
        {
            unit.writes.pop();
        }
        units
    }
}

fn indexes(nodes: &[NodeIndex]) -> Vec<u32> {
    nodes.iter().map(|node| node.0).collect()
}

fn space(id: &'static str, kind: SpaceKind, readable: bool) -> MemorySpace {
    MemorySpace {
        id: Cow::Borrowed(id),
        kind,
        readable,
    }
}

impl Backend for SimBackend {
    fn id(&self) -> &str {
        BACKEND_ID
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![
            space(DEVICE, SpaceKind::Device, true),
            space(HOST, SpaceKind::Host, false),
        ]
    }

    fn transfers(&self) -> Vec<Transfer> {
        if self.config.no_transfers {
            return Vec::new();
        }
        vec![
            Transfer {
                id: TO_DEVICE,
                key: None,
                from: Cow::Borrowed(HOST),
                to: Cow::Borrowed(DEVICE),
                cost: TransferCost::Copy,
                make: make_transfer::<0>,
            },
            Transfer {
                id: TO_HOST,
                key: None,
                from: Cow::Borrowed(DEVICE),
                to: Cow::Borrowed(HOST),
                cost: TransferCost::Copy,
                make: make_transfer::<1>,
            },
        ]
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        offer
            .nodes
            .iter()
            .filter(|node| !node.needs.contains(&UNAVAILABLE))
            .map(|node| NodeRef {
                node: node.node,
                body: node.body,
            })
            .collect()
    }

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let refused: Vec<NodeIndex> = region
            .nodes
            .iter()
            .copied()
            .filter(|node| self.refuses(*node, cx))
            .collect();
        if !refused.is_empty() {
            return Err(Refusal::new(refused, "sim refuses"));
        }
        let mut units = self.units(region, cx);
        let compiled = match self.config.form {
            SimForm::Steps => {
                let steps: Vec<_> = units
                    .iter()
                    .map(|u| step_for(&indexes(&u.covers), self.config.faults))
                    .collect();
                if self.config.omit_covers {
                    units.iter_mut().for_each(|unit| unit.covers.clear());
                }
                Compiled::Steps(
                    steps
                        .into_iter()
                        .zip(units)
                        .map(|(step, info)| StepUnit { step, info })
                        .collect(),
                )
            }
            SimForm::Blob => Compiled::Blob {
                bytes: region
                    .nodes
                    .iter()
                    .flat_map(|node| node.0.to_le_bytes())
                    .collect(),
                units,
            },
            SimForm::Source => {
                let listed: Vec<String> = region.nodes.iter().map(|n| n.0.to_string()).collect();
                let text = format!("// sim region: {}\n", listed.join(" "));
                Compiled::Source {
                    code: GeneratedCode {
                        file: "sim.rs".to_string(),
                        text,
                    },
                    units,
                }
            }
        };
        Ok(compiled)
    }
}
