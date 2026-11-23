//! A finished plan as the facts and requests a host loads: `Planned::made` writes one, and
//! `Bound::from` takes a plan's steps for this process without going through bytes.

use std::mem;

use daedalus_backend::Compiled;
use daedalus_graph::NodeKindDoc;
use daedalus_made::{Bound, MadeEdge, MadeMove, MadeNode, MadePlan, MadeRegion, PlanFacts};

use crate::api::{PlanError, Planned};
use crate::form::Form;
use crate::stitch::transfers::MoveOwner;

impl Planned {
    /// The facts a host reads, which are the same for a plan made here and one loaded elsewhere.
    pub(crate) fn facts(&self) -> PlanFacts {
        let mut backend_of = vec![None; self.graph.nodes.len()];
        for region in &self.regions.regions {
            for &node in &region.formed.nodes {
                backend_of[node as usize] = Some(region.formed.backend.0);
            }
        }
        let nodes = self
            .graph
            .nodes
            .iter()
            .enumerate()
            .map(|(at, node)| MadeNode {
                id: node.doc.id.clone(),
                label: node.doc.label.clone(),
                bridge: node.doc.kind == NodeKindDoc::HostBridge,
                metadata: node.doc.metadata.clone(),
                fire_all: node.fire == daedalus_registry::Fire::All,
                required: node.required.clone(),
                reusable: node.reusable,
                inputs: node.inputs.iter().map(|port| port.name.clone()).collect(),
                consts: node.consts.clone(),
                backend: backend_of[at],
                outputs: node.outputs.iter().map(|port| port.name.clone()).collect(),
            })
            .collect();
        let edges = self
            .graph
            .edges
            .iter()
            .map(|edge| MadeEdge {
                to_node: edge.to.node,
                to_port: edge.to.port.clone(),
                policy: edge.policy.clone(),
            })
            .collect();
        PlanFacts {
            nodes,
            edges,
            types: self.types.exprs().to_vec(),
            stitched: self.stitched.clone(),
        }
    }

    /// The plan as a host loads it. A plan with a control node, which runs a nested plan, has no
    /// made form yet, and neither does a plan in a form other than steps.
    pub fn made(&self) -> Result<MadePlan, PlanError> {
        if self.form != Form::Steps {
            return Err(PlanError::MadeUnavailable {
                node: String::new(),
                reason: "only a plan in the steps form can be made".to_string(),
            });
        }
        if let Some(node) = self
            .graph
            .nodes
            .iter()
            .find(|node| node.doc.kind == NodeKindDoc::Control)
        {
            return Err(PlanError::MadeUnavailable {
                node: node.doc.id.clone(),
                reason: "a control node runs a nested plan, which a made plan doesn't carry"
                    .to_string(),
            });
        }
        let mut regions = Vec::with_capacity(self.regions.regions.len());
        for region in &self.regions.regions {
            let Compiled::Steps(units) = &region.compiled else {
                return Err(PlanError::MadeUnavailable {
                    node: String::new(),
                    reason: "a region did not compile to steps".to_string(),
                });
            };
            let at = usize::from(region.formed.backend.0);
            regions.push(MadeRegion {
                backend: region.formed.backend.0,
                recipe: region.recipe.clone(),
                units: units.iter().map(|unit| unit.info.clone()).collect(),
                decl_hash: self.backend_decls[at],
            });
        }
        let moves = self
            .stitched
            .transfers
            .iter()
            .map(|used| match &self.regions.routing.table[used.table].owner {
                MoveOwner::Backend { backend, index } => MadeMove::Backend {
                    id: used.id.clone(),
                    backend: self.backend_ids[usize::from(*backend)].clone(),
                    index: *index,
                },
                MoveOwner::Record(record) => MadeMove::Record {
                    id: used.id.clone(),
                    backend: record.body.backend.to_string(),
                    key: record.key.clone(),
                    from: record.from.to_string(),
                    to: record.to.to_string(),
                },
            })
            .collect();
        Ok(MadePlan {
            facts: self.facts(),
            backends: self.backend_ids.clone(),
            regions,
            moves,
        })
    }
}

impl From<Planned> for Bound {
    fn from(mut planned: Planned) -> Bound {
        let facts = planned.facts();
        let region_backends = planned
            .backend_ids
            .iter()
            .cloned()
            .zip(planned.backend_versions.iter().copied())
            .collect();
        let units = planned
            .regions
            .regions
            .iter_mut()
            .map(|region| match &mut region.compiled {
                Compiled::Steps(units) => mem::take(units).into_iter().map(Some).collect(),
                _ => Vec::new(),
            })
            .collect();
        let move_steps = mem::take(&mut planned.transfer_steps);
        Bound {
            facts,
            region_backends,
            units,
            move_steps,
        }
    }
}
