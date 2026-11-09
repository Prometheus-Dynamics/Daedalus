//! Assign, form, compile; when a backend refuses, take that choice away and go again.

use daedalus_backend::{Compiled, Refusal};
use daedalus_types::NodeIndex;

use super::compile::{Compiling, check_contract, compile_region};
use super::form::{form, rank};
use super::{CompiledRegion, Formed, Input, RegionError, Regions, rules};
use crate::assign::{self, Removed};

fn discard(compiled: Compiled) {
    if let Compiled::Steps(steps) = compiled {
        for unit in steps {
            unit.step.discard();
        }
    }
}

/// The nodes a refusal names, checked against its region.
fn refused_nodes(
    refusal: &Refusal,
    formed: &Formed,
    region: u32,
    backend: &str,
) -> Result<Vec<u32>, RegionError> {
    if refusal.nodes.is_empty() {
        return Ok(formed.nodes.clone());
    }
    let mut nodes = Vec::with_capacity(refusal.nodes.len());
    for &NodeIndex(node) in &refusal.nodes {
        if !formed.nodes.contains(&node) {
            return Err(RegionError::BackendContract {
                backend: backend.to_string(),
                region,
                rule: "refusal",
                detail: format!("refused node {node}, which isn't in the region"),
            });
        }
        nodes.push(node);
    }
    Ok(nodes)
}

fn discard_all(done: Vec<Compiled>) {
    done.into_iter().for_each(discard);
}

pub fn build(input: &Input<'_>) -> Result<Regions, RegionError> {
    let Input {
        graph,
        registry,
        backends,
        types,
        budget,
        merge_allowed,
    } = *input;
    let mut removed = Removed::new();
    let mut rounds = 0;
    loop {
        rounds += 1;
        let assignment = assign::assign(&assign::Input {
            graph,
            registry,
            backends,
            types,
            budget,
            removed: &removed,
        })
        .map_err(RegionError::Assign)?;
        let (formed, rates) = form(graph, &assignment);
        for (region, one) in formed.iter().enumerate() {
            if let Err(rule) = rules::check(graph, &one.nodes) {
                return Err(RegionError::RuleBroken {
                    rule,
                    region: region as u32,
                });
            }
        }
        let cx = Compiling {
            graph,
            registry,
            types,
            assignment: &assignment,
            merge_allowed,
        };
        let mut done = Vec::with_capacity(formed.len());
        let mut refusals = Vec::new();
        let mut failure = None;
        for (region, one) in formed.iter().enumerate() {
            let backend = backends[rank(one.backend)];
            match compile_region(&cx, one, backend) {
                Ok(compiled) => {
                    if let Err(error) = check_contract(one, region as u32, backend.id(), &compiled)
                    {
                        failure.get_or_insert(error);
                    }
                    done.push(compiled);
                }
                Err(refusal) => refusals.push((region, refusal)),
            }
        }
        if let Some(error) = failure {
            discard_all(done);
            return Err(error);
        }
        if refusals.is_empty() {
            let regions = formed
                .into_iter()
                .zip(done)
                .map(|(formed, compiled)| CompiledRegion { formed, compiled })
                .collect();
            return Ok(Regions {
                assignment,
                regions,
                rates,
                rounds,
            });
        }
        discard_all(done);
        for (region, refusal) in &refusals {
            let one = &formed[*region];
            let name = backends[rank(one.backend)].id();
            for node in refused_nodes(refusal, one, *region as u32, name)? {
                let solved = &graph.nodes[node as usize];
                if solved.doc.backend.is_some() {
                    return Err(RegionError::ForcedRefused {
                        node: solved.doc.id.clone(),
                        backend: name.to_string(),
                        reason: refusal.reason.clone(),
                    });
                }
                removed.insert((node, one.backend.0), refusal.reason.clone());
            }
        }
    }
}
