//! Binds a made plan on a host: asks the host's backends for the steps of each region again, and
//! checks that they answer as the planner's backends did.

use std::fmt;

use daedalus_abi::Step;
use daedalus_backend::{Backend, CompileCx, Compiled, Refusal, StepUnit};
use daedalus_registry::Registry;

use crate::facts::PlanFacts;
use crate::plan::{MadeMove, MadePlan, declaration};

/// A plan with its steps, ready to lay out. The steps not taken are freed when it's dropped.
#[derive(Debug)]
pub struct Bound {
    pub facts: PlanFacts,
    /// For each backend of the plan's list, its id and the version it has here. A plugin backend's
    /// version is a generation of its process, so a reload compares against this one.
    pub region_backends: Vec<(String, u64)>,
    /// For each region, its units' steps, in order.
    pub units: Vec<Vec<Option<StepUnit>>>,
    /// For each of `facts.stitched.transfers`, its step.
    pub move_steps: Vec<Option<Step>>,
}

impl Bound {
    /// The id and version of the backend a region index names.
    pub fn backend(&self, index: u8) -> Option<(&str, u64)> {
        let (id, version) = self.region_backends.get(usize::from(index))?;
        Some((id.as_str(), *version))
    }
}

impl Drop for Bound {
    fn drop(&mut self) {
        for unit in self.units.drain(..).flatten().flatten() {
            unit.step.discard();
        }
        for step in self.move_steps.drain(..).flatten() {
            step.discard();
        }
    }
}

/// Why a made plan doesn't bind on this host. Nothing is left running.
#[derive(Debug)]
#[non_exhaustive]
pub enum BindError {
    /// A backend the plan needs isn't on this host.
    NoBackend { backend: String },
    /// A backend the plan needs declares something other than when the plan was made.
    Declaration { backend: String, region: u32 },
    /// A backend refused a region that the planner compiled.
    Refused { region: u32, refusal: Refusal },
    /// A region compiled to something other than steps.
    NotSteps { region: u32 },
    /// A backend answered a region with other units than the planner's.
    UnitsDiffer { region: u32 },
    /// A move has no step on this host, or the plan lists the moves differently.
    Transfer { id: String, reason: String },
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindError::NoBackend { backend } => {
                write!(
                    f,
                    "the plan needs backend `{backend}`, which this host doesn't have"
                )
            }
            BindError::Declaration { backend, region } => write!(
                f,
                "backend `{backend}` declares something other than when region {region} was made"
            ),
            BindError::Refused { region, refusal } => {
                write!(f, "backend refused region {region}: {}", refusal.reason)
            }
            BindError::NotSteps { region } => {
                write!(f, "region {region} did not compile to steps")
            }
            BindError::UnitsDiffer { region } => {
                write!(f, "region {region} compiled to other units than the plan's")
            }
            BindError::Transfer { id, reason } => write!(f, "transfer `{id}`: {reason}"),
        }
    }
}

impl std::error::Error for BindError {}

fn find<'a>(backends: &[&'a dyn Backend], id: &str) -> Option<&'a dyn Backend> {
    backends.iter().copied().find(|backend| backend.id() == id)
}

/// Binds a made plan against the host's backends and registry.
pub fn bind(
    made: &MadePlan,
    registry: &Registry,
    backends: &[&dyn Backend],
) -> Result<Bound, BindError> {
    let region_backends = made
        .backends
        .iter()
        .map(|id| {
            let version = find(backends, id).map_or(0, |backend| backend.version());
            (id.clone(), version)
        })
        .collect();
    // Built as a `Bound`, so a failure below frees what was already made.
    let mut bound = Bound {
        facts: made.facts.clone(),
        region_backends,
        units: Vec::with_capacity(made.regions.len()),
        move_steps: Vec::with_capacity(made.moves.len()),
    };
    for (at, region) in made.regions.iter().enumerate() {
        let region_no = at as u32;
        let id = made
            .backends
            .get(usize::from(region.backend))
            .map_or("", String::as_str);
        let Some(backend) = find(backends, id) else {
            return Err(BindError::NoBackend {
                backend: id.to_string(),
            });
        };
        if declaration(backend) != region.decl_hash {
            return Err(BindError::Declaration {
                backend: id.to_string(),
                region: region_no,
            });
        }
        let compiled = region
            .recipe
            .compile(backends, registry, &made.facts.types)
            .map_err(|refusal| BindError::Refused {
                region: region_no,
                refusal,
            })?;
        let Compiled::Steps(steps) = compiled else {
            return Err(BindError::NotSteps { region: region_no });
        };
        let same = steps.len() == region.units.len()
            && steps
                .iter()
                .zip(&region.units)
                .all(|(unit, info)| unit.info == *info);
        if !same {
            for unit in steps {
                unit.step.discard();
            }
            return Err(BindError::UnitsDiffer { region: region_no });
        }
        bound.units.push(steps.into_iter().map(Some).collect());
    }
    if made.moves.len() != made.facts.stitched.transfers.len() {
        return Err(BindError::Transfer {
            id: String::new(),
            reason: "the plan lists its moves differently from its layout".to_string(),
        });
    }
    let cx = CompileCx {
        types: &made.facts.types,
        consts: &[],
        spaces: &[],
        bodies: &[],
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 0,
        merge_allowed: false,
        freeze_params: false,
        sharing: &[],
        lent: &[],
        offered: &[],
    };
    for mv in &made.moves {
        let step = bind_move(mv, registry, backends, &cx)?;
        bound.move_steps.push(Some(step));
    }
    Ok(bound)
}

fn bind_move(
    mv: &MadeMove,
    registry: &Registry,
    backends: &[&dyn Backend],
    cx: &CompileCx<'_>,
) -> Result<Step, BindError> {
    let refused = |id: &str, reason: String| BindError::Transfer {
        id: id.to_string(),
        reason,
    };
    match mv {
        MadeMove::Backend { id, backend, index } => {
            let Some(owner) = find(backends, backend) else {
                return Err(BindError::NoBackend {
                    backend: backend.clone(),
                });
            };
            let table = owner.transfers();
            let Some(handler) = table.get(*index as usize).filter(|h| h.id == id.as_str()) else {
                return Err(refused(
                    id,
                    format!("`{backend}` declares no such transfer"),
                ));
            };
            (handler.make)(cx).map_err(|refusal| refused(id, refusal.reason))
        }
        MadeMove::Record {
            id,
            backend,
            key,
            from,
            to,
        } => {
            let record = registry
                .transfers()
                .find(|record| {
                    record.id == id.as_str()
                        && &record.key == key
                        && record.from == from.as_str()
                        && record.to == to.as_str()
                })
                .ok_or_else(|| refused(id, "no node body in this registry writes it".into()))?;
            let bound = backends
                .iter()
                .filter(|candidate| candidate.id() == record.body.backend)
                .find_map(|candidate| candidate.bind_transfer(record.body.code, cx));
            match bound {
                Some(Ok(step)) => Ok(step),
                Some(Err(refusal)) => Err(refused(id, refusal.reason)),
                None => Err(refused(
                    id,
                    format!("no backend `{backend}` is present to bind it"),
                )),
            }
        }
    }
}
