//! The one way to make a plan.

use std::fmt;
use std::path::Path;

use daedalus_abi::Step;
use daedalus_backend::{Backend, CompileCx, Compiled};
use daedalus_graph::GraphBody;
use daedalus_plan::encode;
use daedalus_registry::Registry;

use crate::config::Config;
use crate::control::{ControlBackend, NestedBody};
use crate::diagnostic::Diagnostic;
use crate::form::Form;
use crate::lint::lint;
use crate::pipeline::{SolveError, solve};
use crate::regions::{self, RegionError, Regions};
use crate::solve::Graph;
use crate::solve::type_table::TypeTable;
use crate::stitch::transfers::MoveOwner;
use crate::stitch::{Stitched, stitch};

#[derive(Debug)]
#[non_exhaustive]
pub enum PlanError {
    Solve(Vec<SolveError>),
    /// A control node's body didn't plan.
    Control {
        node: String,
        body: String,
        error: Box<PlanError>,
    },
    Regions(RegionError),
    /// A backend's output can't be delivered in the requested form.
    FormUnavailable {
        form: Form,
        backend: String,
    },
    /// A backend couldn't make the step for a transfer.
    TransferUnavailable {
        id: String,
        reason: String,
    },
    Encode(daedalus_plan::PlanError),
    Write(std::io::Error),
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanError::Control { node, body, error } => {
                write!(f, "control node `{node}`, body `{body}`: {error}")
            }
            PlanError::Solve(errors) => {
                let text: Vec<String> = errors.iter().map(ToString::to_string).collect();
                f.write_str(&text.join("; "))
            }
            PlanError::Regions(error) => error.fmt(f),
            PlanError::FormUnavailable { form, backend } => {
                write!(f, "backend `{backend}` can't produce the {form:?} form")
            }
            PlanError::TransferUnavailable { id, reason } => {
                write!(f, "transfer `{id}`: {reason}")
            }
            PlanError::Encode(error) => write!(f, "couldn't encode the plan: {error:?}"),
            PlanError::Write(error) => write!(f, "couldn't write the plan: {error}"),
        }
    }
}

impl std::error::Error for PlanError {}

/// A finished plan. Its steps are freed when it's dropped, unless taken.
#[derive(Debug)]
pub struct Planned {
    pub graph: Graph,
    pub types: TypeTable,
    pub regions: Regions,
    pub stitched: Stitched,
    pub form: Form,
    /// One per `stitched.transfers`. `None` where the loader binds it, or outside the steps form.
    pub transfer_steps: Vec<Option<Step>>,
    /// What the lints found. Empty unless the config turned them on.
    pub warnings: Vec<Diagnostic>,
    /// What each control node runs, by body.
    pub nested: Vec<NestedBody>,
    pub(crate) backend_ids: Vec<String>,
}

impl Planned {
    /// The stored encoding.
    pub fn blob(&self) -> Result<Vec<u8>, PlanError> {
        let mut bytes = Vec::new();
        encode(&self.stitched.plan, &mut bytes).map_err(PlanError::Encode)?;
        Ok(bytes)
    }

    /// The generated Rust of every region, in region order.
    pub fn source(&self) -> Result<String, PlanError> {
        let mut text = String::new();
        for region in &self.regions.regions {
            let Compiled::Source { code, .. } = &region.compiled else {
                return Err(PlanError::FormUnavailable {
                    form: Form::Source,
                    backend: self.backend_ids[usize::from(region.formed.backend.0)].clone(),
                });
            };
            text.push_str(&code.text);
        }
        Ok(text)
    }

    /// Writes the plan in its form. The steps form has no file, so it's refused.
    pub fn write(&self, path: impl AsRef<Path>) -> Result<(), PlanError> {
        let bytes = match self.form {
            Form::Blob => self.blob()?,
            Form::Source => self.source()?.into_bytes(),
            Form::Steps => {
                return Err(PlanError::FormUnavailable {
                    form: Form::Steps,
                    backend: "(a plan of in-process steps has no file form)".to_string(),
                });
            }
        };
        std::fs::write(path, bytes).map_err(PlanError::Write)
    }
}

impl Drop for Planned {
    fn drop(&mut self) {
        for step in self.transfer_steps.drain(..).flatten() {
            step.discard();
        }
        for region in &mut self.regions.regions {
            if let Compiled::Steps(units) = &mut region.compiled {
                for unit in units.drain(..) {
                    unit.step.discard();
                }
            }
        }
    }
}

pub(crate) fn by_priority<'a>(
    backends: &[&'a dyn Backend],
    priority: &[String],
) -> Vec<&'a dyn Backend> {
    let rank = |backend: &&dyn Backend| {
        priority
            .iter()
            .position(|id| id == backend.id())
            .unwrap_or(priority.len())
    };
    let mut ordered = backends.to_vec();
    ordered.sort_by_key(rank);
    ordered
}

fn check_form(regions: &Regions, backends: &[&dyn Backend], form: Form) -> Result<(), PlanError> {
    for region in &regions.regions {
        let matches_form = form == Form::Blob
            || matches!(
                (&region.compiled, form),
                (Compiled::Steps(_), Form::Steps) | (Compiled::Source { .. }, Form::Source)
            );
        if !matches_form {
            let backend = backends[usize::from(region.formed.backend.0)]
                .id()
                .to_string();
            return Err(PlanError::FormUnavailable { form, backend });
        }
    }
    Ok(())
}

fn table_of(graph: &Graph) -> TypeTable {
    let mut types = TypeTable::default();
    for node in &graph.nodes {
        for port in node.inputs.iter().chain(&node.outputs) {
            types.add(&port.ty);
        }
    }
    types
}

/// Makes the step for each transfer the plan uses.
fn transfer_steps(
    regions: &Regions,
    stitched: &Stitched,
    backends: &[&dyn Backend],
    types: &TypeTable,
) -> Result<Vec<Option<Step>>, PlanError> {
    let cx = CompileCx {
        types: types.exprs(),
        consts: &[],
        spaces: &[],
        bodies: &[],
        merge_allowed: false,
    };
    let mut steps: Vec<Option<Step>> = Vec::with_capacity(stitched.transfers.len());
    for used in &stitched.transfers {
        let made = match regions.routing.table[used.table].owner {
            MoveOwner::Record(_) => None,
            MoveOwner::Backend { backend, index } => {
                let table = backends[usize::from(backend)].transfers();
                match (table[index as usize].make)(&cx) {
                    Ok(step) => Some(step),
                    Err(refusal) => {
                        steps.drain(..).flatten().for_each(Step::discard);
                        return Err(PlanError::TransferUnavailable {
                            id: used.id.clone(),
                            reason: refusal.reason,
                        });
                    }
                }
            }
        };
        steps.push(made);
    }
    Ok(steps)
}

/// Solves, assigns, compiles and stitches a graph. Backends come highest priority first, unless
/// the config's list says otherwise.
pub fn plan(
    graph: &GraphBody,
    registry: &Registry,
    backends: &[&dyn Backend],
    config: &Config,
) -> Result<Planned, PlanError> {
    let solved = solve(graph, registry, config).map_err(PlanError::Solve)?;
    let warnings = if config.lints {
        lint(&solved, registry)
    } else {
        Vec::new()
    };
    let types = table_of(&solved);
    let control = ControlBackend::prepare(&solved, registry, backends, config)?;
    let mut with_control = backends.to_vec();
    if let Some(control) = &control {
        with_control.push(control);
    }
    let backends = by_priority(&with_control, &config.priority);
    let regions = regions::build(&regions::Input {
        graph: &solved,
        registry,
        backends: &backends,
        types: &types,
        budget: config.assignment_budget,
        merge_allowed: config.merge,
    })
    .map_err(PlanError::Regions)?;
    let stitched = stitch(&regions, &types);
    let nested = control
        .as_ref()
        .map(ControlBackend::records)
        .unwrap_or_default();
    let mut planned = Planned {
        graph: solved,
        types,
        regions,
        stitched,
        form: config.form,
        transfer_steps: Vec::new(),
        warnings,
        nested,
        backend_ids: backends.iter().map(|b| b.id().to_string()).collect(),
    };
    check_form(&planned.regions, &backends, config.form)?;
    if config.form == Form::Steps {
        planned.transfer_steps = transfer_steps(
            &planned.regions,
            &planned.stitched,
            &backends,
            &planned.types,
        )?;
    }
    Ok(planned)
}

/// The builder form of [`plan`].
pub struct Planner<'a> {
    graph: &'a GraphBody,
    registry: &'a Registry,
    backends: Vec<&'a dyn Backend>,
    config: Config,
}

impl<'a> Planner<'a> {
    pub fn new(graph: &'a GraphBody, registry: &'a Registry) -> Planner<'a> {
        Planner {
            graph,
            registry,
            backends: Vec::new(),
            config: Config::default(),
        }
    }

    pub fn backends(mut self, backends: &[&'a dyn Backend]) -> Planner<'a> {
        self.backends = backends.to_vec();
        self
    }

    pub fn config(mut self, config: Config) -> Planner<'a> {
        self.config = config;
        self
    }

    pub fn solve(self) -> Result<Planned, PlanError> {
        plan(self.graph, self.registry, &self.backends, &self.config)
    }
}
