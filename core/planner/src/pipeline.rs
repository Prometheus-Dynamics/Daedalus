//! The solve stages, in their one fixed order. Only this file calls stages.

use std::fmt;
use std::sync::Arc;

use daedalus_graph::GraphBody;
use daedalus_registry::Registry;

use crate::solve::catalog::Catalog;
use crate::solve::consts::{self, ConstError};
use crate::solve::convert::{self, ConvertError};
use crate::solve::defaults::{self, DefaultsError};
use crate::solve::exclusive::{self, ExclusiveError};
use crate::solve::expand::{self, ExpandError};
use crate::solve::hydrate::{self, HydrateError};
use crate::solve::order::{self, OrderError};
use crate::solve::ports::{self, PortError};
use crate::solve::rates::{self, RateError};
use crate::solve::rewrite::{self, Rewrite, RewriteError};
use crate::solve::typecheck::{self, TypeCheckError};
use crate::solve::{Graph, Work};

/// What a planning run is told. Nothing here is global.
#[derive(Clone, Default)]
pub struct Config {
    /// Also check a graph's own port lists against the declaration.
    pub strict_ports: bool,
    /// Graph rewrites, run in this order.
    pub rewrites: Vec<Arc<dyn Rewrite>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SolveError {
    Expand(ExpandError),
    Defaults(DefaultsError),
    Hydrate(HydrateError),
    Ports(PortError),
    Types(TypeCheckError),
    Consts(ConstError),
    Convert(ConvertError),
    Exclusive(ExclusiveError),
    Rewrite(RewriteError),
    Order(OrderError),
    Rates(RateError),
}

impl fmt::Display for SolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SolveError::Expand(error) => error.fmt(f),
            SolveError::Defaults(error) => error.fmt(f),
            SolveError::Hydrate(error) => error.fmt(f),
            SolveError::Ports(error) => error.fmt(f),
            SolveError::Types(error) => error.fmt(f),
            SolveError::Consts(error) => error.fmt(f),
            SolveError::Convert(error) => error.fmt(f),
            SolveError::Exclusive(error) => error.fmt(f),
            SolveError::Rewrite(error) => error.fmt(f),
            SolveError::Order(error) => error.fmt(f),
            SolveError::Rates(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for SolveError {}

/// Runs every solve stage. A later stage still runs after an earlier one reports errors, where it
/// can, so one run reports as much as possible.
pub fn solve(
    graph: &GraphBody,
    registry: &Registry,
    config: &Config,
) -> Result<Graph, Vec<SolveError>> {
    let mut work = Work {
        graph: Graph::default(),
        catalog: Catalog::new(registry),
        config,
        errors: Vec::new(),
    };
    expand::run(&mut work, graph);
    defaults::run(&mut work);
    rewrite::run(&mut work);
    hydrate::run(&mut work);
    ports::run(&mut work);
    typecheck::run(&mut work);
    consts::run(&mut work);
    convert::run(&mut work);
    exclusive::run(&mut work);
    order::run(&mut work);
    rates::run(&mut work);
    if work.errors.is_empty() {
        Ok(work.graph)
    } else {
        Err(work.errors)
    }
}
