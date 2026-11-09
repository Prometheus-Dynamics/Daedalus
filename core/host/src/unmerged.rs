//! Running unmerged, by hand: to find which node inside a merged region fails, ask for a plan with
//! merging off and reload it. Nothing ever does this by itself, and the runner doesn't change.

#![cfg(feature = "std")]

use std::boxed::Box;
use std::fmt;
use std::string::String;

use daedalus_planner::Planned;

use crate::load::LoadedHost;
use crate::reload::ReloadError;
use crate::report::ReloadReport;

/// Makes a plan for this graph. The argument is whether merging is allowed.
pub(crate) type Planner = Box<dyn FnMut(bool) -> Result<Planned, String> + Send>;

/// Why switching modes failed. The running graph is unchanged.
#[derive(Debug)]
pub enum UnmergedError {
    /// [`LoadedHost::set_planner`] was never called.
    NoPlanner,
    Plan(String),
    Reload(ReloadError),
}

impl fmt::Display for UnmergedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnmergedError::NoPlanner => write!(f, "this host has no planner to ask"),
            UnmergedError::Plan(reason) => write!(f, "planning failed: {reason}"),
            UnmergedError::Reload(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for UnmergedError {}

impl LoadedHost {
    /// Gives this host a way to plan its graph again.
    pub fn set_planner(
        &mut self,
        planner: impl FnMut(bool) -> Result<Planned, String> + Send + 'static,
    ) {
        self.planner = Some(Box::new(planner));
    }

    /// Whether the graph is running unmerged.
    pub fn unmerged(&self) -> bool {
        self.unmerged
    }

    /// Turns unmerged mode on or off, through a reload. State is carried as for any reload.
    pub fn set_unmerged(&mut self, unmerged: bool) -> Result<ReloadReport, UnmergedError> {
        let planner = self.planner.as_mut().ok_or(UnmergedError::NoPlanner)?;
        let planned = planner(!unmerged).map_err(UnmergedError::Plan)?;
        let report = self.reload(planned).map_err(UnmergedError::Reload)?;
        self.unmerged = unmerged;
        Ok(report)
    }
}
