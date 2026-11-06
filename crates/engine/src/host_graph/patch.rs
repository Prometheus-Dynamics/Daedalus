//! Graph patches applied to a running [`HostGraph`]: its const inputs change between ticks.

use daedalus_planner::{GraphPatch, PatchReport};

use super::HostGraph;
use daedalus_runtime::executor::NodeHandler;

impl<H: NodeHandler + Send + Sync + 'static> HostGraph<H> {
    /// Applies a graph patch's const changes to the running graph, between ticks. A stable
    /// plugin node receives its changed consts on its next tick (see `docs/dynamic-plugins.md`).
    pub fn apply_patch(&mut self, patch: &GraphPatch) -> PatchReport {
        self.runner.executor.apply_patch(patch)
    }
}
