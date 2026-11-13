//! Reading a node's progress: from the host itself, or through a handle another thread keeps
//! while the host ticks.

use daedalus_runner::owned_run::ProgressHandle;

use crate::load::LoadedHost;

/// One node's progress cells, shared with the running graph. Reading is two relaxed loads: no
/// lock, no allocation, and nothing looked up. It may outlive the host, and then reads the last
/// values. It belongs to the version of the graph it was taken from: after a reload, take a new one.
#[derive(Clone)]
pub struct NodeProgress {
    cells: ProgressHandle,
    step: u32,
}

impl NodeProgress {
    /// The `(done, total)` the node's body last reported. The two are read one after the other,
    /// so a pair read while the body writes may hold the new `done` with the old `total`.
    pub fn get(&self) -> (u32, u32) {
        self.cells.get(self.step).unwrap_or((0, 0))
    }
}

impl LoadedHost {
    /// The `(done, total)` the step running `node` last reported, as its body set it. `None` when no
    /// step runs the node, or the runner has no progress cells.
    pub fn node_progress(&self, node: &str) -> Option<(u32, u32)> {
        let step = self.covers.iter().position(|names| names.contains(&node))?;
        self.host.runner.progress(step as u32)
    }

    /// The progress of the step running `node`, as a handle another thread can keep and read while
    /// this host ticks, in parallel or not. The step is found here, once. `None` when no step runs
    /// the node.
    pub fn progress_handle(&self, node: &str) -> Option<NodeProgress> {
        let step = self.covers.iter().position(|names| names.contains(&node))?;
        let cells = self.host.owned.as_ref()?.progress_handle();
        Some(NodeProgress {
            cells,
            step: step as u32,
        })
    }
}
