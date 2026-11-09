//! One row per step for a Detailed report.

use daedalus_runner::Runner;

/// One step's state at report time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeRow {
    pub step: u32,
    pub calls: u32,
    pub failures: u32,
    pub backing_off: bool,
    pub disabled: bool,
    pub auto_off: bool,
}

/// One row for every step of `runner`.
pub fn rows(runner: &Runner<'_>) -> Vec<NodeRow> {
    (0..runner.step_count() as u32)
        .filter_map(|step| {
            let health = runner.health(step)?;
            Some(NodeRow {
                step,
                calls: health.calls(),
                failures: health.failures(),
                backing_off: health.backing_off(),
                disabled: health.disabled(),
                auto_off: health.auto_off(),
            })
        })
        .collect()
}
