//! One row per step for a Detailed report.

use daedalus_runner::Runner;
use std::vec::Vec;

/// One step's state at report time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeRow {
    pub step: u32,
    pub calls: u32,
    pub failures: u32,
    pub backing_off: bool,
    pub disabled: bool,
    pub auto_off: bool,
    /// Runs of the step that were timed. Zero when the host has no probe: see
    /// [`LoadOptions::overhead_window`](crate::LoadOptions::overhead_window).
    pub timed_runs: u32,
    /// The time those runs took together, each from the end of the step before it to its own.
    pub time_ns: u64,
    /// The bytes the step published: the lengths of its outputs. A value handed on by reference
    /// counts its length and is not copied, and a typed value counts as nothing.
    pub bytes: u64,
}

/// One row for every step of `runner`.
pub fn rows(runner: &Runner<'_>) -> Vec<NodeRow> {
    (0..runner.step_count() as u32)
        .filter_map(|step| {
            let health = runner.health(step)?;
            let figures = runner.step_figures(step).unwrap_or_default();
            Some(NodeRow {
                step,
                calls: health.calls(),
                failures: health.failures(),
                backing_off: health.backing_off(),
                disabled: health.disabled(),
                auto_off: health.auto_off(),
                timed_runs: figures.timed_runs,
                time_ns: figures.time_ns,
                bytes: figures.bytes,
            })
        })
        .collect()
}
