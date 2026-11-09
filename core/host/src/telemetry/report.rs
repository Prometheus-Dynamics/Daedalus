//! The execution report: a text table and a JSON form. Both are host side, so they may allocate.

use std::fmt::Write;
use std::string::String;
use std::vec::Vec;

use daedalus_runner::Counters;

use super::{Level, NodeRow};

/// What the host saw of a run, at one point in time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionReport {
    pub level: Level,
    pub counters: Counters,
    /// Ticks that had a clock reading, and their total and longest time in nanoseconds.
    pub ticks_timed: u64,
    pub total_ns: u64,
    pub max_ns: u64,
    /// One row per step at Detailed and above.
    pub nodes: Vec<NodeRow>,
}

impl ExecutionReport {
    /// A plain text table.
    pub fn table(&self) -> String {
        let c = &self.counters;
        let mut out = String::new();
        let _ = writeln!(out, "level        {:?}", self.level);
        let _ = writeln!(out, "ticks        {} ({} idle)", c.ticks, c.idle_ticks);
        let _ = writeln!(out, "steps called {}", c.steps_called);
        let _ = writeln!(out, "skipped      {}", c.skipped);
        let _ = writeln!(out, "faults       {}", c.faults);
        let _ = writeln!(out, "backoff      {}", c.backoff_skips);
        let _ = writeln!(out, "deferrals    {}", c.deferrals);
        let _ = writeln!(out, "pending      {}", c.pending);
        let _ = writeln!(out, "cancelled    {}", c.cancelled);
        let _ = writeln!(
            out,
            "tick time    total {} ns, max {} ns over {} ticks",
            self.total_ns, self.max_ns, self.ticks_timed
        );
        if !self.nodes.is_empty() {
            let _ = writeln!(out, "step  calls  failures  backoff  disabled  auto");
            for row in &self.nodes {
                let _ = writeln!(
                    out,
                    "{:<5} {:<6} {:<9} {:<8} {:<9} {}",
                    row.step, row.calls, row.failures, row.backing_off, row.disabled, row.auto_off
                );
            }
        }
        out
    }

    /// The same content as JSON. Every key is fixed and every value is a number, a bool or a
    /// level name, so nothing needs escaping.
    pub fn json(&self) -> String {
        let c = &self.counters;
        let mut out = String::new();
        let _ = write!(
            out,
            "{{\"level\":\"{:?}\",\"ticks\":{},\"idle_ticks\":{},\"steps_called\":{},\"skipped\":{},\"faults\":{},\"backoff_skips\":{},\"deferrals\":{},\"pending\":{},\"cancelled\":{},\"ticks_timed\":{},\"total_ns\":{},\"max_ns\":{},\"nodes\":[",
            self.level,
            c.ticks,
            c.idle_ticks,
            c.steps_called,
            c.skipped,
            c.faults,
            c.backoff_skips,
            c.deferrals,
            c.pending,
            c.cancelled,
            self.ticks_timed,
            self.total_ns,
            self.max_ns,
        );
        for (index, row) in self.nodes.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            let _ = write!(
                out,
                "{{\"step\":{},\"calls\":{},\"failures\":{},\"backing_off\":{},\"disabled\":{},\"auto_off\":{}}}",
                row.step, row.calls, row.failures, row.backing_off, row.disabled, row.auto_off
            );
        }
        out.push_str("]}");
        out
    }
}
