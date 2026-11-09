//! Host telemetry. The runner keeps fixed counters; this module adds the clock, the levels, the
//! event log and the reports. Nothing here is in the runner's tick path, except the counters.

use std::vec::Vec;
mod events;
mod levels;
mod node;
mod report;

use daedalus_runner::{RunError, Runner, TickReport};

pub use events::{Event, EventLog};
pub use levels::Level;
pub use node::NodeRow;
pub use report::ExecutionReport;

/// The host's telemetry state for one graph.
#[derive(Debug)]
pub struct Telemetry {
    level: Level,
    log: EventLog,
    ticks_timed: u64,
    total_ns: u64,
    max_ns: u64,
}

impl Telemetry {
    /// Telemetry at `level`, with an event log that keeps up to 256 events once it is turned on.
    pub fn new(level: Level) -> Telemetry {
        Telemetry {
            level,
            log: EventLog::new(256),
            ticks_timed: 0,
            total_ns: 0,
            max_ns: 0,
        }
    }

    pub fn level(&self) -> Level {
        self.level
    }

    pub fn set_level(&mut self, level: Level) {
        self.level = level;
    }

    pub fn log(&self) -> &EventLog {
        &self.log
    }

    pub fn log_mut(&mut self) -> &mut EventLog {
        &mut self.log
    }

    /// Runs one tick. `clock` is read before and after it, and its readings time the tick. The
    /// first reading is also the clock the runner uses for backoff. Nothing allocates at Basic
    /// or Off.
    pub fn tick(
        &mut self,
        runner: &mut Runner<'_>,
        clock: &mut dyn FnMut() -> u64,
    ) -> Result<TickReport, RunError> {
        let start = clock();
        runner.set_clock(start);
        let result = runner.tick();
        if self.level != Level::Off {
            let elapsed = clock().saturating_sub(start);
            self.ticks_timed += 1;
            self.total_ns = self.total_ns.saturating_add(elapsed);
            self.max_ns = self.max_ns.max(elapsed);
        }
        if let (Ok(report), Level::Trace) = (&result, self.level) {
            self.log.push(Event {
                tick: report.tick,
                step_calls: report.step_calls,
                faults: report.faults,
                skipped: report.skipped,
            });
        }
        result
    }

    /// The report as of now. Rows for each step come only at Detailed and above.
    pub fn report(&self, runner: &Runner<'_>) -> ExecutionReport {
        let nodes = match self.level {
            Level::Detailed | Level::Trace => node::rows(runner),
            Level::Off | Level::Basic => Vec::new(),
        };
        ExecutionReport {
            level: self.level,
            counters: runner.counters(),
            ticks_timed: self.ticks_timed,
            total_ns: self.total_ns,
            max_ns: self.max_ns,
            nodes,
        }
    }
}
