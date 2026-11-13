//! A run: one loaded graph the host holds. It is stepped by whoever asks, one tick at a time, so
//! a cancel from another client lands between two ticks, or inside one through the cancel word.

use std::string::String;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::vec::Vec;

use daedalus_runner::owned_run::CancelHandle;
use daedalus_runner::{RunError, TickReport};
use daedalus_wire::service::{
    ErrorCode, FaultEvent, PortInfo, RunBrief, RunId, RunInfo, RunState, ServiceError, StartConfig,
    TelemetryInfo, TelemetryLevel, TelemetryRow,
};

use super::lock;
use crate::{Dir, HostError, Level, LoadedHost, Telemetry};

/// The graph and the totals of one run.
pub(crate) struct Run {
    pub(crate) host: LoadedHost,
    pub(crate) config: StartConfig,
    pub(crate) tick: u64,
    pub(crate) step_calls: u64,
    pub(crate) faults: u64,
    pub(crate) skipped: u64,
}

/// A run and what can be asked of it without waiting for a tick to end.
pub(crate) struct RunEntry {
    pub(crate) id: RunId,
    pub(crate) session: String,
    pub(crate) graph: Mutex<String>,
    cancel: CancelHandle,
    cancelled: AtomicBool,
    stopped: AtomicBool,
    pub(crate) run: Mutex<Run>,
}

fn level(level: TelemetryLevel) -> Level {
    match level {
        TelemetryLevel::Off => Level::Off,
        TelemetryLevel::Basic => Level::Basic,
        TelemetryLevel::Detailed => Level::Detailed,
        TelemetryLevel::Trace => Level::Trace,
    }
}

/// What one tick did, for the caller's totals and for watchers.
pub(crate) struct Ticked {
    pub(crate) report: TickReport,
    pub(crate) faults: Vec<FaultEvent>,
}

impl RunEntry {
    pub(crate) fn new(
        id: RunId,
        session: &str,
        graph: &str,
        host: LoadedHost,
        config: StartConfig,
    ) -> RunEntry {
        RunEntry {
            id,
            session: String::from(session),
            graph: Mutex::new(String::from(graph)),
            cancel: host.cancel_handle(),
            cancelled: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            run: Mutex::new(Run {
                host,
                config,
                tick: 0,
                step_calls: 0,
                faults: 0,
                skipped: 0,
            }),
        }
    }

    pub(crate) fn state(&self) -> RunState {
        if self.stopped.load(Ordering::SeqCst) {
            RunState::Stopped
        } else if self.cancelled.load(Ordering::SeqCst) {
            RunState::Cancelled
        } else {
            RunState::Running
        }
    }

    /// Refuses a request that needs a run that is still going.
    pub(crate) fn live(&self) -> Result<(), ServiceError> {
        match self.state() {
            RunState::Running => Ok(()),
            RunState::Cancelled => Err(ServiceError::new(
                ErrorCode::Cancelled,
                std::format!("run {} was cancelled", self.id),
            )),
            RunState::Stopped => Err(ServiceError::new(
                ErrorCode::Stopped,
                std::format!("run {} was stopped", self.id),
            )),
        }
    }

    /// Cancels the run. A tick in progress stops at its next step boundary, and a body that
    /// checks for a cancel stops sooner. The run takes no more feeds or steps.
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.cancel.cancel();
    }

    pub(crate) fn mark_stopped(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    /// Runs one tick at the host time `now_ns`.
    pub(crate) fn tick(&self, now_ns: u64) -> Result<Ticked, ServiceError> {
        self.live()?;
        let mut run = lock(&self.run);
        run.host.host().set_clock(now_ns);
        let report = match run.host.step() {
            Ok(report) => report,
            Err(HostError::Run(RunError::Cancelled { .. })) => {
                return Err(ServiceError::new(
                    ErrorCode::Cancelled,
                    std::format!("run {} was cancelled during a tick", self.id),
                ));
            }
            Err(error) => {
                return Err(ServiceError::new(
                    ErrorCode::InvalidRequest,
                    std::format!(
                        "run {} could not tick: {}",
                        self.id,
                        crate::render_error(&error)
                    ),
                ));
            }
        };
        run.tick += 1;
        run.step_calls += u64::from(report.step_calls);
        run.faults += u64::from(report.faults);
        run.skipped += u64::from(report.skipped);
        let faults = if report.faults == 0 {
            Vec::new()
        } else {
            let id = self.id;
            run.host
                .fault_reports()
                .filter(|fault| fault.tick == report.tick)
                .map(|fault| FaultEvent {
                    run: id,
                    tick: fault.tick,
                    step: fault.step,
                    node: fault.node().map(String::from),
                    covers: fault
                        .covers
                        .iter()
                        .map(|name| String::from(*name))
                        .collect(),
                    consecutive: fault.consecutive,
                    reason: String::from(fault.reason()),
                    reason_cut: fault.reason_cut,
                })
                .collect()
        };
        Ok(Ticked { report, faults })
    }

    pub(crate) fn brief(&self) -> RunBrief {
        RunBrief {
            run: self.id,
            session: self.session.clone(),
            graph: lock(&self.graph).clone(),
            state: self.state(),
        }
    }

    pub(crate) fn info(&self) -> RunInfo {
        let state = self.state();
        let graph = lock(&self.graph).clone();
        let mut run = lock(&self.run);
        let ports = run
            .host
            .host()
            .port_infos()
            .map(|port| PortInfo {
                name: String::from(port.name),
                input: port.dir == Dir::Input,
                type_key: String::from(port.type_key),
            })
            .collect();
        RunInfo {
            run: self.id,
            session: self.session.clone(),
            graph,
            state,
            tick: run.tick,
            faults: run.faults,
            ports,
            nodes: run.host.node_keys().map(String::from).collect(),
        }
    }

    pub(crate) fn telemetry(&self) -> TelemetryInfo {
        let mut run = lock(&self.run);
        let wanted = run.config.telemetry;
        let covers: Vec<Vec<String>> = run
            .host
            .covers
            .iter()
            .map(|covers| covers.iter().map(|name| String::from(*name)).collect())
            .collect();
        let report = Telemetry::new(level(wanted)).report(run.host.host().runner());
        let rows = report
            .nodes
            .iter()
            .map(|row| TelemetryRow {
                step: row.step,
                nodes: covers.get(row.step as usize).cloned().unwrap_or_default(),
                calls: row.calls,
                failures: row.failures,
                backing_off: row.backing_off,
                disabled: row.disabled,
                auto_off: row.auto_off,
            })
            .collect();
        TelemetryInfo {
            run: self.id,
            level: wanted,
            tick: run.tick,
            step_calls: run.step_calls,
            faults: run.faults,
            skipped: run.skipped,
            rows,
        }
    }

    /// How many values wait in the run's host ports.
    pub(crate) fn queued(&self) -> u32 {
        let mut run = lock(&self.run);
        run.host.host().port_infos().map(|port| port.queued).sum()
    }
}
