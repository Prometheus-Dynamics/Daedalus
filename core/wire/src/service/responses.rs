//! What the server answers, and the events a watch delivers.

use super::errors::{Diagnostic, ErrorCode};
use super::fields::{
    TypedValue, bad, flag, get_list, get_opt_str, get_strs, put_list, put_opt_str, put_strs,
};
use super::requests::{OpKind, PlanForm, RunId, SubId, TelemetryLevel};
use crate::codec::{Reader, Writer};
use crate::{Schema, WireError};

/// A list of names cut at a bound, with the exact count.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Names {
    pub names: Vec<String>,
    pub total: u32,
}

impl Names {
    fn encode(&self, out: &mut Writer) -> Result<(), WireError> {
        out.record(|out| {
            put_strs(out, &self.names)?;
            out.u32(self.total);
            Ok(())
        })
    }

    fn decode(input: &mut Reader<'_>) -> Result<Names, WireError> {
        input.record(|input| {
            Ok(Names {
                names: get_strs(input)?,
                total: input.u32()?,
            })
        })
    }
}

/// What a loaded plugin brought.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PluginInfo {
    pub name: String,
    pub generation: u32,
    pub nodes: Vec<String>,
    pub backends: Vec<String>,
}

/// What planning a graph gave.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlanSummary {
    pub graph: String,
    pub hash: u64,
    pub form: PlanForm,
    pub nodes: u32,
    pub edges: u32,
    pub regions: u32,
    pub steps: u32,
    /// True when the plan came from the cache.
    pub cached: bool,
    /// What the lints found. Planning succeeded, so none is an error.
    pub diagnostics: Vec<Diagnostic>,
    /// The plan explained as text, when the request asked to inspect it.
    pub explain: Option<String>,
}

/// What a batch of ticks did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StepSummary {
    /// Ticks run by this request.
    pub ticks: u32,
    /// The run's tick number after them.
    pub tick: u64,
    pub step_calls: u64,
    pub faults: u64,
    pub skipped: u64,
    /// True when the last tick had nothing to do.
    pub idle: bool,
}

/// The state carry report of a reload.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CarryReport {
    /// Same body: state carried.
    pub carried: Names,
    /// Same node, new body: state started fresh.
    pub fresh: Names,
    pub added: Names,
    pub removed: Names,
    pub removed_ports: Names,
    /// Values dropped with removed ports.
    pub dropped_values: u32,
}

/// Where a run is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunState {
    Running,
    /// Cancel was asked. The run still exists until it is stopped.
    Cancelled,
    Stopped,
}

impl RunState {
    pub const ALL: [RunState; 3] = [RunState::Running, RunState::Cancelled, RunState::Stopped];

    pub const fn name(self) -> &'static str {
        match self {
            RunState::Running => "running",
            RunState::Cancelled => "cancelled",
            RunState::Stopped => "stopped",
        }
    }

    fn decode(input: &mut Reader<'_>) -> Result<RunState, WireError> {
        let tag = input.u8()?;
        RunState::ALL
            .into_iter()
            .find(|state| *state as u8 == tag)
            .ok_or(bad("run state", u32::from(tag)))
    }
}

/// One host port of a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortInfo {
    pub name: String,
    pub input: bool,
    pub type_key: String,
}

/// One run, as inspect shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunInfo {
    pub run: RunId,
    pub session: String,
    pub graph: String,
    pub state: RunState,
    pub tick: u64,
    pub faults: u64,
    pub ports: Vec<PortInfo>,
    pub nodes: Vec<String>,
}

impl RunInfo {
    fn encode(&self, out: &mut Writer) -> Result<(), WireError> {
        out.record(|out| {
            out.u64(self.run);
            out.str(&self.session)?;
            out.str(&self.graph)?;
            out.u8(self.state as u8);
            out.u64(self.tick);
            out.u64(self.faults);
            put_list(out, &self.ports, |out, port| {
                out.record(|out| {
                    out.str(&port.name)?;
                    out.bool(port.input);
                    out.str(&port.type_key)
                })
            })?;
            put_strs(out, &self.nodes)
        })
    }

    fn decode(input: &mut Reader<'_>) -> Result<RunInfo, WireError> {
        input.record(|input| {
            Ok(RunInfo {
                run: input.u64()?,
                session: input.str()?,
                graph: input.str()?,
                state: RunState::decode(input)?,
                tick: input.u64()?,
                faults: input.u64()?,
                ports: get_list(input, |input| {
                    input.record(|input| {
                        Ok(PortInfo {
                            name: input.str()?,
                            input: flag(input, "port direction")?,
                            type_key: input.str()?,
                        })
                    })
                })?,
                nodes: get_strs(input)?,
            })
        })
    }
}

/// The plan cache's counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheInfo {
    pub entries: u32,
    pub hits: u64,
    pub misses: u64,
}

/// One step's record in a run's telemetry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TelemetryRow {
    pub step: u32,
    /// The nodes the step covers.
    pub nodes: Vec<String>,
    pub calls: u32,
    pub failures: u32,
    pub backing_off: bool,
    pub disabled: bool,
    pub auto_off: bool,
}

/// A run's telemetry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TelemetryInfo {
    pub run: RunId,
    pub level: TelemetryLevel,
    pub tick: u64,
    pub step_calls: u64,
    pub faults: u64,
    pub skipped: u64,
    /// One row per step. Empty below the detailed level.
    pub rows: Vec<TelemetryRow>,
}

/// The host as a whole.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HostInfo {
    pub sessions: Vec<String>,
    /// The graphs stored in the request's session.
    pub graphs: Vec<String>,
    /// Every run, whichever session started it.
    pub runs: Vec<RunBrief>,
    pub plugins: Vec<String>,
}

/// One run in a host listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunBrief {
    pub run: RunId,
    pub session: String,
    pub graph: String,
    pub state: RunState,
}

/// What an inspect found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Inspection {
    Run(RunInfo),
    Graph { name: String, json: String },
    Cache(CacheInfo),
    Telemetry(TelemetryInfo),
    Host(HostInfo),
}

impl Inspection {
    fn encode(&self, out: &mut Writer) -> Result<(), WireError> {
        match self {
            Inspection::Run(info) => {
                out.u8(0);
                info.encode(out)?;
            }
            Inspection::Graph { name, json } => {
                out.u8(1);
                out.str(name)?;
                out.str(json)?;
            }
            Inspection::Cache(info) => {
                out.u8(2);
                out.u32(info.entries);
                out.u64(info.hits);
                out.u64(info.misses);
            }
            Inspection::Telemetry(info) => {
                out.u8(3);
                out.u64(info.run);
                out.u8(info.level as u8);
                out.u64(info.tick);
                out.u64(info.step_calls);
                out.u64(info.faults);
                out.u64(info.skipped);
                put_list(out, &info.rows, |out, row| {
                    out.record(|out| {
                        out.u32(row.step);
                        put_strs(out, &row.nodes)?;
                        out.u32(row.calls);
                        out.u32(row.failures);
                        out.bool(row.backing_off);
                        out.bool(row.disabled);
                        out.bool(row.auto_off);
                        Ok(())
                    })
                })?;
            }
            Inspection::Host(info) => {
                out.u8(4);
                put_strs(out, &info.sessions)?;
                put_strs(out, &info.graphs)?;
                put_list(out, &info.runs, |out, run| {
                    out.record(|out| {
                        out.u64(run.run);
                        out.str(&run.session)?;
                        out.str(&run.graph)?;
                        out.u8(run.state as u8);
                        Ok(())
                    })
                })?;
                put_strs(out, &info.plugins)?;
            }
        }
        Ok(())
    }

    fn decode(input: &mut Reader<'_>) -> Result<Inspection, WireError> {
        Ok(match input.u8()? {
            0 => Inspection::Run(RunInfo::decode(input)?),
            1 => Inspection::Graph {
                name: input.str()?,
                json: input.str()?,
            },
            2 => Inspection::Cache(CacheInfo {
                entries: input.u32()?,
                hits: input.u64()?,
                misses: input.u64()?,
            }),
            3 => Inspection::Telemetry(TelemetryInfo {
                run: input.u64()?,
                level: TelemetryLevel::decode(input)?,
                tick: input.u64()?,
                step_calls: input.u64()?,
                faults: input.u64()?,
                skipped: input.u64()?,
                rows: get_list(input, |input| {
                    input.record(|input| {
                        Ok(TelemetryRow {
                            step: input.u32()?,
                            nodes: get_strs(input)?,
                            calls: input.u32()?,
                            failures: input.u32()?,
                            backing_off: flag(input, "backing off")?,
                            disabled: flag(input, "disabled")?,
                            auto_off: flag(input, "auto off")?,
                        })
                    })
                })?,
            }),
            4 => Inspection::Host(HostInfo {
                sessions: get_strs(input)?,
                graphs: get_strs(input)?,
                runs: get_list(input, |input| {
                    input.record(|input| {
                        Ok(RunBrief {
                            run: input.u64()?,
                            session: input.str()?,
                            graph: input.str()?,
                            state: RunState::decode(input)?,
                        })
                    })
                })?,
                plugins: get_strs(input)?,
            }),
            tag => return Err(bad("inspection", u32::from(tag))),
        })
    }
}

/// One served request, as the trace keeps it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceEntry {
    /// Counts every request the server has served, so a gap shows what the ring dropped.
    pub seq: u64,
    pub id: u64,
    pub session: String,
    pub op: OpKind,
    pub target: String,
    /// `None` when the request succeeded.
    pub error: Option<ErrorCode>,
}

impl TraceEntry {
    fn encode(&self, out: &mut Writer) -> Result<(), WireError> {
        out.record(|out| {
            out.u64(self.seq);
            out.u64(self.id);
            out.str(&self.session)?;
            out.u8(self.op as u8);
            out.str(&self.target)?;
            out.u16(self.error.map_or(0, ErrorCode::code));
            Ok(())
        })
    }

    fn decode(input: &mut Reader<'_>) -> Result<TraceEntry, WireError> {
        input.record(|input| {
            Ok(TraceEntry {
                seq: input.u64()?,
                id: input.u64()?,
                session: input.str()?,
                // Another client's request may be for an op this side doesn't have.
                op: OpKind::named(input.u8()?),
                target: input.str()?,
                error: match input.u16()? {
                    0 => None,
                    code => Some(ErrorCode::from_code(code)),
                },
            })
        })
    }
}

/// A successful answer.
#[derive(Clone, Debug, PartialEq)]
pub enum Reply {
    Pong {
        version: u32,
    },
    /// Done, with nothing to say: unwatch, cancel, cache clear.
    Ack,
    Plugin(PluginInfo),
    /// A graph was stored or patched. The diagnostics are what the lints found in it.
    Graph {
        name: String,
        diagnostics: Vec<Diagnostic>,
    },
    Plan(PlanSummary),
    Started {
        run: RunId,
    },
    /// A feed was taken. `dropped` is true when the port's policy gave a value up for it.
    Fed {
        woke: u32,
        dropped: bool,
    },
    Stepped(StepSummary),
    /// What a take found. `None` when the port had nothing.
    Value(Option<TypedValue>),
    Watching {
        sub: SubId,
    },
    Reloaded(CarryReport),
    /// A run stopped. `dropped` counts the values an immediate stop threw away.
    Stopped {
        run: RunId,
        tick: u64,
        dropped: u32,
    },
    Inspection(Inspection),
    Trace(Vec<TraceEntry>),
}

fn put_diagnostics(out: &mut Writer, items: &[Diagnostic]) -> Result<(), WireError> {
    put_list(out, items, |out, item| item.encode(out))
}

impl Reply {
    pub(crate) fn encode(&self, out: &mut Writer, codecs: &Schema) -> Result<(), WireError> {
        match self {
            Reply::Pong { version } => {
                out.u8(0);
                out.u32(*version);
            }
            Reply::Ack => out.u8(1),
            Reply::Plugin(info) => {
                out.u8(2);
                out.str(&info.name)?;
                out.u32(info.generation);
                put_strs(out, &info.nodes)?;
                put_strs(out, &info.backends)?;
            }
            Reply::Graph { name, diagnostics } => {
                out.u8(3);
                out.str(name)?;
                put_diagnostics(out, diagnostics)?;
            }
            Reply::Plan(plan) => {
                out.u8(4);
                out.str(&plan.graph)?;
                out.u64(plan.hash);
                out.u8(plan.form as u8);
                out.u32(plan.nodes);
                out.u32(plan.edges);
                out.u32(plan.regions);
                out.u32(plan.steps);
                out.bool(plan.cached);
                put_diagnostics(out, &plan.diagnostics)?;
                put_opt_str(out, &plan.explain)?;
            }
            Reply::Started { run } => {
                out.u8(5);
                out.u64(*run);
            }
            Reply::Fed { woke, dropped } => {
                out.u8(6);
                out.u32(*woke);
                out.bool(*dropped);
            }
            Reply::Stepped(summary) => {
                out.u8(7);
                out.u32(summary.ticks);
                out.u64(summary.tick);
                out.u64(summary.step_calls);
                out.u64(summary.faults);
                out.u64(summary.skipped);
                out.bool(summary.idle);
            }
            Reply::Value(value) => {
                out.u8(8);
                match value {
                    None => out.u8(0),
                    Some(value) => {
                        out.u8(1);
                        value.encode(out, codecs)?;
                    }
                }
            }
            Reply::Watching { sub } => {
                out.u8(9);
                out.u64(*sub);
            }
            Reply::Reloaded(report) => {
                out.u8(10);
                for names in [
                    &report.carried,
                    &report.fresh,
                    &report.added,
                    &report.removed,
                    &report.removed_ports,
                ] {
                    names.encode(out)?;
                }
                out.u32(report.dropped_values);
            }
            Reply::Stopped { run, tick, dropped } => {
                out.u8(11);
                out.u64(*run);
                out.u64(*tick);
                out.u32(*dropped);
            }
            Reply::Inspection(inspection) => {
                out.u8(12);
                inspection.encode(out)?;
            }
            Reply::Trace(entries) => {
                out.u8(13);
                put_list(out, entries, |out, entry| entry.encode(out))?;
            }
        }
        Ok(())
    }

    pub(crate) fn decode(input: &mut Reader<'_>, codecs: &Schema) -> Result<Reply, WireError> {
        Ok(match input.u8()? {
            0 => Reply::Pong {
                version: input.u32()?,
            },
            1 => Reply::Ack,
            2 => Reply::Plugin(PluginInfo {
                name: input.str()?,
                generation: input.u32()?,
                nodes: get_strs(input)?,
                backends: get_strs(input)?,
            }),
            3 => Reply::Graph {
                name: input.str()?,
                diagnostics: get_list(input, Diagnostic::decode)?,
            },
            4 => Reply::Plan(PlanSummary {
                graph: input.str()?,
                hash: input.u64()?,
                form: PlanForm::decode(input)?,
                nodes: input.u32()?,
                edges: input.u32()?,
                regions: input.u32()?,
                steps: input.u32()?,
                cached: flag(input, "cached flag")?,
                diagnostics: get_list(input, Diagnostic::decode)?,
                explain: get_opt_str(input)?,
            }),
            5 => Reply::Started { run: input.u64()? },
            6 => Reply::Fed {
                woke: input.u32()?,
                dropped: flag(input, "dropped flag")?,
            },
            7 => Reply::Stepped(StepSummary {
                ticks: input.u32()?,
                tick: input.u64()?,
                step_calls: input.u64()?,
                faults: input.u64()?,
                skipped: input.u64()?,
                idle: flag(input, "idle flag")?,
            }),
            8 => Reply::Value(match flag(input, "value flag")? {
                false => None,
                true => Some(TypedValue::decode(input, codecs)?),
            }),
            9 => Reply::Watching { sub: input.u64()? },
            10 => Reply::Reloaded(CarryReport {
                carried: Names::decode(input)?,
                fresh: Names::decode(input)?,
                added: Names::decode(input)?,
                removed: Names::decode(input)?,
                removed_ports: Names::decode(input)?,
                dropped_values: input.u32()?,
            }),
            11 => Reply::Stopped {
                run: input.u64()?,
                tick: input.u64()?,
                dropped: input.u32()?,
            },
            12 => Reply::Inspection(Inspection::decode(input)?),
            13 => Reply::Trace(get_list(input, TraceEntry::decode)?),
            tag => return Err(bad("reply", u32::from(tag))),
        })
    }
}

/// Why a watch, or a run it follows, ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndReason {
    /// The run was stopped.
    Stopped,
    /// The run was cancelled.
    Cancelled,
    /// The client asked to unwatch.
    Unwatched,
    /// The server is shutting down.
    Shutdown,
}

impl EndReason {
    pub const ALL: [EndReason; 4] = [
        EndReason::Stopped,
        EndReason::Cancelled,
        EndReason::Unwatched,
        EndReason::Shutdown,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            EndReason::Stopped => "stopped",
            EndReason::Cancelled => "cancelled",
            EndReason::Unwatched => "unwatched",
            EndReason::Shutdown => "shutdown",
        }
    }
}

/// A step that failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FaultEvent {
    pub run: RunId,
    pub tick: u32,
    pub step: u32,
    /// The node at fault, when the body said or the step runs only one.
    pub node: Option<String>,
    /// Every node the step runs.
    pub covers: Vec<String>,
    /// Failures in a row.
    pub consecutive: u32,
    pub reason: String,
    /// The body's reason was longer than a fault keeps, and `reason` is only its start.
    pub reason_cut: bool,
    /// What the step gave the host beyond its reason, before it failed. Empty when it gave
    /// nothing, or the host keeps no detail.
    pub detail: String,
    /// The step wrote more detail than the host keeps, and `detail` is only its start.
    pub detail_cut: bool,
    /// The code of the failure: the node's own, unless `own_code`. Zero when the node gave
    /// none. Added after the first version: a server built before it sends none, and it reads
    /// as zero.
    pub code: u32,
    /// The code is one of Daedalus's own (`daedalus_abi::own`), and not the node's. Added with
    /// `code`.
    pub own_code: bool,
}

/// What one batch of ticks did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TelemetryEvent {
    pub run: RunId,
    pub tick: u64,
    pub step_calls: u64,
    pub faults: u64,
    pub skipped: u64,
}

/// What a watch delivers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventBody {
    Fault(FaultEvent),
    Telemetry(TelemetryEvent),
    /// The watcher was too slow: this many events were dropped, oldest first.
    Lagged {
        dropped: u64,
    },
    /// A run this watch follows ended, or the watch itself did. A watch on one run delivers
    /// nothing after this. `run` is zero when the watch ended without a run ending.
    Ended {
        run: RunId,
        reason: EndReason,
    },
    /// An event of a kind this side doesn't have: the server is newer. A watcher sees that
    /// something happened and what kind it was, and nothing is taken to be something else.
    Unknown {
        kind: u8,
    },
}

impl EventBody {
    pub(crate) fn encode(&self, out: &mut Writer) -> Result<(), WireError> {
        match self {
            EventBody::Fault(fault) => {
                out.u8(0);
                out.u64(fault.run);
                out.u32(fault.tick);
                out.u32(fault.step);
                put_opt_str(out, &fault.node)?;
                put_strs(out, &fault.covers)?;
                out.u32(fault.consecutive);
                out.str(&fault.reason)?;
                out.bool(fault.reason_cut);
                out.str(&fault.detail)?;
                out.bool(fault.detail_cut);
                out.u32(fault.code);
                out.bool(fault.own_code);
            }
            EventBody::Telemetry(event) => {
                out.u8(1);
                out.u64(event.run);
                out.u64(event.tick);
                out.u64(event.step_calls);
                out.u64(event.faults);
                out.u64(event.skipped);
            }
            EventBody::Lagged { dropped } => {
                out.u8(2);
                out.u64(*dropped);
            }
            EventBody::Ended { run, reason } => {
                out.u8(3);
                out.u64(*run);
                out.u8(*reason as u8);
            }
            EventBody::Unknown { kind } => out.u8(*kind),
        }
        Ok(())
    }

    pub(crate) fn decode(input: &mut Reader<'_>) -> Result<EventBody, WireError> {
        Ok(match input.u8()? {
            0 => {
                let mut fault = FaultEvent {
                    run: input.u64()?,
                    tick: input.u32()?,
                    step: input.u32()?,
                    node: get_opt_str(input)?,
                    covers: get_strs(input)?,
                    consecutive: input.u32()?,
                    reason: input.str()?,
                    reason_cut: flag(input, "reason cut")?,
                    detail: input.str()?,
                    detail_cut: flag(input, "detail cut")?,
                    code: 0,
                    own_code: false,
                };
                // Added together, after the first version: both are there, or neither is. A
                // code without its marker could be the host's own read as a node's.
                if input.more() {
                    fault.code = input.u32()?;
                    fault.own_code = flag(input, "own code")?;
                }
                EventBody::Fault(fault)
            }
            1 => EventBody::Telemetry(TelemetryEvent {
                run: input.u64()?,
                tick: input.u64()?,
                step_calls: input.u64()?,
                faults: input.u64()?,
                skipped: input.u64()?,
            }),
            2 => EventBody::Lagged {
                dropped: input.u64()?,
            },
            3 => {
                let run = input.u64()?;
                let tag = input.u8()?;
                let reason = EndReason::ALL
                    .into_iter()
                    .find(|reason| *reason as u8 == tag)
                    .ok_or(bad("end reason", u32::from(tag)))?;
                EventBody::Ended { run, reason }
            }
            kind => EventBody::Unknown { kind },
        })
    }
}
