//! What a client asks for. An op is one kind byte, then its fields in fixed order.

use super::fields::{TypedValue, bad, flag, get_strs, put_strs};
use crate::codec::{Reader, Writer};
use crate::{Schema, WireError};

/// A run's number. The server gives one out when a run starts, and never reuses it.
pub type RunId = u64;
/// A watch subscription's number.
pub type SubId = u64;

/// The form a plan is asked for in.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PlanForm {
    #[default]
    Steps,
    Blob,
    Source,
}

impl PlanForm {
    pub const ALL: [PlanForm; 3] = [PlanForm::Steps, PlanForm::Blob, PlanForm::Source];

    pub const fn name(self) -> &'static str {
        match self {
            PlanForm::Steps => "steps",
            PlanForm::Blob => "blob",
            PlanForm::Source => "source",
        }
    }

    pub(crate) fn decode(input: &mut Reader<'_>) -> Result<PlanForm, WireError> {
        let tag = input.u8()?;
        PlanForm::ALL
            .into_iter()
            .find(|form| *form as u8 == tag)
            .ok_or(bad("plan form", u32::from(tag)))
    }
}

/// How to plan a graph.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlanOptions {
    pub form: PlanForm,
    /// Also return the plan's explanation as text.
    pub inspect: bool,
    /// Backend ids in the order ties go to. Empty leaves the host's own order.
    pub priority: Vec<String>,
}

impl PlanOptions {
    fn encode(&self, out: &mut Writer) -> Result<(), WireError> {
        out.u8(self.form as u8);
        out.bool(self.inspect);
        put_strs(out, &self.priority)
    }

    fn decode(input: &mut Reader<'_>) -> Result<PlanOptions, WireError> {
        Ok(PlanOptions {
            form: PlanForm::decode(input)?,
            inspect: flag(input, "inspect flag")?,
            priority: get_strs(input)?,
        })
    }
}

/// How much a run records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TelemetryLevel {
    Off,
    #[default]
    Basic,
    Detailed,
    Trace,
}

impl TelemetryLevel {
    pub const ALL: [TelemetryLevel; 4] = [
        TelemetryLevel::Off,
        TelemetryLevel::Basic,
        TelemetryLevel::Detailed,
        TelemetryLevel::Trace,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            TelemetryLevel::Off => "off",
            TelemetryLevel::Basic => "basic",
            TelemetryLevel::Detailed => "detailed",
            TelemetryLevel::Trace => "trace",
        }
    }

    pub(crate) fn decode(input: &mut Reader<'_>) -> Result<TelemetryLevel, WireError> {
        let tag = input.u8()?;
        TelemetryLevel::ALL
            .into_iter()
            .find(|level| *level as u8 == tag)
            .ok_or(bad("telemetry level", u32::from(tag)))
    }
}

/// How to start a run. A zero leaves the host's default.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StartConfig {
    /// The capacity of a FIFO edge that declares none.
    pub fifo_capacity: u32,
    /// How many failure reports the run keeps.
    pub fault_reports: u32,
    pub telemetry: TelemetryLevel,
    /// Run every node as its own step, for finding a faulty node.
    pub unmerged: bool,
    /// Backend ids in the order ties go to. Empty leaves the host's own order.
    pub priority: Vec<String>,
}

impl StartConfig {
    fn encode(&self, out: &mut Writer) -> Result<(), WireError> {
        out.u32(self.fifo_capacity);
        out.u32(self.fault_reports);
        out.u8(self.telemetry as u8);
        out.bool(self.unmerged);
        put_strs(out, &self.priority)
    }

    fn decode(input: &mut Reader<'_>) -> Result<StartConfig, WireError> {
        Ok(StartConfig {
            fifo_capacity: input.u32()?,
            fault_reports: input.u32()?,
            telemetry: TelemetryLevel::decode(input)?,
            unmerged: flag(input, "unmerged flag")?,
            priority: get_strs(input)?,
        })
    }
}

/// What a watch follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchTarget {
    /// Every run, including ones started later.
    All,
    Run(RunId),
}

/// The event kinds a watch asks for. `lagged` and `ended` always arrive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventKinds(pub u8);

impl EventKinds {
    pub const FAULT: EventKinds = EventKinds(1);
    pub const TELEMETRY: EventKinds = EventKinds(2);
    pub const ALL: EventKinds = EventKinds(3);

    pub const fn has(self, kind: EventKinds) -> bool {
        self.0 & kind.0 == kind.0
    }

    pub const fn with(self, kind: EventKinds) -> EventKinds {
        EventKinds(self.0 | kind.0)
    }
}

/// How a run stops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopMode {
    /// Finish what is waiting, then stop.
    Drain,
    /// Stop now. What is waiting is dropped.
    Immediate,
}

/// What an inspect looks at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InspectTarget {
    Run(RunId),
    /// A stored graph, by name, in the request's session.
    Graph(String),
    /// The plan cache.
    Cache,
    /// A run's telemetry.
    Telemetry(RunId),
    /// The host as a whole: its sessions, graphs, runs and plugins.
    Host,
}

/// What a cache request does. Looking at the cache is an inspect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheAction {
    Clear,
}

/// One request.
#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    LoadPlugin {
        path: String,
    },
    /// Stores a graph document: JSON text in the graph format.
    PutGraph {
        name: String,
        json: String,
    },
    /// Applies a patch document (JSON text) to a stored graph.
    PatchGraph {
        graph: String,
        patch: String,
    },
    Plan {
        graph: String,
        options: PlanOptions,
    },
    Start {
        graph: String,
        config: StartConfig,
    },
    Feed {
        run: RunId,
        port: String,
        value: TypedValue,
    },
    Step {
        run: RunId,
        count: u32,
    },
    Take {
        run: RunId,
        port: String,
    },
    Watch {
        target: WatchTarget,
        kinds: EventKinds,
    },
    Unwatch {
        sub: SubId,
    },
    Reload {
        run: RunId,
        graph: String,
    },
    Cancel {
        run: RunId,
    },
    Stop {
        run: RunId,
        mode: StopMode,
    },
    Inspect {
        target: InspectTarget,
    },
    Cache {
        action: CacheAction,
    },
    Trace {
        limit: u32,
    },
    Ping,
}

/// Which op a request is, without its fields. The discriminant is the op's kind byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OpKind {
    LoadPlugin = 1,
    PutGraph = 2,
    PatchGraph = 3,
    Plan = 4,
    Start = 5,
    Feed = 6,
    Step = 7,
    Take = 8,
    Watch = 9,
    Unwatch = 10,
    Reload = 11,
    Cancel = 12,
    Stop = 13,
    Inspect = 14,
    Cache = 15,
    Trace = 16,
    Ping = 17,
}

impl OpKind {
    pub const ALL: [OpKind; 17] = [
        OpKind::LoadPlugin,
        OpKind::PutGraph,
        OpKind::PatchGraph,
        OpKind::Plan,
        OpKind::Start,
        OpKind::Feed,
        OpKind::Step,
        OpKind::Take,
        OpKind::Watch,
        OpKind::Unwatch,
        OpKind::Reload,
        OpKind::Cancel,
        OpKind::Stop,
        OpKind::Inspect,
        OpKind::Cache,
        OpKind::Trace,
        OpKind::Ping,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            OpKind::LoadPlugin => "load_plugin",
            OpKind::PutGraph => "put_graph",
            OpKind::PatchGraph => "patch_graph",
            OpKind::Plan => "plan",
            OpKind::Start => "start",
            OpKind::Feed => "feed",
            OpKind::Step => "step",
            OpKind::Take => "take",
            OpKind::Watch => "watch",
            OpKind::Unwatch => "unwatch",
            OpKind::Reload => "reload",
            OpKind::Cancel => "cancel",
            OpKind::Stop => "stop",
            OpKind::Inspect => "inspect",
            OpKind::Cache => "cache",
            OpKind::Trace => "trace",
            OpKind::Ping => "ping",
        }
    }

    pub fn from_byte(byte: u8) -> Result<OpKind, WireError> {
        OpKind::ALL
            .into_iter()
            .find(|kind| *kind as u8 == byte)
            .ok_or(bad("op", u32::from(byte)))
    }

    /// True when sending the op twice does what sending it once does, so a client may resend it
    /// after a disconnect: ping, inspect and trace.
    pub const fn idempotent(self) -> bool {
        matches!(self, OpKind::Ping | OpKind::Inspect | OpKind::Trace)
    }
}

impl Op {
    pub const fn kind(&self) -> OpKind {
        match self {
            Op::LoadPlugin { .. } => OpKind::LoadPlugin,
            Op::PutGraph { .. } => OpKind::PutGraph,
            Op::PatchGraph { .. } => OpKind::PatchGraph,
            Op::Plan { .. } => OpKind::Plan,
            Op::Start { .. } => OpKind::Start,
            Op::Feed { .. } => OpKind::Feed,
            Op::Step { .. } => OpKind::Step,
            Op::Take { .. } => OpKind::Take,
            Op::Watch { .. } => OpKind::Watch,
            Op::Unwatch { .. } => OpKind::Unwatch,
            Op::Reload { .. } => OpKind::Reload,
            Op::Cancel { .. } => OpKind::Cancel,
            Op::Stop { .. } => OpKind::Stop,
            Op::Inspect { .. } => OpKind::Inspect,
            Op::Cache { .. } => OpKind::Cache,
            Op::Trace { .. } => OpKind::Trace,
            Op::Ping => OpKind::Ping,
        }
    }

    /// What the op is about, for the trace: a graph name, a run number, a path.
    pub fn target(&self) -> String {
        match self {
            Op::LoadPlugin { path } => path.clone(),
            Op::PutGraph { name, .. } => name.clone(),
            Op::PatchGraph { graph, .. } | Op::Plan { graph, .. } | Op::Start { graph, .. } => {
                graph.clone()
            }
            Op::Feed { run, port, .. } | Op::Take { run, port } => format!("{run}:{port}"),
            Op::Step { run, .. } | Op::Cancel { run } | Op::Stop { run, .. } => run.to_string(),
            Op::Reload { run, graph } => format!("{run}:{graph}"),
            Op::Watch { target, .. } => match target {
                WatchTarget::All => "all".to_owned(),
                WatchTarget::Run(run) => run.to_string(),
            },
            Op::Unwatch { sub } => sub.to_string(),
            Op::Inspect { target } => match target {
                InspectTarget::Run(run) => format!("run:{run}"),
                InspectTarget::Graph(name) => format!("graph:{name}"),
                InspectTarget::Cache => "cache".to_owned(),
                InspectTarget::Telemetry(run) => format!("telemetry:{run}"),
                InspectTarget::Host => "host".to_owned(),
            },
            Op::Cache { .. } | Op::Trace { .. } | Op::Ping => String::new(),
        }
    }

    pub(crate) fn encode(&self, out: &mut Writer, codecs: &Schema) -> Result<(), WireError> {
        out.u8(self.kind() as u8);
        match self {
            Op::LoadPlugin { path } => out.str(path)?,
            Op::PutGraph { name, json } => {
                out.str(name)?;
                out.str(json)?;
            }
            Op::PatchGraph { graph, patch } => {
                out.str(graph)?;
                out.str(patch)?;
            }
            Op::Plan { graph, options } => {
                out.str(graph)?;
                options.encode(out)?;
            }
            Op::Start { graph, config } => {
                out.str(graph)?;
                config.encode(out)?;
            }
            Op::Feed { run, port, value } => {
                out.u64(*run);
                out.str(port)?;
                value.encode(out, codecs)?;
            }
            Op::Step { run, count } => {
                out.u64(*run);
                out.u32(*count);
            }
            Op::Take { run, port } => {
                out.u64(*run);
                out.str(port)?;
            }
            Op::Watch { target, kinds } => {
                match target {
                    WatchTarget::All => out.u8(0),
                    WatchTarget::Run(run) => {
                        out.u8(1);
                        out.u64(*run);
                    }
                }
                out.u8(kinds.0);
            }
            Op::Unwatch { sub } => out.u64(*sub),
            Op::Reload { run, graph } => {
                out.u64(*run);
                out.str(graph)?;
            }
            Op::Cancel { run } => out.u64(*run),
            Op::Stop { run, mode } => {
                out.u64(*run);
                out.u8(match mode {
                    StopMode::Drain => 0,
                    StopMode::Immediate => 1,
                });
            }
            Op::Inspect { target } => match target {
                InspectTarget::Run(run) => {
                    out.u8(0);
                    out.u64(*run);
                }
                InspectTarget::Graph(name) => {
                    out.u8(1);
                    out.str(name)?;
                }
                InspectTarget::Cache => out.u8(2),
                InspectTarget::Telemetry(run) => {
                    out.u8(3);
                    out.u64(*run);
                }
                InspectTarget::Host => out.u8(4),
            },
            Op::Cache { action } => out.u8(match action {
                CacheAction::Clear => 0,
            }),
            Op::Trace { limit } => out.u32(*limit),
            Op::Ping => {}
        }
        Ok(())
    }

    pub(crate) fn decode(input: &mut Reader<'_>, codecs: &Schema) -> Result<Op, WireError> {
        Ok(match OpKind::from_byte(input.u8()?)? {
            OpKind::LoadPlugin => Op::LoadPlugin { path: input.str()? },
            OpKind::PutGraph => Op::PutGraph {
                name: input.str()?,
                json: input.str()?,
            },
            OpKind::PatchGraph => Op::PatchGraph {
                graph: input.str()?,
                patch: input.str()?,
            },
            OpKind::Plan => Op::Plan {
                graph: input.str()?,
                options: PlanOptions::decode(input)?,
            },
            OpKind::Start => Op::Start {
                graph: input.str()?,
                config: StartConfig::decode(input)?,
            },
            OpKind::Feed => Op::Feed {
                run: input.u64()?,
                port: input.str()?,
                value: TypedValue::decode(input, codecs)?,
            },
            OpKind::Step => Op::Step {
                run: input.u64()?,
                count: input.u32()?,
            },
            OpKind::Take => Op::Take {
                run: input.u64()?,
                port: input.str()?,
            },
            OpKind::Watch => {
                let target = match input.u8()? {
                    0 => WatchTarget::All,
                    1 => WatchTarget::Run(input.u64()?),
                    tag => return Err(bad("watch target", u32::from(tag))),
                };
                let kinds = input.u8()?;
                if kinds & !EventKinds::ALL.0 != 0 {
                    return Err(bad("event kinds", u32::from(kinds)));
                }
                Op::Watch {
                    target,
                    kinds: EventKinds(kinds),
                }
            }
            OpKind::Unwatch => Op::Unwatch { sub: input.u64()? },
            OpKind::Reload => Op::Reload {
                run: input.u64()?,
                graph: input.str()?,
            },
            OpKind::Cancel => Op::Cancel { run: input.u64()? },
            OpKind::Stop => Op::Stop {
                run: input.u64()?,
                mode: match input.u8()? {
                    0 => StopMode::Drain,
                    1 => StopMode::Immediate,
                    tag => return Err(bad("stop mode", u32::from(tag))),
                },
            },
            OpKind::Inspect => Op::Inspect {
                target: match input.u8()? {
                    0 => InspectTarget::Run(input.u64()?),
                    1 => InspectTarget::Graph(input.str()?),
                    2 => InspectTarget::Cache,
                    3 => InspectTarget::Telemetry(input.u64()?),
                    4 => InspectTarget::Host,
                    tag => return Err(bad("inspect target", u32::from(tag))),
                },
            },
            OpKind::Cache => Op::Cache {
                action: match input.u8()? {
                    0 => CacheAction::Clear,
                    tag => return Err(bad("cache action", u32::from(tag))),
                },
            },
            OpKind::Trace => Op::Trace {
                limit: input.u32()?,
            },
            OpKind::Ping => Op::Ping,
        })
    }
}
