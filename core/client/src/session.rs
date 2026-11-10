//! One typed method per op. Each sends the op in the client's session and waits for its answer.

use daedalus_wire::service::{
    CacheAction, CacheInfo, CarryReport, Diagnostic, EventKinds, InspectTarget, Inspection, Op,
    PlanOptions, PlanSummary, PluginInfo, Reply, RunId, SESSION_CAP, StartConfig, StepSummary,
    StopMode, TraceEntry, TypedValue, WatchTarget,
};

use crate::connect::{Client, ClientError};
use crate::watch::Watch;

/// What a feed did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fed {
    /// How many consumer steps were woken.
    pub woke: u32,
    /// True when the port's policy gave a value up for this one.
    pub dropped: bool,
}

/// How a run ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stopped {
    /// The run's last tick.
    pub tick: u64,
    /// Values thrown away by an immediate stop.
    pub dropped: u32,
}

fn unexpected<T>(op: &'static str, reply: Reply) -> Result<T, ClientError> {
    Err(ClientError::Unexpected {
        op,
        reply: format!("{reply:?}"),
    })
}

impl Client {
    /// The same connection, with requests in the session `name`. A session is a namespace for
    /// graph names. Runs are visible from every session.
    pub fn in_session(&self, name: &str) -> Result<Client, ClientError> {
        if name.is_empty() || name.len() > SESSION_CAP {
            return Err(ClientError::BadSession(name.len()));
        }
        Ok(self.with_session(name))
    }

    /// Asks the server for its protocol version.
    pub fn ping(&self) -> Result<u32, ClientError> {
        match self.call(Op::Ping)? {
            Reply::Pong { version } => Ok(version),
            other => unexpected("ping", other),
        }
    }

    pub fn load_plugin(&self, path: &str) -> Result<PluginInfo, ClientError> {
        let path = path.to_owned();
        match self.call(Op::LoadPlugin { path })? {
            Reply::Plugin(info) => Ok(info),
            other => unexpected("load_plugin", other),
        }
    }

    /// Stores a graph document under `name`. Returns what the lints found in it.
    pub fn put_graph(&self, name: &str, json: &str) -> Result<Vec<Diagnostic>, ClientError> {
        let op = Op::PutGraph {
            name: name.to_owned(),
            json: json.to_owned(),
        };
        match self.call(op)? {
            Reply::Graph { diagnostics, .. } => Ok(diagnostics),
            other => unexpected("put_graph", other),
        }
    }

    /// Applies a patch document to a stored graph.
    pub fn patch_graph(&self, graph: &str, patch: &str) -> Result<Vec<Diagnostic>, ClientError> {
        let op = Op::PatchGraph {
            graph: graph.to_owned(),
            patch: patch.to_owned(),
        };
        match self.call(op)? {
            Reply::Graph { diagnostics, .. } => Ok(diagnostics),
            other => unexpected("patch_graph", other),
        }
    }

    pub fn plan(&self, graph: &str, options: PlanOptions) -> Result<PlanSummary, ClientError> {
        let graph = graph.to_owned();
        match self.call(Op::Plan { graph, options })? {
            Reply::Plan(summary) => Ok(summary),
            other => unexpected("plan", other),
        }
    }

    /// Plans a stored graph and starts a run of it.
    pub fn start(&self, graph: &str, config: StartConfig) -> Result<RunId, ClientError> {
        let graph = graph.to_owned();
        match self.call(Op::Start { graph, config })? {
            Reply::Started { run } => Ok(run),
            other => unexpected("start", other),
        }
    }

    pub fn feed(&self, run: RunId, port: &str, value: TypedValue) -> Result<Fed, ClientError> {
        let port = port.to_owned();
        match self.call(Op::Feed { run, port, value })? {
            Reply::Fed { woke, dropped } => Ok(Fed { woke, dropped }),
            other => unexpected("feed", other),
        }
    }

    /// Runs `count` ticks.
    pub fn step(&self, run: RunId, count: u32) -> Result<StepSummary, ClientError> {
        match self.call(Op::Step { run, count })? {
            Reply::Stepped(summary) => Ok(summary),
            other => unexpected("step", other),
        }
    }

    /// Takes the oldest value out of an output port. `None` when the port is empty.
    pub fn take(&self, run: RunId, port: &str) -> Result<Option<TypedValue>, ClientError> {
        let port = port.to_owned();
        match self.call(Op::Take { run, port })? {
            Reply::Value(value) => Ok(value),
            other => unexpected("take", other),
        }
    }

    /// Subscribes to events of one run, or of every run.
    pub fn watch(&self, target: WatchTarget, kinds: EventKinds) -> Result<Watch, ClientError> {
        self.subscribe(target, kinds)
    }

    /// Replaces a run's graph with a stored one. Returns what happened to each node's state.
    pub fn reload(&self, run: RunId, graph: &str) -> Result<CarryReport, ClientError> {
        let graph = graph.to_owned();
        match self.call(Op::Reload { run, graph })? {
            Reply::Reloaded(report) => Ok(report),
            other => unexpected("reload", other),
        }
    }

    pub fn cancel(&self, run: RunId) -> Result<(), ClientError> {
        match self.call(Op::Cancel { run })? {
            Reply::Ack => Ok(()),
            other => unexpected("cancel", other),
        }
    }

    pub fn stop(&self, run: RunId, mode: StopMode) -> Result<Stopped, ClientError> {
        match self.call(Op::Stop { run, mode })? {
            Reply::Stopped { tick, dropped, .. } => Ok(Stopped { tick, dropped }),
            other => unexpected("stop", other),
        }
    }

    pub fn inspect(&self, target: InspectTarget) -> Result<Inspection, ClientError> {
        match self.call(Op::Inspect { target })? {
            Reply::Inspection(inspection) => Ok(inspection),
            other => unexpected("inspect", other),
        }
    }

    /// The plan cache's counters.
    pub fn cache_inspect(&self) -> Result<CacheInfo, ClientError> {
        match self.inspect(InspectTarget::Cache)? {
            Inspection::Cache(info) => Ok(info),
            other => Err(ClientError::Unexpected {
                op: "inspect",
                reply: format!("{other:?}"),
            }),
        }
    }

    pub fn cache_clear(&self) -> Result<(), ClientError> {
        let action = CacheAction::Clear;
        match self.call(Op::Cache { action })? {
            Reply::Ack => Ok(()),
            other => unexpected("cache", other),
        }
    }

    /// The newest `limit` served requests, oldest first. Zero is all the server keeps.
    pub fn trace(&self, limit: u32) -> Result<Vec<TraceEntry>, ClientError> {
        match self.call(Op::Trace { limit })? {
            Reply::Trace(entries) => Ok(entries),
            other => unexpected("trace", other),
        }
    }
}
