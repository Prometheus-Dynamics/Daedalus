//! Each op, mapped onto the planner and the loaded hosts. A dispatch that panics is answered
//! with an error, and the host goes on.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::string::{String, ToString};
use std::sync::Arc;
use std::vec::Vec;

use daedalus_backend::Backend;
use daedalus_graph::{GraphBody, GraphDoc, GraphPatch};
use daedalus_planner::cache::PlanKey;
use daedalus_planner::edit::patch::apply_patch;
use daedalus_planner::explain::{explain_plan, json::to_json};
use daedalus_planner::hash::plan_hash;
use daedalus_planner::lint::lint;
use daedalus_planner::{Config, Form, Planned, plan, solve};
use daedalus_wire::service::{
    CacheAction, CacheInfo, CarryReport, Diagnostic, EndReason, ErrorCode, EventBody, EventKinds,
    HostInfo, InspectTarget, Inspection, Names, Op, PlanForm, PlanOptions, PlanSummary, PluginInfo,
    Reply, Request, Response, RunId, ServiceError, Severity, StartConfig, StepSummary, StopMode,
    TelemetryEvent, TypedValue,
};

use super::runs::RunEntry;
use super::session::Session;
use super::values::FeedRefusal;
use super::{Engine, ServiceHost, lock};
use crate::report::BoundedNames;
use crate::{HostError, LoadOptions, LoadedHost, ReloadError, ReloadReport};

type Answer = Result<Reply, ServiceError>;

fn refuse(code: ErrorCode, message: impl Into<String>) -> ServiceError {
    ServiceError::new(code, message)
}

fn diagnostics(found: &[daedalus_planner::diagnostic::Diagnostic]) -> Vec<Diagnostic> {
    found
        .iter()
        .map(|item| Diagnostic {
            severity: match item.severity {
                daedalus_planner::diagnostic::Severity::Error => Severity::Error,
                daedalus_planner::diagnostic::Severity::Warning => Severity::Warning,
            },
            // The planner's code as it names it in JSON: snake case.
            code: serde_json::to_value(item.code)
                .ok()
                .and_then(|code| code.as_str().map(String::from))
                .unwrap_or_default(),
            message: item.message.clone(),
            node: item.node.clone(),
            port: item.port.clone(),
        })
        .collect()
}

fn names(list: &BoundedNames) -> Names {
    Names {
        names: list.names().to_vec(),
        total: u32::try_from(list.total()).unwrap_or(u32::MAX),
    }
}

fn carry(report: &ReloadReport) -> CarryReport {
    CarryReport {
        carried: names(&report.carried),
        fresh: names(&report.fresh),
        added: names(&report.added),
        removed: names(&report.removed),
        removed_ports: names(&report.removed_ports),
        dropped_values: report.dropped_values,
    }
}

fn form(form: PlanForm) -> Form {
    match form {
        PlanForm::Steps => Form::Steps,
        PlanForm::Blob => Form::Blob,
        PlanForm::Source => Form::Source,
    }
}

fn port_error(error: &HostError) -> ServiceError {
    let code = match error {
        HostError::UnknownPort { .. }
        | HostError::NotAnInput { .. }
        | HostError::NotAnOutput { .. } => ErrorCode::UnknownPort,
        HostError::Full { .. } => ErrorCode::Busy,
        HostError::Run(_) => ErrorCode::InvalidRequest,
        _ => ErrorCode::ValueType,
    };
    refuse(code, crate::render_error(error))
}

impl Engine {
    fn graph(&self, session: &str, name: &str) -> Result<&GraphBody, ServiceError> {
        self.graphs
            .get(session)
            .and_then(|graphs| graphs.get(name))
            .ok_or_else(|| {
                refuse(
                    ErrorCode::UnknownGraph,
                    std::format!("session `{session}` has no graph `{name}`"),
                )
            })
    }

    fn backends(&self) -> Vec<&dyn Backend> {
        self.backends.iter().map(|backend| &**backend).collect()
    }

    /// What solving and linting `graph` finds, without planning it.
    fn check(&self, graph: &GraphBody) -> Vec<Diagnostic> {
        let config = Config {
            lints: true,
            ..Config::default()
        };
        match solve(graph, &self.registry, &config) {
            Ok(solved) => diagnostics(&lint(&solved, &self.registry)),
            Err(errors) => {
                let found: Vec<_> = errors.iter().map(Into::into).collect();
                diagnostics(&found)
            }
        }
    }

    /// Plans a stored graph in the steps form, ready to load.
    fn plan_steps(
        &self,
        session: &str,
        name: &str,
        config: &StartConfig,
    ) -> Result<Planned, ServiceError> {
        let planning = Config {
            priority: config.priority.clone(),
            merge: !config.unmerged,
            ..Config::default()
        };
        let graph = self.graph(session, name)?;
        plan(graph, &self.registry, &self.backends(), &planning).map_err(|error| {
            ServiceError::planning(error.to_string(), diagnostics(&error.diagnostics()))
        })
    }
}

impl ServiceHost {
    /// Serves one request. The answer always carries the request's id, whatever happens.
    pub(crate) fn dispatch(&self, session: &Arc<Session>, request: Request) -> Response {
        let served = catch_unwind(AssertUnwindSafe(|| self.serve(session, &request)));
        let result = served.unwrap_or_else(|panic| {
            let what = daedalus_abi::panic_message(&*panic);
            Err(refuse(
                ErrorCode::InvalidRequest,
                std::format!("{} panicked in the host: {what}", request.op.kind().name()),
            ))
        });
        let code = result.as_ref().err().map(|error| error.code);
        lock(&self.trace).record(&request, code);
        Response {
            id: request.id,
            result,
        }
    }

    fn run(&self, run: RunId) -> Result<Arc<RunEntry>, ServiceError> {
        lock(&self.runs)
            .get(&run)
            .cloned()
            .ok_or_else(|| refuse(ErrorCode::UnknownRun, std::format!("no run {run}")))
    }

    fn serve(&self, session: &Arc<Session>, request: &Request) -> Answer {
        if self.is_stopping() {
            return Err(refuse(ErrorCode::Stopped, "the host is shutting down"));
        }
        let space = request.session.as_str();
        match &request.op {
            Op::Ping => Ok(Reply::Pong {
                version: daedalus_wire::service::SERVICE_VERSION,
            }),
            Op::LoadPlugin { path } => self.load_plugin(path).map(Reply::Plugin),
            Op::PutGraph { name, json } => self.put_graph(space, name, json),
            Op::PatchGraph { graph, patch } => self.patch_graph(space, graph, patch),
            Op::Plan { graph, options } => self.plan(space, graph, options),
            Op::Start { graph, config } => self.start(space, graph, config),
            Op::Feed { run, port, value } => self.feed(&self.run(*run)?, port, value),
            Op::Step { run, count } => self.step(&self.run(*run)?, *count),
            Op::Take { run, port } => self.take(&self.run(*run)?, port),
            Op::Watch { target, kinds } => {
                if let daedalus_wire::service::WatchTarget::Run(run) = target {
                    self.run(*run)?;
                }
                let sub = lock(&self.watchers).add(session, *target, *kinds);
                Ok(Reply::Watching { sub })
            }
            Op::Unwatch { sub } => match lock(&self.watchers).remove(session, *sub) {
                true => Ok(Reply::Ack),
                false => Err(refuse(
                    ErrorCode::InvalidRequest,
                    std::format!("this connection has no watch {sub}"),
                )),
            },
            Op::Reload { run, graph } => self.reload(space, &self.run(*run)?, graph),
            Op::Cancel { run } => {
                let entry = self.run(*run)?;
                entry.cancel();
                lock(&self.watchers).end_run(entry.id, EndReason::Cancelled);
                Ok(Reply::Ack)
            }
            Op::Stop { run, mode } => self.stop(&self.run(*run)?, *mode),
            Op::Inspect { target } => self.inspect(space, target).map(Reply::Inspection),
            Op::Cache {
                action: CacheAction::Clear,
            } => {
                let mut engine = lock(&self.engine);
                let capacity = self.options().plan_cache.max(1);
                engine.cache = daedalus_planner::cache::PlanCache::new(capacity);
                Ok(Reply::Ack)
            }
            Op::Trace { limit } => Ok(Reply::Trace(lock(&self.trace).recent(*limit))),
        }
    }

    /// Loads a plugin through the host's loader, as the `load_plugin` op does. The embedder
    /// calls this for the plugins it loads at start.
    pub fn load_plugin(&self, path: &str) -> Result<PluginInfo, ServiceError> {
        let mut engine = lock(&self.engine);
        let Engine {
            registry,
            backends,
            plugins,
            loaded,
            ..
        } = &mut *engine;
        let Some(loader) = plugins else {
            return Err(refuse(ErrorCode::Unsupported, "this host loads no plugins"));
        };
        let info = loader
            .load(path, registry, backends)
            .map_err(|reason| refuse(ErrorCode::PluginError, reason))?;
        if !loaded.contains(&info.name) {
            loaded.push(info.name.clone());
        }
        Ok(info)
    }

    fn put_graph(&self, session: &str, name: &str, json: &str) -> Answer {
        if name.is_empty() {
            return Err(refuse(ErrorCode::InvalidRequest, "a graph needs a name"));
        }
        let doc = GraphDoc::parse(json)
            .map_err(|error| refuse(ErrorCode::InvalidRequest, error.to_string()))?;
        let mut engine = lock(&self.engine);
        let diagnostics = engine.check(&doc.graph);
        engine
            .graphs
            .entry(String::from(session))
            .or_default()
            .insert(String::from(name), doc.graph);
        Ok(Reply::Graph {
            name: String::from(name),
            diagnostics,
        })
    }

    fn patch_graph(&self, session: &str, name: &str, patch: &str) -> Answer {
        let patch = GraphPatch::parse(patch)
            .map_err(|error| refuse(ErrorCode::InvalidRequest, error.to_string()))?;
        let mut engine = lock(&self.engine);
        let mut graph = engine.graph(session, name)?.clone();
        let report = apply_patch(&mut graph, &patch)
            .map_err(|error| refuse(ErrorCode::InvalidRequest, std::format!("{error:?}")))?;
        let mut diagnostics: Vec<Diagnostic> = report
            .not_found
            .iter()
            .map(|missing| Diagnostic {
                severity: Severity::Warning,
                code: String::from("selector_not_found"),
                message: std::format!(
                    "op {} selects a node the graph does not have: {:?}",
                    missing.op,
                    missing.selector
                ),
                node: missing.selector.id.clone(),
                port: None,
            })
            .collect();
        diagnostics.extend(engine.check(&graph));
        if let Some(graphs) = engine.graphs.get_mut(session) {
            graphs.insert(String::from(name), graph);
        }
        Ok(Reply::Graph {
            name: String::from(name),
            diagnostics,
        })
    }

    fn plan(&self, session: &str, name: &str, options: &PlanOptions) -> Answer {
        let mut engine = lock(&self.engine);
        let config = Config {
            priority: options.priority.clone(),
            lints: true,
            form: form(options.form),
            ..Config::default()
        };
        let key = {
            let graph = engine.graph(session, name)?;
            PlanKey::new(graph, &engine.registry, &engine.backends(), &config)
        };
        // An inspection is made from the plan itself, so it is planned again.
        if !options.inspect
            && let Some(summary) = engine.cache.get(&key)
        {
            let mut summary = summary.clone();
            summary.graph = String::from(name);
            summary.cached = true;
            return Ok(Reply::Plan(summary));
        }
        let graph = engine.graph(session, name)?;
        let backends = engine.backends();
        let planned = plan(graph, &engine.registry, &backends, &config).map_err(|error| {
            ServiceError::planning(error.to_string(), diagnostics(&error.diagnostics()))
        })?;
        let summary = PlanSummary {
            graph: String::from(name),
            // A plan with no stored encoding is named by what it was made from.
            hash: plan_hash(&planned, &config, &backends)
                .unwrap_or(key.graph_hash() ^ key.settings_hash().rotate_left(1)),
            form: options.form,
            nodes: planned.graph.nodes.len() as u32,
            edges: planned.graph.edges.len() as u32,
            regions: planned.regions.regions.len() as u32,
            steps: planned.stitched.layout.len() as u32,
            cached: false,
            diagnostics: diagnostics(&planned.warnings),
            explain: match options.inspect {
                true => Some(
                    to_json(&explain_plan(&planned))
                        .map_err(|error| refuse(ErrorCode::InvalidRequest, error.to_string()))?,
                ),
                false => None,
            },
        };
        drop(planned);
        let mut stored = summary.clone();
        stored.explain = None;
        engine.cache.insert(key, stored);
        Ok(Reply::Plan(summary))
    }

    fn load_options(config: &StartConfig) -> LoadOptions {
        let defaults = LoadOptions::default();
        LoadOptions {
            fifo_capacity: match config.fifo_capacity {
                0 => defaults.fifo_capacity,
                capacity => capacity,
            },
            fault_reports: match config.fault_reports {
                0 => defaults.fault_reports,
                reports => reports as usize,
            },
            overhead_window: 0,
        }
    }

    fn start(&self, session: &str, name: &str, config: &StartConfig) -> Answer {
        let limit = self.options().max_runs as usize;
        if limit > 0 && lock(&self.runs).len() >= limit {
            return Err(refuse(
                ErrorCode::Busy,
                std::format!("the host holds {limit} runs at once"),
            ));
        }
        let planned = lock(&self.engine).plan_steps(session, name, config)?;
        let host = LoadedHost::load(planned, Self::load_options(config))
            .map_err(|error| ServiceError::planning(error.to_string(), Vec::new()))?;
        let run = self.next_run();
        let entry = Arc::new(RunEntry::new(run, session, name, host, config.clone()));
        lock(&self.runs).insert(run, entry);
        Ok(Reply::Started { run })
    }

    fn feed(&self, entry: &Arc<RunEntry>, port: &str, value: &TypedValue) -> Answer {
        entry.live()?;
        let mut run = lock(&entry.run);
        let host = run.host.host();
        let key = host
            .port_infos()
            .find(|info| info.name == port && info.dir == crate::Dir::Input)
            .map(|info| String::from(info.type_key))
            .ok_or_else(|| {
                refuse(
                    ErrorCode::UnknownPort,
                    std::format!("run {} has no input port `{port}`", entry.id),
                )
            })?;
        let codec = self.codecs.get(&key).ok_or_else(|| {
            refuse(
                ErrorCode::Unsupported,
                std::format!("port `{port}` carries `{key}`, which has no wire codec"),
            )
        })?;
        if codec.ty != value.ty {
            return Err(refuse(
                ErrorCode::ValueType,
                std::format!(
                    "port `{port}` carries `{key}`, which travels as {}, not as {}",
                    codec.ty.name(),
                    value.ty.name()
                ),
            ));
        }
        match (codec.feed)(host, port, &value.value) {
            Ok(outcome) => Ok(Reply::Fed {
                woke: outcome.woke,
                dropped: outcome.dropped.is_some(),
            }),
            Err(FeedRefusal::Shape) => Err(refuse(
                ErrorCode::ValueType,
                std::format!("the value is not a `{key}`"),
            )),
            Err(FeedRefusal::Host(error)) => Err(port_error(&error)),
        }
    }

    fn step(&self, entry: &Arc<RunEntry>, count: u32) -> Answer {
        entry.live()?;
        let mut summary = StepSummary::default();
        for _ in 0..count {
            let ticked = entry.tick(self.now_ns())?;
            summary.ticks += 1;
            summary.step_calls += u64::from(ticked.report.step_calls);
            summary.faults += u64::from(ticked.report.faults);
            summary.skipped += u64::from(ticked.report.skipped);
            summary.idle = ticked.report.idle;
            if !ticked.faults.is_empty() {
                let mut watchers = lock(&self.watchers);
                for fault in ticked.faults {
                    watchers.emit(entry.id, EventKinds::FAULT, &EventBody::Fault(fault));
                }
            }
        }
        summary.tick = lock(&entry.run).tick;
        if summary.ticks > 0 {
            let event = EventBody::Telemetry(TelemetryEvent {
                run: entry.id,
                tick: summary.tick,
                step_calls: summary.step_calls,
                faults: summary.faults,
                skipped: summary.skipped,
            });
            lock(&self.watchers).emit(entry.id, EventKinds::TELEMETRY, &event);
        }
        Ok(Reply::Stepped(summary))
    }

    fn take(&self, entry: &Arc<RunEntry>, port: &str) -> Answer {
        let mut run = lock(&entry.run);
        let host = run.host.host();
        let key = host
            .port_infos()
            .find(|info| info.name == port && info.dir == crate::Dir::Output)
            .map(|info| String::from(info.type_key))
            .ok_or_else(|| {
                refuse(
                    ErrorCode::UnknownPort,
                    std::format!("run {} has no output port `{port}`", entry.id),
                )
            })?;
        // Checked before the value is taken, so a refusal leaves it in the port.
        let codec = self.codecs.get(&key).ok_or_else(|| {
            refuse(
                ErrorCode::Unsupported,
                std::format!("port `{port}` carries `{key}`, which has no wire codec"),
            )
        })?;
        let Some(value) = host.take_value(port).map_err(|error| port_error(&error))? else {
            return Ok(Reply::Value(None));
        };
        let wire = (codec.read)(&value).ok_or_else(|| {
            refuse(
                ErrorCode::ValueType,
                std::format!("the value in `{port}` is not held as a `{key}`"),
            )
        })?;
        Ok(Reply::Value(Some(TypedValue::new(codec.ty.clone(), wire))))
    }

    fn reload(&self, session: &str, entry: &Arc<RunEntry>, name: &str) -> Answer {
        entry.live()?;
        let config = lock(&entry.run).config.clone();
        let planned = lock(&self.engine).plan_steps(session, name, &config)?;
        let report = lock(&entry.run)
            .host
            .reload(planned)
            .map_err(|error| match error {
                ReloadError::OtherBridges { .. } => {
                    refuse(ErrorCode::InvalidRequest, error.to_string())
                }
                ReloadError::Load(_) => ServiceError::planning(error.to_string(), Vec::new()),
            })?;
        *lock(&entry.graph) = String::from(name);
        Ok(Reply::Reloaded(carry(&report)))
    }

    fn stop(&self, entry: &Arc<RunEntry>, mode: StopMode) -> Answer {
        if mode == StopMode::Drain {
            // Finish what is waiting: tick until a tick finds nothing to do. A graph that never
            // goes idle is stopped after the bound, and a cancelled run is not ticked at all.
            for _ in 0..self.options().drain_ticks {
                match entry.tick(self.now_ns()) {
                    Ok(ticked) if ticked.report.idle => break,
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        }
        entry.mark_stopped();
        entry.cancel();
        let removed = lock(&self.runs).remove(&entry.id).is_some();
        if !removed {
            return Err(refuse(
                ErrorCode::Stopped,
                std::format!("run {} was already stopped", entry.id),
            ));
        }
        let tick = lock(&entry.run).tick;
        let dropped = entry.queued();
        lock(&self.watchers).end_run(entry.id, EndReason::Stopped);
        Ok(Reply::Stopped {
            run: entry.id,
            tick,
            dropped,
        })
    }

    fn inspect(&self, session: &str, target: &InspectTarget) -> Result<Inspection, ServiceError> {
        Ok(match target {
            InspectTarget::Run(run) => Inspection::Run(self.run(*run)?.info()),
            InspectTarget::Telemetry(run) => Inspection::Telemetry(self.run(*run)?.telemetry()),
            InspectTarget::Graph(name) => {
                let engine = lock(&self.engine);
                let doc = GraphDoc::new(engine.graph(session, name)?.clone());
                Inspection::Graph {
                    name: name.clone(),
                    json: doc
                        .to_json()
                        .map_err(|error| refuse(ErrorCode::InvalidRequest, error.to_string()))?,
                }
            }
            InspectTarget::Cache => {
                let engine = lock(&self.engine);
                Inspection::Cache(CacheInfo {
                    entries: engine.cache.len() as u32,
                    hits: engine.cache.hits(),
                    misses: engine.cache.misses(),
                })
            }
            InspectTarget::Host => {
                let runs = lock(&self.runs).values().map(|run| run.brief()).collect();
                let engine = lock(&self.engine);
                Inspection::Host(HostInfo {
                    sessions: engine.graphs.keys().cloned().collect(),
                    graphs: engine
                        .graphs
                        .get(session)
                        .map(|graphs| graphs.keys().cloned().collect())
                        .unwrap_or_default(),
                    runs,
                    plugins: engine.loaded.clone(),
                })
            }
        })
    }
}
